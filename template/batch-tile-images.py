# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A workflow for tiling images
"""

import os
import io
import argparse
import logging
import math
import itertools
from concurrent.futures import ThreadPoolExecutor
from abc import ABC, abstractmethod

import imageio
import requests

import apache_beam as beam
from apache_beam.io.fileio import MatchFiles
from apache_beam.io.gcp import gcsio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms.core import ParDo

from google.cloud import storage
from google.cloud import bigquery


class DoFnWithGCSClient(ABC, beam.DoFn):
    def setup(self):
        self.client = storage.Client()
        adapter = requests.adapters.HTTPAdapter(pool_connections=128, pool_maxsize=128, max_retries=3, pool_block=True)
        self.client._http.mount("https://", adapter)
        self.client._http._auth_request.session.mount("https://", adapter)
    def start_bundle(self):
        self.executor = ThreadPoolExecutor(128)
    @abstractmethod
    def process(self, el):
        pass
    def finish_bundle(self):
        self.executor.shutdown()

class FilterByStatus(DoFnWithGCSClient):
    def __init__(self, output, input_dir, project, dataset, table, final_bucket):
        self.output = output
        self.input_dir = input_dir
        self.project = project
        self.dataset = dataset
        self.table = table
        self.final_bucket = final_bucket
    def setup(self):
        super().setup()
        self.bgclient = bigquery.Client(project=self.project)
    def process(self, el):
        # Get image names
        path = el.path
        directory, filename = os.path.split(path)
        name, ext = os.path.splitext(filename)
        imagecode = name
        subpath = directory[len(self.input_dir):]
        if subpath:
           # remove leading "/" before joining with output
           subpath = subpath[1:]
        save_to = self.output + subpath + "/" + name
        # Get input image md5
        bucket_name, target_key = split_path(path)
        bucket = self.client.bucket(bucket_name)
        blob = bucket.get_blob(target_key)
        input_md5 = blob.md5_hash
        dataset = self.dataset
        table = self.table
        # Check if image was already imported
        cmd = f"select filename, path, md5 from `{dataset}.{table}` where imagecode='{imagecode}' limit 1"
        query_job = self.bgclient.query(cmd)
        rows = query_job.result()
        action = 'insert'
        for row in rows:
            action = 'update'
            # Compare with recorded md5
            if row.md5 == input_md5:
                # ignore unchanged images, removing them
                blob.delete()
                return
            # delete old version of the image (which can be in another location!)
            orig_name, orig_ext = os.path.splitext(row.filename)
            orig_key = row.path + orig_ext
            final_bucket = self.client.bucket(self.final_bucket)
            orig_blob = final_bucket.blob(orig_key)
            orig_blob.delete()
            break
        # Only process new or changed images
        yield{
            "input_path": path, 
            "save_to": save_to,
            "filename": filename,
            "ext": ext,
            "imagecode": imagecode,
            "barcode": imagecode.split("_")[0],
            "md5": input_md5, 
            "action": action
        }

class UploadGCS(DoFnWithGCSClient):
    def process(self, el):
        tile_path = el[0]
        data = el[1]
        bucket_name, target_key = split_path(tile_path)
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(target_key)
        try:
            self.executor.submit(blob.upload_from_string, data)
        except Exception as e:
            print(e)
        return ["Ok"]

class ProcessTiles(DoFnWithGCSClient):
    def __init__(self, project, dataset, table, final_bucket):
        self.project = project
        self.dataset = dataset
        self.table = table
        self.final_bucket = final_bucket
    def setup(self):
        super().setup()
        self.bgclient = bigquery.Client(project=self.project)
    def process(self, el):
        im = el["image"]
        save_to = el["save_to"]
        # Remove existing tiles if they exist
        bucket_name, target_key = split_path(save_to)
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(target_key)
        try:
            self.executor.submit(blob.delete)
        except Exception as e:
            print(e)

        # Create new tiles
        tile_size = 256
        num_tiles = 0
        tier = math.ceil(max([math.log(shape/tile_size, 2) for shape in im.shape]))
        max_step = 2 ** tier
        steps = [int(max_step/2**i) for i in range(tier + 1)]
        tiers = [im[::step, ::step] for step in steps]
        def next_i():
            i = 0
            while i < tile_size:
                yield i // tile_size
                i += 1
        tile_group = next_i()
        try:
            for tier_number, tier in enumerate(tiers):
                for item in itertools.product([i for i in range(math.ceil(tier.shape[0]/tile_size))], [i for i in range(math.ceil(tier.shape[1]/tile_size))]):
                    num_tiles += 1
                    yield{
                        "tier": tier,
                        "tier_number": tier_number,
                        "item": item,
                        "tile_group": next(tile_group),
                        "save_to": save_to
                    }
        finally:
            # Save ImageProperties.xml
            img_width = im.shape[1]
            img_height = im.shape[0]
            data = f'<IMAGE_PROPERTIES WIDTH="{img_width}" HEIGHT="{img_height}" NUMTILES="{num_tiles}" NUMIMAGES="1" VERSION="1.8" TILESIZE="{tile_size}"/>'
            output_folder = save_to.replace(f"gs://{bucket_name}/","")
            target_key = output_folder + "/ImageProperties.xml"
            blob = bucket.blob(target_key)
            try:
                self.executor.submit(blob.upload_from_string, data)
            except Exception as e:
                print(e)
            # Save image metadata in db
            barcode = el["barcode"]
            imagecode = el["imagecode"]
            filename = el["filename"]
            md5 = el["md5"]
            dataset = self.dataset
            table = self.table
            if el["action"] == "insert":
                cmd = f'insert into `{dataset}.{table}` (barcode, imagecode, filename, path, width, height, md5) values("{barcode}", "{imagecode}", "{filename}", "{output_folder}", {img_width}, {img_height}, "{md5}")'
            else:
                cmd = f'update `{dataset}.{table}` set filename="{filename}", path="{output_folder}", width={img_width}, height={img_height}, md5="{md5}" where imagecode="{imagecode}"'
            query_job = self.bgclient.query(cmd)
            result = query_job.result()
            # Move input image to final destination
            input_bucket_name, target_key = split_path(el["input_path"])
            input_bucket = self.client.bucket(input_bucket_name)
            input_blob = input_bucket.blob(target_key)
            final_bucket = self.client.bucket(self.final_bucket)
            blob_copy = input_bucket.copy_blob(input_blob, final_bucket, target_key)
            try:
                self.executor.submit(input_blob.delete)
            except Exception as e:
                print(e)


def split_path(path):
    bucket_name = path.replace("gs://", "").split("/")[0]
    target_key = path.replace(f"gs://{bucket_name}/","")
    return bucket_name, target_key

def by_extension(el, extensions):
  ext = el.path.split('.')[-1]
  return ext.lower() in extensions

def img_read(el):
    kwargs = {}
    path = el["input_path"]
    if el["ext"].lower() == ".png":
        kwargs["pilmode"] = "RGB" # discard alpha channel
    gcs = gcsio.GcsIO()
    return {"image": imageio.imread(gcs.open(path), **kwargs),
            "save_to": el["save_to"],
            "md5": el["md5"],
            "input_path": path,
            "filename": el["filename"],
            "imagecode": el["imagecode"],
            "barcode": el["barcode"],
            "action": el["action"]
           }

def save_tile(el):
    # gcs = gcsio.GcsIO()
    tier = el["tier"]
    tier_number = el["tier_number"]
    item = el["item"]
    tile_group = el["tile_group"]
    save_to = el["save_to"]
    path = f"{save_to}/TileGroup{tile_group}/{tier_number}-{item[1]}-{item[0]}.jpg"

    f = io.BytesIO()
    # gcs.open(path, 'w', mime_type="image/jpeg")
    try:
        imageio.imwrite(
            #f"image/TileGroup-{next(tile_group)}-{tier_number}-{item[0]}-{item[1]}.jpg",
            f,
            tier[item[0]*256:min(tier.shape[0],item[0]*256+256), item[1]*256:min(tier.shape[1],item[1]*256+256), :3],
            format="JPG", quality=80)
        return [(path, f.getvalue())]
    except ValueError as e:
        print(e)

def main(argv=None, save_main_session=True):
  """Main entry point"""

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://YOUR_INPUT_BUCKET/AND_INPUT_PREFIX/',
      help='Input folder to process.')
  parser.add_argument(
      '--output',
      dest='output',
      default='gs://YOUR_OUTPUT_BUCKET/AND_OUTPUT_PREFIX',
      help='Output file to write results to.')
  parser.add_argument(
      '--extensions',
      dest='extensions',
      default='jpg',
      help='File extensions to be processed.')
  parser.add_argument(
      '--project',
      dest='project',
      help='Bigquery project name.')
  parser.add_argument(
      '--dataset',
      dest='dataset',
      help='Bigquery dataset name.')
  parser.add_argument(
      '--table',
      dest='table',
      help='Bigquery table name.')
  parser.add_argument(
      '--final-bucket',
      dest='final_bucket',
      help='Final bucket destination for input images.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  valid_extensions = known_args.extensions.split(',')

  input_dir, file_pattern = os.path.split(known_args.input)

  with beam.Pipeline(options=pipeline_options) as p:
    # Read the text file[pattern] into a PCollection.
    files = p | MatchFiles(known_args.input)
    (
        files
        | 'Filter by file extension' >> beam.Filter(by_extension, valid_extensions)
        | 'Filter by file status' >> beam.ParDo(FilterByStatus(known_args.output, input_dir, known_args.project, known_args.dataset, known_args.table, known_args.final_bucket))
        | 'Read images to arrays' >> beam.Map(lambda x: img_read(x))
        | 'Tile images' >> beam.ParDo(ProcessTiles(known_args.project, known_args.dataset, known_args.table, known_args.final_bucket))
        | 'Save tile' >> beam.ParDo(save_tile)
        | 'Upload to GCS' >> beam.ParDo(UploadGCS())
    )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
