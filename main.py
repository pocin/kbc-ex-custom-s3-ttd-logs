import sys
import json
import pytz
import datetime
import os
import itertools
from operator import itemgetter
import requests
import logging
from keboola.docker import Config
import boto3

def get_s3_client(access_key, secret_key):
    sess = boto3.session.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)
    return sess.resource('s3')


def list_objects(s3, bucket_name, prefix, newer_than):
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=prefix):
        if obj.last_modified > newer_than:
            # 'impressions_c....151908.log.gz'
            #  ^category ^
            filename = os.path.basename(obj.key)
            category = filename.split('_')[0]
            yield category, filename, obj


def main(datadir):
    params = Config(datadir).get_parameters()
    if params.get('wipe_statefile'):
        print("since {wipe_statefile: true} I am wiping clean statefile and exiting")
        with open('/data/out/state.json', 'w') as m:
            json.dump({}, m)
        return
    access_key = params['#access_key']
    secret_key = params['#secret_key']

    bucket_name = params['bucket_name']
    prefix = params['prefix']

    categories = params.get('categories')
    if categories is None:
        raise ValueError("Please specify which categories to download ('clicks', 'conversions', 'impressions', 'videoevents')")

    print("parsing statefile for already downloaded files")
    latest_file_datetime = load_latest_downloaded_file(statefile='/data/in/state.json')

    s3 = get_s3_client(access_key, secret_key)

    stream_of_objects = list_objects(s3, bucket_name, prefix, newer_than=latest_file_datetime)

    category_paths = {}
    for category in categories:
        slice_path = '/data/out/tables/{}.csv'.format(category)
        print("making directory '{}' for saving slices".format(slice_path))
        os.makedirs(slice_path, exist_ok=True)
        category_paths[category] = slice_path
        write_slice_manifest(slice_path, category)

    i = 0
    for i, (category, filename, obj) in enumerate(stream_of_objects):
        # categories are "impressions", "clicks", etc...
        if category not in categories:
            # we don't want these categories because they are not in the config
            continue
        actual_object = obj.get()
        file_path = os.path.join(category_paths[category], filename)
        print("Downloading {}".format(filename))
        save_one_file(file_path, actual_object['Body'])
        # keep track of latest downloaded file
        latest_file_datetime = max(obj.last_modified, latest_file_datetime)

    print("Downloaded {} files total".format(i))

    if params.get('remember_downloaded'):
        print("Saving statefile with already downloaded files")
        with open("/data/out/state.json", "w") as fout:
            json.dump({"latest_downloaded_file": latest_file_datetime.timestamp()}, fout)

def write_slice_manifest(slice_folder_path, category):
    print("Creating manifest for {} sliced table".format(slice_folder_path))
    # HARDCODED Let's hope this doesn't change
    num_of_cols = {"clicks":17,
                   "conversions":21,
                   "impressions":49,
                   "videoevents":16}[category]
    manifest_path = slice_folder_path + '.manifest'
    with open(manifest_path, 'w') as manifout:
        manifest = {
            'columns': ['col_{}'.format(i) for i in range(num_of_cols)],
            'delimiter': "\t",
            }
        json.dump(manifest, manifout)
    return manifest_path

def save_one_file(outpath, stream):
    with open(outpath, 'wb') as outf:
        while True:
            chunk = stream.read(1024*40)
            if chunk:
                outf.write(chunk)
            else:
                break

def load_latest_downloaded_file(statefile):
    with open(statefile) as sf:
        state = json.load(sf)
        latest_ts = state.get('latest_downloaded_file', 0)
        return datetime.datetime.utcfromtimestamp(latest_ts).astimezone(pytz.utc)

if __name__ == "__main__":
    try:
        logging.basicConfig(level=logging.INFO, stream=sys.stdout)
        main('/data/')
    except (ValueError, KeyError, requests.HTTPError) as err:
        logging.error(err)
        sys.exit(1)
    except:
        logging.exception("Internal error")
        sys.exit(2)
