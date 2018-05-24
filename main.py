import sys
import json
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


def list_objects(s3, bucket_name, prefix, skip_keys=None):
    skip_keys = skip_keys or set()
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=prefix):
        if obj.key not in skip_keys:
            # 'impressions_c....151908.log.gz'
            #  ^category ^
            filename = os.path.basename(obj.key)
            category = filename.split('_')[0]
            yield category, filename, obj

def group_categories(stream_of_categories):
    # there are 4 groups of logs s3://bucket/reds5/date=yyyy-mm-dd/hour=hh/<category>_timestamp.log.gz
    # we process them chunk by chunk
    for group, objects in itertools.groupby(
            sorted(stream_of_categories, key=itemgetter(0)),
            key=itemgetter(0)):
        # objects == (category, filename, obj)
        # and group == category in this case
        yield group, objects


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

    categories = params.get('categories', [])

    print("parsing statefile for already downloaded files")
    already_downloaded = load_downloaded_files(statefile='/data/in/state.json')

    s3 = get_s3_client(access_key, secret_key)

    stream_of_objects = list_objects(s3, bucket_name, prefix, skip_keys=already_downloaded)

    for category, objects in group_categories(stream_of_objects):
        if categories and (category not in categories):
            print("Found category of logs '{}' but it's not in config file so skipping".format(category))
            continue
        elif not categories:
            print("did not specify categories, taking all i can find!")
        # categories are "impressions", "clicks", etc...
        print("parsing category '{}'".format(category))
        slice_path = '/data/out/tables/{}.csv'.format(category)
        print("making directory '{}' for saving slices".format(slice_path))
        os.mkdir(slice_path)
        for i, (_, filename, obj) in enumerate(objects):
            actual_object = obj.get()
            file_path = os.path.join(slice_path, filename)
            save_one_file(file_path, actual_object['Body'])
            already_downloaded.add(obj.key)
            if i % 50 == 0:
                print("Downloaded {} files".format(i))
        print("Downloaded {} files total".format(i))
        write_slice_manifest(slice_path, category)


    print("Saving statefile with already downloaded files")
    with open("/data/out/state.json", "w") as fout:
        json.dump({"already_downloaded": list(already_downloaded)}, fout)

def write_slice_manifest(slice_folder_path, category):
    print("Creating manifest for {} sliced table".format(slice_folder_path))
    # HARDCODED Let's hope this doesn't change
    num_of_cols = {"clicks":17, "conversions":21,
                   "impressions":19, "videoevents":16}[category]
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
            chunk = stream.read(1048)
            if chunk:
                outf.write(chunk)
            else:
                break

def load_downloaded_files(statefile):
    with open(statefile) as sf:
        return set(json.load(sf).get('already_downloaded', []))

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
