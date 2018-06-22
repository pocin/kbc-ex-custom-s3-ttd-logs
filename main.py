import sys
import json
import time
from hashlib import md5
import pytz
import subprocess
import datetime
import os
import itertools
from operator import itemgetter
import requests
import logging
from keboola.docker import Config
import pytz
import boto3

WHEN_COMBINE_CHUNKS=10000

COLUMNS = {
    "conversions": [
        "LogEntryTime", "ConversionId", "AdvertiserId", "ConversionType", "TDID",
        "IPAddress", "ReferrerURL", "MonetaryValue", "MonetaryValueCurrency",
        "OrderId", "custom1", "custom2", "custom3", "custom4", "custom5",
        "custom6", "custom7", "custom8", "custom9", "custom10", "ProcessTime"
    ],
    "videoevents": [
        "LogEntryTime", "ImpressionId", "VideoEventCreativeView",
        "VideoEventStart", "VideoEventFirstQuarter", "VideoEventMidPoint",
        "VideoEventThirdQuarter", "VideoEventComplete", "VideoEventMuted",
        "VideoEventUnmuted", "CreativeIsTrackable", "CreativeWasViewable",
        "VideoPlayTimeInSeconds", "VideoViewableTimeInSeconds",
        "VideoEventCompanionCreativeView", "ProcessTime"
    ],
    "clicks": [
        "LogEntryTime", "ClickId", "IPAddress", "ReferrerURL", "RedirectURL",
        "CampaignID", "ChannelID", "AdvertiserID", "DisplayImpressionID",
        "Keyword", "KeywordID", "MatchType", "DistributionNetwork",
        "TDID", "RawUrl", "ProcessedTime", "DeviceID"
    ],
    "impressions": [
        "LogEntryTime", "ImpressionId", "PartnerId", "AdvertiserId",
        "CampaignId", "AdGroupId", "PrivateContractID", "AudienceID",
        "CreativeId", "AdFormat", "Frequency", "SupplyVendor",
        "SupplyVendorPublisherID", "DealID", "Site", "ReferrerCategoriesList",
        "FoldPosition", "UserHourOfWeek", "UserAgent", "IPAddress", "TDID",
        "Country", "Region", "Metro", "City", "DeviceType", "OSFamily", "OS",
        "Browser", "Recency", "LanguageCode", "MediaCost", "FeeFeatureCost",
        "DataUsageTotalCost", "TTDCostInUSD", "PartnerCostInUSD",
        "AdvertiserCostInUSD", "Latitude", "Longitude", "DeviceID", "ZipCode",
        "ProcessedTime", "DeviceMake", "DeviceModel", "RenderingContext",
        "CarrierID", "TemperatureInCelsiusName",
        "TemperatureBucketStartInCelsiusName", "TemperatureBucketEndInCelsiusName"
    ]
}
PRIMARY_KEYS = {
    "conversions": ["ConversionId"],
    "videoevents": ["ImpressionId"],
    "clicks": ["ClickId"],
    "impressions": ["ImpressionId"]
}



def get_s3_client(access_key, secret_key):
    sess = boto3.session.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)
    return sess.resource('s3')


def list_objects(s3, bucket_name, prefix, newer_than=None):
    newer_than = newer_than or datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)
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
        logging.info("since {wipe_statefile: true} I am wiping clean statefile and exiting")
        with open('/data/out/state.json', 'w') as m:
            json.dump({}, m)
        return
    access_key = params['#access_key']
    secret_key = params['#secret_key']

    combining_chunks_threshold = params.get('chunks_combining_threshold') or WHEN_COMBINE_CHUNKS

    bucket_name = params['bucket_name']
    prefix = params['prefix']

    categories = params.get('categories')
    if categories is None:
        raise ValueError("Please specify which categories to download ('clicks', 'conversions', 'impressions', 'videoevents')")

    logging.info("parsing statefile for already downloaded files")
    latest_file_datetime = load_latest_downloaded_file(statefile='/data/in/state.json')

    s3 = get_s3_client(access_key, secret_key)

    stream_of_objects = list_objects(s3, bucket_name, prefix, newer_than=latest_file_datetime)

    category_paths = {}
    temp_category_paths = {}
    chunk_counter = {}
    for category in categories:
        slice_path = '/data/out/tables/{}.csv'.format(category)
        tmp_slice_path = '/tmp/{}'.format(category)
        logging.debug("making directory '{}' for saving slices".format(slice_path))
        os.makedirs(slice_path, exist_ok=True)
        os.makedirs(tmp_slice_path, exist_ok=True)
        category_paths[category] = slice_path
        temp_category_paths[category] = tmp_slice_path
        chunk_counter[category] = 0
        write_slice_manifest(slice_path, category)

    i = 0
    filename = None
    for i, (category, filename, obj) in enumerate(stream_of_objects, 1):
        # categories are "impressions", "clicks", etc...
        if category not in categories:
            # we don't want these categories because they are not in the config
            continue
        actual_object = obj.get()
        file_path = os.path.join(temp_category_paths[category], filename)
        save_one_file(file_path, actual_object['Body'])
        logging.debug("Downloaded %s", filename)
        if i % 500 == 0:
            logging.info("Downloaded %s files so far, currently %s", i, filename)

        chunk_counter[category] += 1
        if chunk_counter[category] == combining_chunks_threshold:
            logging.info("Combining {} small chunks into 1 bigger table")
            # each file has about 3kb and there are thousands of them
            # it's better to merge them otherwise the upload to kbc takes ages
            combine_chunks(temp_category_paths[category], category_paths[category])
            chunk_counter[category] = 0

        # keep track of latest downloaded file
        latest_file_datetime = max(obj.last_modified, latest_file_datetime)


    # combine remaining chunks leftover in temp dirs
    for category in categories:
        if chunk_counter[category] > 0:
            combine_chunks(temp_category_paths[category], category_paths[category])

    logging.info("Downloaded {} files total, last one was {}".format(i, filename))

    if params.get('remember_downloaded'):
        logging.info("Saving statefile with already downloaded files")
        with open("/data/out/state.json", "w") as fout:
            state = {"latest_downloaded_file": latest_file_datetime.timestamp()}
            logging.debug(json.dumps(state))

            json.dump(state, fout)


def combine_chunks(tmp_dir, final_dir, clean_tmp_dir=True):
    pj = os.path.join
    nonce = md5(str(time.time()).encode('utf-8')).hexdigest()
    final_combined_path = pj(final_dir, 'chunk_'+ nonce + '.csv')
    logging.info("combining chunks from %s to %s", tmp_dir, final_combined_path)

    decompress_cmd = "find {tmp_dir} -name '*.gz' -exec gzip -d {{}} \;".format(tmp_dir=tmp_dir)
    logging.debug("decompressing %s", decompress_cmd)
    subprocess.check_call(decompress_cmd, shell=True)
    cat_cmd = "find {tmp_dir} -name '*.log' | xargs cat > {final_path}".format(
        tmp_dir=tmp_dir,
        final_path=final_combined_path)
    logging.debug("merging with %s", cat_cmd)
    subprocess.check_call(cat_cmd, shell=True)
    if clean_tmp_dir:
        logging.debug("cleaning up")
        subprocess.check_call('rm ' + pj(tmp_dir, '*.log'), shell=True)

    logging.info("Combined")
    return final_combined_path


def write_slice_manifest(slice_folder_path, category):
    logging.info("Creating manifest for {} sliced table".format(slice_folder_path))
    columns = COLUMNS[category]
    pk = PRIMARY_KEYS[category]

    manifest_path = slice_folder_path + '.manifest'
    with open(manifest_path, 'w') as manifout:
        manifest = {
            'columns': columns,
            'primary_key': pk,
            'incremental': True,
            'delimiter': "\t",
            }
        json.dump(manifest, manifout)
    return manifest_path

def save_one_file(outpath, stream):
    with open(outpath, 'wb') as outf:
        while True:
            chunk = stream.read(1024**2)
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
        logging.getLogger('boto3').setLevel(logging.CRITICAL)
        logging.getLogger('botocore').setLevel(logging.CRITICAL)
        main('/data/')
    except (ValueError, KeyError, requests.HTTPError) as err:
        logging.error(err)
        sys.exit(1)
    except:
        logging.exception("Internal error")
        sys.exit(2)
