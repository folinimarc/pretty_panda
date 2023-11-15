# -*- coding: utf-8 -*-
# For each roof in Switzerland, detailed data about the solar potential is available. The data is provided
# by the Swiss Federal Office of Energy (SFOE) and is updated regularly. The data is provided here:
# https://www.geocat.ch/geonetwork/srv/ger/catalog.search#/metadata/b614de5c-2f12-4355-b2c9-7aef2c363ad6
# We leverage the federal geoportal STAC API to obtain metadata (last updated timestamp) and the asset download urls.
# The data is distributed as a single zipped geopackage or a single zipped esri filegeodatabase.
# Sadly, it seems that as of 2023-11 something is broken with the geopackage zip compression,
# which is why we work with the filegeodatabase >.<

# The script does the following:
# - Fetch the STAC metadata from the source url.
# - Check the cloud storage for a stac metadata snapshot file from previous runs.
# - Decide on whether to download the data or not.
# - If newer data is available, download the data and save it in the landing zone, overwriting existing ones.

from utils_misc import fetch
from utils_gcs import read_blob, write_blob
import datetime
import json

SOURCE_STAC_URL = "https://data.geo.admin.ch/api/stac/v0.9/collections/ch.bfe.solarenergie-eignung-daecher/items/solarenergie-eignung-daecher"
SINK_LOCATION = "folimar-geotest-store001/landing/ch.bfe.solarenergie-eignung-daecher"
STAC_META_SNAPSHOT_NAME = "stac_json.json"
STAC_ASSET_KEY = "solarenergie-eignung-daecher_2056.gdb.zip"


def parse_timestamp(stac_asset_info):
    return datetime.datetime.strptime(
        stac_asset_info["updated"], "%Y-%m-%dT%H:%M:%S.%fZ"
    )


def fetch_stac_data(url):
    return fetch(url).json()


def is_new_data_available(current_stac, existing_stac):
    current_timestamp = parse_timestamp(current_stac["assets"][STAC_ASSET_KEY])
    existing_timestamp = (
        parse_timestamp(existing_stac["assets"][STAC_ASSET_KEY])
        if existing_stac
        else None
    )
    return existing_timestamp is None or current_timestamp > existing_timestamp


def fetch_and_save_data(download_url, location):
    data = fetch(download_url).content
    write_blob(location, data, "wb")


def main():
    print("Fetching STAC metadata from source url.")
    current_stac = fetch_stac_data(SOURCE_STAC_URL)
    print("Fetching existing STAC metadata snapshot from cloud storage if exists.")
    existing_stac = json.loads(read_blob(f"{SINK_LOCATION}/{STAC_META_SNAPSHOT_NAME}"))

    if is_new_data_available(current_stac, existing_stac):
        asset_download_url = current_stac["assets"][STAC_ASSET_KEY]["href"]
        location = f"{SINK_LOCATION}/{STAC_ASSET_KEY}"
        print(f"Downloading new data from {asset_download_url} to location {location}.")
        fetch_and_save_data(asset_download_url, location)
    else:
        print("No new data available. Nothing to do.")
    print("Updating STAC metadata snapshot.")
    write_blob(
        f"{SINK_LOCATION}/{STAC_META_SNAPSHOT_NAME}", json.dumps(current_stac), "w"
    )


if __name__ == "__main__":
    main()
