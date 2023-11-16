# -*- coding: utf-8 -*-
# For each roof in Switzerland, detailed data about the solar potential is available. The data is provided
# by the Swiss Federal Office of Energy (SFOE) and is updated regularly. The data is provided here:
# https://www.geocat.ch/geonetwork/srv/ger/catalog.search#/metadata/b614de5c-2f12-4355-b2c9-7aef2c363ad6
# We leverage the federal geoportal STAC API to obtain metadata (last updated timestamp) and the asset download urls.
# The data is distributed as a single zipped geopackage or a single zipped esri filegeodatabase.
# Sadly, it seems that as of 2023-11 something is broken with the geopackage zip compression,
# which is why we work with the filegeodatabase >.<

# The script does the following:
# - Fetch the STAC metadata from the source url and extract updated timestamp and asset download url.
# - Check the cloud storage for an existing asset. If it is outdated or non-existent, download the asset and save it in the landing zone.

from utils_misc import fetch
from utils_gcs import list_blobs, delete_blob, write_blob
from datetime import datetime

SOURCE_STAC_URL = "https://data.geo.admin.ch/api/stac/v0.9/collections/ch.bfe.solarenergie-eignung-daecher/items/solarenergie-eignung-daecher"
SINK_LOCATION = "folimar-geotest-store001/landing/ch.bfe.solarenergie-eignung-daecher"
STAC_ASSET_KEYS = ["solarenergie-eignung-daecher_2056.gdb.zip"]


def get_expected_asset_blob_names(stac_item_url: str) -> (set, dict):
    """
    Parse the stac file and create a set of expected asset blob names.
    The asset blob names are constructed from the last part of the asset url,
    prefixed by the updated timestamp in format YYYYMMDD and the sink location.
    Example:
    1) Stac asset json: {"updated": "2023-11-16T00:00:00.000Z", "href": "https://data.geo.admin.ch/ch.bfe.solarenergie-eignung-daecher/2056/gdb/ch.bfe.solarenergie-eignung-daecher_2056.gdb.zip"}
    2) Sink location: folimar-geotest-store001/landing/ch.bfe.solarenergie-eignung-daecher
    3) Resulting full blob name: folimar-geotest-store001/landing/ch.bfe.solarenergie-eignung-daecher/20231116_solarenergie-eignung-daecher_2056.gdb.zip
    """
    stac_item = fetch(stac_item_url).json()
    blob_names = set()
    blob_names_url_map = {}
    for key in STAC_ASSET_KEYS:
        asset_info = stac_item["assets"][key]
        url_part = asset_info["href"].split("/")[-1]
        date_part = datetime.strptime(
            asset_info["updated"], "%Y-%m-%dT%H:%M:%S.%fZ"
        ).strftime("%Y%m%d")
        blob_name = f"{SINK_LOCATION}/{date_part}_{url_part}"
        blob_names.add(blob_name)
        blob_names_url_map[blob_name] = asset_info["href"]
    return blob_names, blob_names_url_map


def get_existing_asset_blob_names(sink_location: str) -> set:
    """Retrieve the list of currently stored zip files."""
    return set(b for b in list_blobs(sink_location))


def remove_outdated_assets(existing_assets: set, expected_assets: set) -> None:
    """Identify and remove outdated or deleted assets."""
    to_delete = existing_assets - expected_assets
    total_to_delete = len(to_delete)
    for i, blob_name in enumerate(to_delete, 1):
        print(f"{i}/{total_to_delete} Delete: {blob_name}")
        delete_blob(blob_name)


def fetch_and_upload_assets(
    expected_assets: set, existing_assets: set, asset_url_map: dict
) -> None:
    """Fetch and store the new or updated assets."""
    to_fetch = expected_assets - existing_assets
    total_to_fetch = len(to_fetch)
    for i, blob_name in enumerate(to_fetch, 1):
        print(f"{i}/{total_to_fetch} Fetch and upload: {blob_name}")
        url = asset_url_map[blob_name]
        write_blob(blob_name, fetch(url).content, "wb")


def main():
    # Extract expected asset locations from stac metadata
    expected_assets, asset_url_map = get_expected_asset_blob_names(SOURCE_STAC_URL)

    # Retrieve the list of currently stored assets
    existing_assets = get_existing_asset_blob_names(SINK_LOCATION)

    # Identify and remove outdated or deleted assets
    remove_outdated_assets(existing_assets, expected_assets)

    # Fetch and store the new or updated assets
    fetch_and_upload_assets(expected_assets, existing_assets, asset_url_map)

    print("All done!")


if __name__ == "__main__":
    main()
