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

from utils import GoogleCloudStorageIoManager, LocalIoManager, fetch
from datetime import datetime

# LANDING_FOLDER = LocalIoManager("./landing/ch.bfe.solarenergie-eignung-daecher/")
LANDING_FOLDER = GoogleCloudStorageIoManager(
    bucket_name="folimar-geotest-store001",
    prefix="landing/ch.bfe.solarenergie-eignung-daecher/",
)
SOURCE_STAC_URL = "https://data.geo.admin.ch/api/stac/v0.9/collections/ch.bfe.solarenergie-eignung-daecher/items/solarenergie-eignung-daecher"
STAC_ASSET_KEYS = ["solarenergie-eignung-daecher_2056.gdb.zip"]


def get_expected_asset_names() -> (set, dict):
    """
    Parse the stac file and create a set of expected asset paths.
    The expected asset name is constructed from the last part of the asset url,
    prefixed by the updated timestamp in format YYYYMMDD.
    Example:
    1) Stac asset json: {"updated": "2023-11-16T00:00:00.000Z", "href": "https://data.geo.admin.ch/ch.bfe.solarenergie-eignung-daecher/2056/gdb/ch.bfe.solarenergie-eignung-daecher_2056.gdb.zip"}
    3) Resulting asset name: 20231116_solarenergie-eignung-daecher_2056.gdb.zip
    """
    stac_item = fetch(SOURCE_STAC_URL).json()
    asset_names = set()
    asset_names_url_map = {}
    for key in STAC_ASSET_KEYS:
        asset = stac_item["assets"][key]
        url_part = asset["href"].split("/")[-1]
        date_part = datetime.strptime(
            asset["updated"], "%Y-%m-%dT%H:%M:%S.%fZ"
        ).strftime("%Y%m%d")
        asset_name = f"{date_part}_{url_part}"
        asset_names.add(asset_name)
        asset_names_url_map[asset_name] = asset["href"]
    return asset_names, asset_names_url_map


def get_existing_asset_asset_names() -> set:
    """Retrieve the list of currently stored zip files."""
    return set(LANDING_FOLDER.list())


def remove_outdated_assets(
    existing_asset_names: set, expected_asset_names: set
) -> None:
    """Identify and remove outdated or deleted assets."""
    to_delete = existing_asset_names - expected_asset_names
    total_to_delete = len(to_delete)
    for i, asset_name in enumerate(to_delete, 1):
        print(
            f"{i}/{total_to_delete} Delete: {LANDING_FOLDER.absolute_path(asset_name)}"
        )
        LANDING_FOLDER.delete(asset_name)


def fetch_and_save_assets(
    expected_asset_names: set, existing_asset_names: set, asset_url_map: dict
) -> None:
    """Fetch and store the new or updated assets."""
    to_fetch = expected_asset_names - existing_asset_names
    total_to_fetch = len(to_fetch)
    for i, asset_name in enumerate(to_fetch, 1):
        print(
            f"{i}/{total_to_fetch} Fetch and save: {LANDING_FOLDER.absolute_path(asset_name)}"
        )
        url = asset_url_map[asset_name]
        LANDING_FOLDER.write(asset_name, fetch(url).content, "wb")


def main():
    # Extract expected asset locations from stac metadata
    expected_asset_names, asset_url_map = get_expected_asset_names()

    # Retrieve the list of currently stored assets
    existing_asset_names = get_existing_asset_asset_names()

    # Identify and remove outdated or deleted assets
    remove_outdated_assets(existing_asset_names, expected_asset_names)

    # Fetch and store the new or updated assets
    fetch_and_save_assets(expected_asset_names, existing_asset_names, asset_url_map)

    print("All done!")


if __name__ == "__main__":
    main()
