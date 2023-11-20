# -*- coding: utf-8 -*-
"""
For each roof in Switzerland, detailed data about the solar potential is available. The data is provided
by the Swiss Federal Office of Energy (SFOE) and is updated regularly. The data is provided here:
https://www.geocat.ch/geonetwork/srv/ger/catalog.search#/metadata/b614de5c-2f12-4355-b2c9-7aef2c363ad6
We leverage the federal geoportal STAC API to obtain metadata (last updated timestamp) and the asset download urls.
The data is distributed as a single zipped geopackage or a single zipped esri filegeodatabase.
Sadly, it seems that as of 2023-11 something is broken with the geopackage zip compression,
which is why we work with the filegeodatabase >.<

The script does the following:
- Fetch the STAC metadata from the source url and extract updated timestamp and asset download url.
- Check the cloud storage for an existing asset. If it is outdated or non-existent, download the asset
  and save it in the landing zone under a provided name using filename timestamp versioning (filename__YYYYMMDD).
"""

from utils import fetch
from utils_io import (
    LocalStorageBackend,
    GoogleCloudStorageStorageBackend,
    YYYYMMDDFilenamePrefix,
    VersionedFile,
)
from datetime import datetime

# storage_backend = GoogleCloudStorageStorageBackend(
#     bucket_name="folimar-geotest-store001",
#     root_directory="",
# )

storage_backend = LocalStorageBackend(
    root_directory="./data/",
)

OUTPUT_FILE = VersionedFile(
    file_path="landing/ch.bfe.solarenergie-eignung/solarenergie-eignung-daecher_2056.gdb.zip",
    storage_backend=storage_backend,
    versioning_scheme=YYYYMMDDFilenamePrefix(),
)

SOURCE_STAC_ITEM_URL = "https://data.geo.admin.ch/api/stac/v0.9/collections/ch.bfe.solarenergie-eignung-daecher/items/solarenergie-eignung-daecher"
SOURCE_STAC_ASSET_KEY = "solarenergie-eignung-daecher_2056.gdb.zip"


def parse_stac_item(stac_item_url: str, stac_asset_key: str) -> (str, str):
    """
    Parse stac item and return the asset download url and the version,
    which is the updated timestamp in format YYYYMMDD."""
    stac_item = fetch(stac_item_url).json()
    asset = stac_item["assets"][stac_asset_key]
    version = datetime.strptime(asset["updated"], "%Y-%m-%dT%H:%M:%S.%fZ").strftime(
        "%Y%m%d"
    )
    return asset["href"], version


def main():
    print(f"{datetime.now():%Y%m%d-%H:%M:%S} Start script {__file__}")
    download_url, version = parse_stac_item(SOURCE_STAC_ITEM_URL, SOURCE_STAC_ASSET_KEY)
    if version not in OUTPUT_FILE.list_versions():
        print(
            f"New version available: {version}. Download from {download_url} and save as {OUTPUT_FILE.absolute_path(version)}"
        )
        OUTPUT_FILE.write(fetch(download_url).content, version, "wb")
        assert OUTPUT_FILE.exists(version)
    else:
        print(
            f"File {OUTPUT_FILE.file_path} with version {version} already exists. No need to download. Available file versions: {OUTPUT_FILE.list_versions()}"
        )
    print(f"{datetime.now():%Y%m%d-%H:%M:%S} All done!")


if __name__ == "__main__":
    main()
