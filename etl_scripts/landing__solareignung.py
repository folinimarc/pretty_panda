# -*- coding: utf-8 -*-
"""
For each roof in Switzerland, detailed data about the solar potential is available. The data is provided
by the Swiss Federal Office of Energy (SFOE) and is updated regularly. The data is provided here:
https://www.geocat.ch/geonetwork/srv/ger/catalog.search#/metadata/b614de5c-2f12-4355-b2c9-7aef2c363ad6
We leverage the federal geoportal STAC API to obtain metadata (last updated timestamp) and the asset download urls.
The data is distributed as a single zipped geopackage or a single zipped esri filegeodatabase.
Sadly, it seems that as of 2023-11 something is broken with the geopackage zip compression,
which is why we work with the filegeodatabase >.<
"""

from util import PandaPath, get_metadata_asof, set_metadata_asof_now
import json
from datetime import datetime
from typing import Tuple
import subprocess
import logging

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

SOURCE_STAC_ITEM = PandaPath(
    "https://data.geo.admin.ch/api/stac/v0.9/collections/ch.bfe.solarenergie-eignung-daecher/items/solarenergie-eignung-daecher"
)
SOURCE_STAC_ASSET_KEY = "solarenergie-eignung-daecher_2056.gdb.zip"
SOURCE_FILE_GDB_LAYERNAME = "SOLKAT_CH_DACH"

SINK_FOLDER = PandaPath(
    "gs://folimar-geotest-store001/landing/test_ch.bfe.solarenergie-eignung"
)
# SINK_FOLDER = PandaPath(
#     "/workspaces/pretty_panda/data/landing/ch.bfe.solarenergie-eignung"
# )
SINK_FILE_SOLARDAECHER = SINK_FOLDER / "solarenergie-eignung-daecher_2056.fgb"
SINK_META_FILE = SINK_FOLDER / "processing_metadata.json"


def get_asset_from_stac_item(
    stac_item: PandaPath, stac_asset_key: str
) -> Tuple[PandaPath, datetime]:
    stac_item = json.loads(stac_item.read_text())
    asset = stac_item["assets"][stac_asset_key]
    asof = datetime.strptime(asset["updated"], "%Y-%m-%dT%H:%M:%S.%fZ")
    asset_path = PandaPath(asset["href"])
    return asset_path, asof


def landing__solareignung():
    logging.info(f"Start {__name__}")

    asset_path, asset_asof = get_asset_from_stac_item(
        SOURCE_STAC_ITEM, SOURCE_STAC_ASSET_KEY
    )
    if asset_asof <= get_metadata_asof(SINK_META_FILE):
        logging.info(f"Asset is up to date. No update required.")
        return

    sink_raw_file = SINK_FOLDER / "raw" / asset_path.name
    logging.info(f"Newer asset available. Downloading raw file to {sink_raw_file}...")
    sink_raw_file.parent.mkdir(parents=True, exist_ok=True)
    sink_raw_file.write_bytes(asset_path.read_bytes())

    logging.info(
        f"Extracting layer {SOURCE_FILE_GDB_LAYERNAME} from {sink_raw_file} to {SINK_FILE_SOLARDAECHER}..."
    )
    SINK_FILE_SOLARDAECHER.parent.mkdir(parents=True, exist_ok=True)
    command = [
        "ogr2ogr",
        "-progress",
        "--debug",
        "OFF",
        "-makevalid",
        "-nlt",
        "PROMOTE_TO_MULTI",
        "-a_srs",
        "EPSG:2056",
        SINK_FILE_SOLARDAECHER.as_gdal(),
        sink_raw_file.as_gdal(),
        SOURCE_FILE_GDB_LAYERNAME,
    ]
    subprocess.run(command, check=True)

    logging.info(f"Write metadata to {SINK_META_FILE}...")
    set_metadata_asof_now(SINK_META_FILE)

    logging.info(f"All done!")


if __name__ == "__main__":
    landing__solareignung()
