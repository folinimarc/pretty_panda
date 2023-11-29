# -*- coding: utf-8 -*-
"""

"""

from util import PandaPath
import geopandas as gpd
from typing import Tuple
import logging

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

SOURCE_FILE_GEBAEUDE = PandaPath("/workspaces/pretty_panda/data/refined/gebaeude")
SOURCE_FILE_SOLARDAECHER = PandaPath(
    "/workspaces/pretty_panda/data/landing/ch.bfe.solarenergie-eignung/solarenergie-eignung-daecher_2056.fgb"
)

SCRATCH_FOLDER = PandaPath(
    "/workspaces/pretty_panda/data/scratch/solarpotential_daecher"
)

SINK_FILE = PandaPath(
    "/workspaces/pretty_panda/data/refined/solarpotential_daecher/solarpotential_daecher.fgb"
)


def process():
    logging.info(f"Reading data from {SOURCE_FILE_GEBAEUDE}...")
    gdf_gebaeude = gpd.read_file(SOURCE_FILE_GEBAEUDE.as_gdal(), engine="pyogrio")
    assert gdf_gebaeude.crs.to_epsg() == 2056
    assert gdf_gebaeude.geometry.is_valid.all()

    logging.info(f"Reading data from {SOURCE_FILE_SOLARDAECHER}...")
    gdf_solar = gpd.read_file(SOURCE_FILE_SOLARDAECHER.as_gdal(), engine="pyogrio")
    assert gdf_solar.crs.to_epsg() == 2056
    assert gdf_solar.geometry.is_valid.all()

    logging.info(f"Joining AV gebaeudenummer to AV gebaeudeflaeche...")
    gdf_av_flaeche.sindex
    gdf_av_nummer.sindex
    gdf_av_flaeche = gpd.sjoin_nearest(
        gdf_av_flaeche, gdf_av_nummer, how="left", max_distance=10, rsuffix="_av_nummer"
    )
    del gdf_av_nummer

    logging.info(f"Prepare for export")
    gdf_av_flaeche = gdf_av_flaeche[
        gdf_av_flaeche.geom_type.isin(["Polygon", "MultiPolygon"])
    ]

    scratch_file = SCRATCH_FOLDER / "gebaeude.fgb"
    logging.info(f"Writing temprary file to {scratch_file}...")
    SCRATCH_FOLDER.mkdir(parents=True, exist_ok=True)
    gdf_av_flaeche.to_file(scratch_file, engine="pyogrio")

    return scratch_file


def upload(scratch_file: PandaPath):
    logging.info(f"Uploading to {SINK_FILE}...")
    SINK_FILE.parent.mkdir(parents=True, exist_ok=True)
    SINK_FILE.write_bytes(scratch_file.read_bytes())


def refined__solarpotential():
    logging.info(f"Start {__name__}")
    scratch_file = process()
    upload(scratch_file)
    logging.info(f"All done!")


if __name__ == "__main__":
    refined__solarpotential()
