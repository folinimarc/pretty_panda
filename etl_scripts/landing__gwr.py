# -*- coding: utf-8 -*-
"""
From: https://www.housing-stat.ch/de/madd/public.html
GWR data has no explicit asof date, but rather seems to be updated daily.
SOURCE_OUTDATED_DAYS specifies how many days old the data can be at most before updating.
"""

from util import PandaPath, get_metadata_asof, set_metadata_asof_now, extract_zip_file
import pandas as pd
import geopandas as gpd
import logging
import subprocess

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

SOURCE_FILE = PandaPath("https://public.madd.bfs.admin.ch/ch.zip")
SOURCE_OUTDATED_DAYS = 14

SCRATCH_FOLDER = PandaPath("/workspaces/pretty_panda/data/scratch/ch.bfs.gwr")

# SINK_FOLDER = PandaPath("gs://folimar-geotest-store001/landing/ch.bfs.gwr")
SINK_FOLDER = PandaPath("/workspaces/pretty_panda/data/landing/ch.bfs.gwr")
SINK_FILE = SINK_FOLDER / "gwr_buildings.fgb"


def create_buildings_file(extracted_zip_folder):
    logging.info(f"Reading building geometries...")
    buildings_path = extracted_zip_folder / "buildings.geojson"
    # Specify Switzerland bbox (xmin, ymin, xmax, ymax) in epsg2056,
    # because the GWR data contains an outlier building in the middle of the ocean >.<
    buildings = gpd.read_file(
        buildings_path, bbox=(2485071, 1074261, 2837119, 1299941), engine="pyogrio"
    )[["egid", "geometry"]]
    assert buildings.crs.to_epsg() == 2056
    assert all(buildings.geometry.is_valid)
    assert not any(buildings["egid"].duplicated())
    buildings.set_index("egid", inplace=True)

    logging.info(f"Reading building data...")
    # Specify dtype for columns where pandas is not able to infer datatype.
    data_path = extracted_zip_folder / "gebaeude_batiment_edificio.csv"
    building_data = pd.read_csv(
        data_path, sep="\t", dtype={"LPARZ": "str", "GEBNR": "str"}
    )
    assert not any(building_data["EGID"].duplicated())
    building_data.set_index("EGID", inplace=True)

    logging.info(f"Join geometries and data...")
    buildings = buildings.join(building_data, how="left").reset_index()

    scratch_file = SCRATCH_FOLDER / "buildings.fgb"
    logging.info(f"Writing temprary file to {scratch_file}...")
    SCRATCH_FOLDER.mkdir(parents=True, exist_ok=True)
    buildings.to_file(scratch_file, engine="pyogrio")

    return scratch_file


def upload(scratch_file: PandaPath):
    logging.info(f"Uploading to {SINK_FILE}...")
    SINK_FILE.parent.mkdir(parents=True, exist_ok=True)
    SINK_FILE.unlink(missing_ok=True)
    command = [
        "ogr2ogr",
        "-progress",
        "--debug",
        "OFF",
        "-makevalid",
        "-nln",
        SINK_FILE.stem,
        "-nlt",
        "PROMOTE_TO_MULTI",
        SINK_FILE.as_gdal(),
        scratch_file.as_gdal(),
    ]
    subprocess.run(command, check=True)


def landing__gwr():
    logging.info(f"Start {__name__}")

    sink_raw_file = SINK_FOLDER / "raw" / SOURCE_FILE.name
    if sink_raw_file.exists():
        logging.info(
            f"{sink_raw_file} already exists, skip copying from {SOURCE_FILE}..."
        )
    else:
        logging.info(f"Copy {SOURCE_FILE} to {sink_raw_file}...")
        SOURCE_FILE.copy_to(sink_raw_file)

    logging.info(f"Extracting zip file to scratch {SCRATCH_FOLDER}...")
    extracted_folder = extract_zip_file(sink_raw_file, SCRATCH_FOLDER)

    logging.info(f"Processing {extracted_folder}...")
    scratch_file = create_buildings_file(extracted_folder)

    logging.info(f"Uploading to {SINK_FILE}...")
    upload(scratch_file)

    logging.info("Cleaning up scratch folder...")
    SCRATCH_FOLDER.clean_dir()

    logging.info(f"All done!")


if __name__ == "__main__":
    landing__gwr()
