# -*- coding: utf-8 -*-
"""
Download OSM data from Geofabrik:
http://download.geofabrik.de/europe/switzerland.html
"""

from util import PandaPath, extract_zip_file, retry
from multiprocessing.pool import Pool
import subprocess
from typing import Set
import logging
import multiprocessing as mp

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

SOURCE_FILE = PandaPath(
    "http://download.geofabrik.de/europe/switzerland-latest-free.shp.zip"
)

SCRATCH_FOLDER = PandaPath("/workspaces/pretty_panda/data/scratch/de.geofabrik")

SINK_FOLDER = PandaPath("/workspaces/pretty_panda/data/landing/de.geofabrik")

FILES_TO_EXTRACT = {
    "gis_osm_buildings_a_free_1.shp": SINK_FOLDER / "gis_osm_buildings_a_free_1.fgb",
    "gis_osm_landuse_a_free_1.shp": SINK_FOLDER / "gis_osm_landuse_a_free_1.fgb",
    "gis_osm_natural_a_free_1.shp": SINK_FOLDER / "gis_osm_natural_a_free_1.fgb",
    "gis_osm_places_a_free_1.shp": SINK_FOLDER / "gis_osm_places_a_free_1.fgb",
    "gis_osm_pofw_free_1.shp": SINK_FOLDER / "gis_osm_pofw_free_1.fgb",
    "gis_osm_pois_a_free_1.shp": SINK_FOLDER / "gis_osm_pois_a_free_1.fgb",
    "gis_osm_railways_free_1.shp": SINK_FOLDER / "gis_osm_railways_free_1.fgb",
    "gis_osm_roads_free_1.shp": SINK_FOLDER / "gis_osm_roads_free_1.fgb",
    "gis_osm_traffic_a_free_1.shp": SINK_FOLDER / "gis_osm_traffic_a_free_1.fgb",
    "gis_osm_transport_a_free_1.shp": SINK_FOLDER / "gis_osm_transport_a_free_1.fgb",
    "gis_osm_water_a_free_1.shp": SINK_FOLDER / "gis_osm_water_a_free_1.fgb",
    "gis_osm_waterways_free_1.shp": SINK_FOLDER / "gis_osm_waterways_free_1.fgb",
    "README": SINK_FOLDER / "README.txt",
}


@retry
def extract_and_upload(args) -> str:
    extract_folder, file_in_zip, sink_path = args
    file_path = list(extract_folder.glob(f"**/{file_in_zip}"))
    assert len(file_path) == 1, f"Expected 1 file, got {len(file_path)}: {file_in_zip}"
    file_path = file_path[0]
    if file_path.name == "README":
        file_path.copy_to(sink_path, bytes=False)
    else:
        sink_path.parent.mkdir(parents=True, exist_ok=True)
        sink_path.unlink(missing_ok=True)
        command = [
            "ogr2ogr",
            "--debug",
            "OFF",
            "-makevalid",
            "-t_srs",
            "EPSG:2056",
            "-nlt",
            "PROMOTE_TO_MULTI",
            "-nln",
            sink_path.stem,
            sink_path.as_gdal(),
            file_path.as_gdal(),
        ]
        subprocess.run(command, check=True)
    return f"Extracted {file_path} to {sink_path}."


def landing__osm():
    logging.info(f"Start {__name__}")

    # https://github.com/fsspec/gcsfs/issues/379
    mp.set_start_method("spawn")

    sink_raw_file = SINK_FOLDER / "raw" / SOURCE_FILE.name
    if sink_raw_file.exists():
        logging.info(
            f"{sink_raw_file} already exists, skip copying from {SOURCE_FILE}..."
        )
    else:
        logging.info(f"Copy {SOURCE_FILE} to {sink_raw_file}...")
        SOURCE_FILE.copy_to(sink_raw_file)

    logging.info("Cleaning up scratch folder...")
    SCRATCH_FOLDER.clean_dir()

    logging.info(f"Download and extract...")
    extract_folder = extract_zip_file(
        sink_raw_file, SCRATCH_FOLDER / sink_raw_file.stem
    )

    logging.info(f"Extract and upload...")
    args = [
        (extract_folder, file_in_zip, sink_path)
        for file_in_zip, sink_path in FILES_TO_EXTRACT.items()
    ]
    with Pool(processes=min(len(args), mp.cpu_count() - 1)) as pool:
        for i, result_msg in enumerate(
            pool.imap_unordered(extract_and_upload, args), 1
        ):
            logging.info(f"{i}/{len(args)} {result_msg}")

    logging.info("Cleaning up scratch folder...")
    SCRATCH_FOLDER.clean_dir()

    logging.info(f"All done!")


if __name__ == "__main__":
    landing__osm()
