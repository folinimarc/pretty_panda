# -*- coding: utf-8 -*-
"""

"""

from util import PandaPath, get_ch_2056_processing_extents, retry
from typing import Tuple
import geopandas as gpd
import pandas as pd
import subprocess
import logging
import multiprocessing as mp
from multiprocessing.pool import Pool
from functools import partial

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

SOURCE_ROOT = PandaPath("/workspaces/pretty_panda/data/landing")
SOURCE_AV_FLAECHE = (
    SOURCE_ROOT / "ch.swisstopo-vd.amtliche-vermessung/Bo_BoFlaeche_A.fgb"
)
SOURCE_AV_NUMMER = (
    SOURCE_ROOT / "ch.swisstopo-vd.amtliche-vermessung/Bo_GebaeudenummerPos.fgb"
)
SOURCE_GWR = SOURCE_ROOT / "ch.bfs.gwr/gwr_buildings.fgb"
SOURCE_OSM_BUILDINGS = SOURCE_ROOT / "de.geofabrik/gis_osm_buildings_a_free_1.fgb"

SCRATCH_FOLDER = PandaPath("/workspaces/pretty_panda/data/scratch/gebaeude")

SINK_FILE = PandaPath("/workspaces/pretty_panda/data/refined/buildings/buildings.fgb")


def combine_av_and_osm_footprints(
    bbox: Tuple[float, float, float, float]
) -> gpd.GeoDataFrame:

    av_footprints = gpd.read_file(SOURCE_AV_FLAECHE.as_gdal(), bbox=bbox)
    osm_footprints = gpd.read_file(SOURCE_OSM_BUILDINGS.as_gdal(), bbox=bbox)

    # For AV footprints we want only buildings
    av_footprints = av_footprints[av_footprints.ART_TXT == "Gebaeude"]

    # Only keep the columns we need
    av_footprints = av_footprints[
        [
            "OBJID",
            "QUALITAET",
            "QUALITAET_",
            "R1_OBJID",
            "R1_NBIDENT",
            "R1_IDENTIF",
            "R1_BESCHRE",
            "R1_GUELTIG",
            "R1_GUELTI1",
            "R1_GUELTI2",
            "R1_DATUM1",
            "etl_zip_source",
            "geometry",
        ]
    ]
    osm_footprints = osm_footprints[["osm_id", "geometry"]]

    # Prefix columns of av_footprints and osm_footprints (all except geometry)
    av_footprints_prefixed = av_footprints.add_prefix("avFl_")
    av_footprints_prefixed = av_footprints_prefixed.rename(
        columns={"avFl_geometry": "geometry"}
    )
    osm_footprints_prefixed = osm_footprints.add_prefix("osm_")
    osm_footprints_prefixed = osm_footprints_prefixed.rename(
        columns={"osm_geometry": "geometry"}
    )

    # Add a column to indicate the geometry source
    av_footprints_prefixed["etl_geom_source"] = "av"

    # Perform a spatial join to find non-intersecting geometries from osm_footprints
    joined = gpd.sjoin(
        osm_footprints_prefixed,
        av_footprints_prefixed,
        how="left",
        predicate="intersects",
    )
    non_intersecting_osm_footprints = joined[joined["index_right"].isna()]

    # Drop the columns added by sjoin
    non_intersecting_osm_footprints = non_intersecting_osm_footprints.drop(
        columns=[
            col
            for col in non_intersecting_osm_footprints.columns
            if col.startswith("index_")
        ]
    )
    non_intersecting_osm_footprints["etl_geom_source"] = "osm"

    # Concatenate av_footprints and non-intersecting part of osm_footprints
    result_gdf = pd.concat(
        [av_footprints_prefixed, non_intersecting_osm_footprints], ignore_index=True
    )

    return result_gdf


def join_data_to_footprints(
    bbox: Tuple[float, float, float, float], footprints: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    av_points = gpd.read_file(SOURCE_AV_NUMMER.as_gdal(), bbox=bbox)
    gwr_points = gpd.read_file(SOURCE_GWR.as_gdal(), bbox=bbox)

    # Only keep the columns we need
    av_points = av_points[["OBJID", "R1_OBJID", "R1_GEBAEUD", "R1_GWR_EGI", "geometry"]]

    # Prefix columns of av_footprints and osm_footprints (all except geometry)
    av_points_prefixed = av_points.add_prefix("avNr_")
    av_points_prefixed = av_points_prefixed.rename(
        columns={"avNr_geometry": "geometry"}
    )
    gwr_points_prefixed = gwr_points.add_prefix("gwr_")
    gwr_points_prefixed = gwr_points_prefixed.rename(
        columns={"gwr_geometry": "geometry"}
    )

    footprints = gpd.sjoin(
        footprints, av_points_prefixed, how="left", predicate="intersects"
    )
    footprints = footprints.drop(columns=["index_right"])

    footprints = gpd.sjoin(
        footprints, gwr_points_prefixed, how="left", predicate="intersects"
    )
    footprints = footprints.drop(columns=["index_right"])

    return footprints


def process(bbox: Tuple[float, float, float, float], chunks_folder: PandaPath) -> str:
    bbox_str = "".join(str([int(x) for x in bbox]).split()).strip("[]").replace(",", "")
    chunk_path = chunks_folder / f"{bbox_str}.gpkg"
    if chunk_path.exists():
        return f"Chunk {chunk_path} already exists - skip"
    footprints = combine_av_and_osm_footprints(bbox)
    if len(footprints) == 0:
        return "No footprints found"
    footprints = join_data_to_footprints(bbox, footprints)
    footprints.to_file(chunk_path.as_gdal(), driver="GPKG", SPATIAL_INDEX="NO")
    return f"Created {chunk_path}"


@retry
def combine(scratch_folder: PandaPath) -> PandaPath:
    chunks = scratch_folder / "chunks" / "*.gpkg"
    combined_file = scratch_folder / f"combined.gpkg"
    command = [
        "ogrmerge.py",
        "-f",
        "GPKG",
        "-progress",
        "-single",
        "-overwrite_ds",
        "-o",
        combined_file.as_gdal(),
        chunks.as_gdal(),
    ]
    subprocess.run(command, check=True)
    return combined_file


@retry
def upload(source_file: PandaPath, sink_file: PandaPath) -> None:
    sink_file.parent.mkdir(parents=True, exist_ok=True)
    sink_file.unlink(missing_ok=True)
    command = [
        "ogr2ogr",
        "--debug",
        "OFF",
        "-progress",
        "-nln",
        sink_file.stem,
        sink_file.as_gdal(),
        source_file.as_gdal(),
    ]
    subprocess.run(command, check=True)


def refined__buildings():
    logging.info(f"Start {__name__}")

    # https://github.com/fsspec/gcsfs/issues/379
    mp.set_start_method("spawn")

    # logging.info(f"Clean scratch folder {SCRATCH_FOLDER}")
    # SCRATCH_FOLDER.clean_dir()

    logging.info(f"Process chunks...")
    chunks_folder = SCRATCH_FOLDER / "chunks"
    chunks_folder.mkdir(parents=True, exist_ok=True)

    bboxes = list(get_ch_2056_processing_extents(30, 15))
    with Pool(processes=mp.cpu_count() - 1) as pool:
        for i, result_msg in enumerate(
            pool.imap_unordered(partial(process, chunks_folder=chunks_folder), bboxes),
            1,
        ):
            logging.info(f"{i}/{len(bboxes)} {result_msg}")

    logging.info(f"Combine...")
    combined_file = combine(SCRATCH_FOLDER)

    logging.info(f"Upload...")
    upload(combined_file, SINK_FILE)

    logging.info(f"Clean scratch folder {SCRATCH_FOLDER}")
    SCRATCH_FOLDER.clean_dir()

    logging.info(f"All done!")


if __name__ == "__main__":
    refined__buildings()
