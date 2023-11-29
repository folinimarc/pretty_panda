# -*- coding: utf-8 -*-
"""
The data of the "Amtliche Vermessung (AV) Switzerland" is provided
through many (>1200) zip files, each covering a specific area. For each area, two
zip files exist with identical content differing in the data format. We are only
interested in the zip files containing shapefiles (SHP) and ignore the ITF ones.
Each zip file contains many data layers represented by shapefiles. Each zip
file download url is associated with an asof timestamp, provided in a meta.txt file.
The zip files are updated individually. We are only interested in a
small subset of the data layers, but the ones of interest
should be available in the latest version across whole Switzerland.

Note: For some cantons, data is not freely available but needs an applicationa
and green-light. See here: https://geodienste.ch/services/av
"""

from util import PandaPath, extract_zip_file
import geopandas as gpd
import pandas as pd
from datetime import datetime
from multiprocessing.pool import ThreadPool, Pool
import multiprocessing as mp
import subprocess
from typing import List, Dict, Set, Tuple, Iterable
import logging

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

SOURCE_META_URL = PandaPath(
    "https://data.geo.admin.ch/ch.swisstopo-vd.amtliche-vermessung/meta.txt"
)

SCRATCH_FOLDER = PandaPath(
    "/workspaces/pretty_panda/data/scratch/ch.swisstopo-vd.amtliche-vermessung"
)

SINK_FOLDER = PandaPath(
    "/workspaces/pretty_panda/data/landing/ch.swisstopo-vd.amtliche-vermessung"
)
SINK_FILE_GEBAEUDEFLAECHE = SINK_FOLDER / "gebaeudeflaeche.fgb"
SINK_FILE_GEBAEUDENUMMER = SINK_FOLDER / "gebaeudenummer.fgb"
SINK_FILE_LIEGENSCHAFT = SINK_FOLDER / "liegenschaft.fgb"


def parse_meta(sink_raw_folder: PandaPath) -> List[Dict[str, PandaPath]]:
    """
    Parse metadata and return a list of tuples with the zip source and sink paths.
    Only consider urls with string "SHP", because we are only interested
    in shapefiles. The full filename is constructed by joining the last 4 parts of the url
    by underscores and a timestamp prefix in format YYYYMMDD. Example:
    - Meta file line: https://data.geo.admin.ch/ch.swisstopo-vd.amtliche-vermessung/DM01AVCH24D/SHP/AG/4022.zip 2023-11-16
    - Filename: 20231116_DM01AVCH24D_SHP_AG_4022.zip
    """
    meta_text = PandaPath(SOURCE_META_URL).read_text()
    zip_sink_source_map = {}
    for line in meta_text.splitlines():
        zip_url, date_str = line.split(" ")
        if "SHP" in zip_url:
            asof = datetime.strptime(date_str, "%Y-%m-%d")
            filename = "_".join(zip_url.rsplit("/", 4)[1:])
            timestamp = asof.strftime("%Y%m%d")
            zip_sink = sink_raw_folder / f"{timestamp}_{filename}"
            zip_source = PandaPath(zip_url)
            zip_sink_source_map[zip_sink] = zip_source
    return zip_sink_source_map


def delete_outdated_zip(
    existing: Set[PandaPath], zip_sink_source_map: Dict[PandaPath, PandaPath]
) -> None:
    """
    Delete zip files that do not exist anymore in the new metadata or are outdated.
    """
    incoming = set(zip_sink_source_map.keys())
    to_delete = existing.difference(incoming)
    for p in to_delete:
        p.unlink()


def download_new_zip(
    existing: List[PandaPath], zip_sink_source_map: Dict[PandaPath, PandaPath]
) -> None:
    """
    Download zip files that are new or were updated.
    Do this multithreadedd.
    """

    def _download(args: Tuple[PandaPath, PandaPath]) -> None:
        sink_path, source_path = args
        sink_path.write_bytes(source_path.read_bytes())
        return f"Saved {sink_path.name}"

    incoming = set(zip_sink_source_map.keys())
    to_download = incoming.difference(existing)
    sink_source_args = [
        (sink_path, zip_sink_source_map[sink_path]) for sink_path in to_download
    ]
    with ThreadPool(8) as pool:
        for i, result_msg in enumerate(
            pool.imap_unordered(_download, sink_source_args), 1
        ):
            logging.info(f"{i}/{len(sink_source_args)} {result_msg}")


def common_gdf_processing(
    gdf: gpd.GeoDataFrame, expected_geom_types=Iterable[str]
) -> gpd.GeoDataFrame:
    expected_geom_types = set(expected_geom_types)
    # Assert crs is CH1903+/LV95 (epsg 2056).
    if not gdf.crs:
        gdf = gdf.set_crs("epsg:2056")
    assert (
        gdf.crs.to_epsg() == 2056
    ), f"CRS is not CH1903+/LV95 (epsg 2056) but {gdf.crs}"

    # Fix invalid geometries
    gdf = gdf.set_geometry(gdf.geometry.make_valid())

    # Some files contain geomtry collections, for example as a result of make_valid.
    # Explode them and only retain expected geometry types.
    gdf_only_geomcollection = gdf[gdf.geom_type == "GeometryCollection"]
    gdf_not_geomcollection = gdf[gdf.geom_type != "GeometryCollection"]
    gdf_only_geomcollection = gdf_only_geomcollection.explode(ignore_index=True)
    gdf = pd.concat(
        [gdf_not_geomcollection, gdf_only_geomcollection], ignore_index=True
    )
    gdf = gdf[gdf.geom_type.isin(expected_geom_types)]

    # Drop invalid and empty geometries
    gdf = gdf[gdf.geometry.is_valid & ~(gdf.geometry.isna() | gdf.geometry.is_empty)]

    return gdf


def create_gebaeudeflaeche_chunk(zip_extract_folder: PandaPath) -> None:
    p = list(zip_extract_folder.glob("**/de/Bo_BoFlaeche_A.shp"))
    if not p:
        return None
    gdf = gpd.read_file(p[0], engine="pyogrio")
    # Only work with buildings
    gdf = gdf[gdf["ART_TXT"] == "Gebaeude"]
    # Add information about where data came from
    gdf["av_source"] = zip_extract_folder.stem
    # Common processing
    gdf = common_gdf_processing(gdf, expected_geom_types=["Polygon", "MultiPolygon"])
    assert (
        gdf.geometry.is_valid.all()
    ), f"Found invalid geometries {zip_extract_folder.stem}"
    return gdf if len(gdf) > 0 else None


def create_gebaeudenummer_chunk(zip_extract_folder: PandaPath) -> None:
    p = list(zip_extract_folder.glob("**/de/Bo_GebaeudenummerPos.shp"))
    if not p:
        return None
    gdf = gpd.read_file(p[0], engine="pyogrio")
    gdf["av_source"] = zip_extract_folder.stem
    gdf = common_gdf_processing(gdf, expected_geom_types=["Point", "MultiPoint"])
    assert (
        gdf.geometry.is_valid.all()
    ), f"Found invalid geometries {zip_extract_folder.stem}"
    return gdf if len(gdf) > 0 else None


def create_liegenschaft_chunk(zip_extract_folder: PandaPath) -> None:
    p = list(zip_extract_folder.glob("**/de/Li_Liegenschaft_A.shp"))
    if not p:
        return None
    gdf = gpd.read_file(p[0], engine="pyogrio")
    gdf["av_source"] = zip_extract_folder.stem
    gdf = common_gdf_processing(gdf, expected_geom_types=["Polygon", "MultiPolygon"])
    assert (
        gdf.geometry.is_valid.all()
    ), f"Found invalid geometries {zip_extract_folder.stem}"
    return gdf if len(gdf) > 0 else None


def process_zip(args) -> str:
    zip_path, tmp_gebaeudeflaeche, tmp_gebaeudenummer, tmp_liegenschaft = args
    # Extract zip
    zip_extract_folder = extract_zip_file(zip_path, SCRATCH_FOLDER / "zip")
    # Gebaudeflaeche
    gdf = create_gebaeudeflaeche_chunk(zip_extract_folder)
    if gdf is not None:
        p = tmp_gebaeudeflaeche / f"{zip_path.stem}.gpkg"
        gdf.to_file(p, engine="pyogrio", SPATIAL_INDEX="NO")
    # Gebaudenummer
    gdf = create_gebaeudenummer_chunk(zip_extract_folder)
    if gdf is not None:
        p = tmp_gebaeudenummer / f"{zip_path.stem}.gpkg"
        gdf.to_file(p, engine="pyogrio", SPATIAL_INDEX="NO")
    # Liegenschaft
    gdf = create_liegenschaft_chunk(zip_extract_folder)
    if gdf is not None:
        p = tmp_liegenschaft / f"{zip_path.stem}.gpkg"
        gdf.to_file(p, engine="pyogrio", SPATIAL_INDEX="NO")
    return f"Processed {zip_path.name}"


def combine(chunks_folder) -> str:
    tmp_combined_file = chunks_folder.parent / "combined.gpkg"
    file_chunks = list(chunks_folder.glob("*.gpkg"))
    for file_chunk in file_chunks:
        print(f"Combining {file_chunk}")
        command = [
            "ogr2ogr",
            "-update",
            "-append",
            "--debug",
            "OFF",
            "-lco",
            "SPATIAL_INDEX=NO",
            tmp_combined_file.as_gdal(),
            file_chunk.as_gdal(),
        ]
        subprocess.run(command, check=True)
    return f"Combined {chunks_folder}"


def upload(args) -> str:
    chunks_folder, sink_file = args
    tmp_combined_file = chunks_folder.parent / "combined.gpkg"
    command = [
        "ogr2ogr",
        "--debug",
        "OFF",
        "-nln",
        sink_file.stem,
        "-nlt",
        "PROMOTE_TO_MULTI",
        sink_file.as_gdal(),
        tmp_combined_file.as_gdal(),
    ]
    subprocess.run(command, check=True)
    return f"Uploaded {tmp_combined_file} to {sink_file}"


def landing__av():
    logging.info(f"Start {__name__}")

    logging.info(f"Gathering existing zip files...")
    sink_raw_folder = SINK_FOLDER / "raw"
    sink_raw_folder.mkdir(parents=True, exist_ok=True)
    existing_zip = set(sink_raw_folder.glob("*.zip"))

    logging.info(f"Reading meta data...")
    zip_sink_source_map = parse_meta(sink_raw_folder)

    logging.info(f"Delete outdated zip files...")
    delete_outdated_zip(existing_zip, zip_sink_source_map)

    logging.info(f"Download zip files...")
    download_new_zip(existing_zip, zip_sink_source_map)

    logging.info(f"Extract data from zip files...")
    tmp_gebaeudeflaeche = SCRATCH_FOLDER / "gebaeudeflaeche" / "chunks"
    tmp_gebaeudeflaeche.parent.clean_dir()
    tmp_gebaeudeflaeche.mkdir(parents=True, exist_ok=True)
    tmp_gebaeudenummer = SCRATCH_FOLDER / "gebaeudenummer" / "chunks"
    tmp_gebaeudenummer.parent.clean_dir()
    tmp_gebaeudenummer.mkdir(parents=True, exist_ok=True)
    tmp_liegenschaft = SCRATCH_FOLDER / "liegenschaft" / "chunks"
    tmp_liegenschaft.parent.clean_dir()
    tmp_liegenschaft.mkdir(parents=True, exist_ok=True)

    args = [
        (zip_path, tmp_gebaeudeflaeche, tmp_gebaeudenummer, tmp_liegenschaft)
        for zip_path in sink_raw_folder.glob("*.zip")
    ]
    with Pool(processes=mp.cpu_count() - 1) as pool:
        for i, result_msg in enumerate(pool.imap_unordered(process_zip, args), 1):
            logging.info(f"{i}/{len(args)} {result_msg}")

    logging.info(f"Combine...")
    tmp_sink_map = {
        tmp_gebaeudeflaeche: SINK_FILE_GEBAEUDEFLAECHE,
        tmp_gebaeudenummer: SINK_FILE_GEBAEUDENUMMER,
        tmp_liegenschaft: SINK_FILE_LIEGENSCHAFT,
    }
    args = list(tmp_sink_map.keys())
    with Pool(processes=3) as pool:
        for i, result_msg in enumerate(pool.imap_unordered(combine, args), 1):
            logging.info(f"{i}/{len(args)} {result_msg}")

    logging.info(f"Upload...")
    args = [kv for kv in tmp_sink_map.items()]
    with Pool(processes=3) as pool:
        for i, result_msg in enumerate(pool.imap_unordered(upload, args), 1):
            logging.info(f"{i}/{len(args)} {result_msg}")

    logging.info(f"All done!")


if __name__ == "__main__":
    landing__av()
