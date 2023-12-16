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

from util import PandaPath, extract_zip_file, retry
from datetime import datetime
from multiprocessing.pool import Pool
import subprocess
from typing import List, Dict, Set, Tuple
import logging
import multiprocessing as mp

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

FILES_TO_EXTRACT = {
    PandaPath("de/Bo_BoFlaeche_A.shp"): SINK_FOLDER / "Bo_BoFlaeche_A.fgb",
    PandaPath("de/Bo_GebaeudenummerPos.shp"): SINK_FOLDER / "Bo_GebaeudenummerPos.fgb",
    PandaPath("de/Li_Liegenschaft_A.shp"): SINK_FOLDER / "Li_Liegenschaft_A.fgb",
}


def sink_source_map_from_meta(
    meta_url_path: PandaPath, sink_raw_folder: PandaPath
) -> List[Dict[str, PandaPath]]:
    """
    Parse metadata and return a list of tuples with the zip source and sink paths.
    Only consider urls with string "SHP", because we are only interested
    in shapefiles. The full filename is constructed by joining the last 4 parts of the url
    by underscores and a timestamp prefix in format YYYYMMDD. Example:
    - Meta file line: https://data.geo.admin.ch/ch.swisstopo-vd.amtliche-vermessung/DM01AVCH24D/SHP/AG/4022.zip 2023-11-16
    - Filename: 20231116_DM01AVCH24D_SHP_AG_4022.zip
    """
    meta_text = meta_url_path.read_text()
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


def delete_outdated_zip(incoming: Set[PandaPath], existing: Set[PandaPath]) -> None:
    """
    Delete zip files that do not exist anymore in the new metadata or are outdated.
    """
    to_delete = existing.difference(incoming)
    logging.info(f"Delete {len(to_delete)} outdated zip files...")
    for p in to_delete:
        p.unlink()


@retry
def download_zip(args: Tuple[PandaPath, PandaPath]) -> None:
    sink_path, source_path = args
    source = source_path.read_bytes()
    sink_path.write_bytes(source)
    return f"Saved {sink_path.name}"


@retry
def process_zip(args) -> str:
    zip_path, files_to_extract, scratch_folder = args
    # Extract zip
    zip_extract_folder = extract_zip_file(
        zip_path, scratch_folder / "zip" / zip_path.stem
    )
    for path_within_zip in files_to_extract.keys():
        file_path = zip_extract_folder / path_within_zip
        if not file_path.exists():
            logging.warning(f"File {file_path} does not exist in zip {zip_path.name}")
            continue
        chunk_path = (
            scratch_folder / file_path.stem / "chunks" / f"{zip_path.stem}.gpkg"
        )
        chunk_path.parent.mkdir(parents=True, exist_ok=True)
        command = [
            "ogr2ogr",
            "--debug",
            "OFF",
            "-makevalid",
            "-a_srs",
            "EPSG:2056",
            "-oo",
            "encoding=Windows-1252",  # .cpg files indicate "1252", which ogr2ogr does not seem to understand.
            "-lco",
            "SPATIAL_INDEX=NO",
            "-nlt",
            "PROMOTE_TO_MULTI",
            "-sql",
            f"SELECT *, '{zip_path.stem}' as etl_zip_source FROM \"{file_path.stem}\"",
            chunk_path.as_gdal(),
            file_path.as_gdal(),
        ]
        subprocess.run(command, check=True)
    zip_extract_folder.clean_dir()
    zip_extract_folder.rmdir()
    return f"Processed {zip_path.name}."


@retry
def combine(args) -> str:
    file_scratch_folder = args
    chunks = file_scratch_folder / "chunks" / "*.gpkg"
    combined_file = file_scratch_folder / f"combined.gpkg"
    command = [
        "ogrmerge.py",
        "-f",
        "GPKG",
        "-single",
        "-overwrite_ds",
        "-o",
        combined_file.as_gdal(),
        chunks.as_gdal(),
    ]
    subprocess.run(command, check=True)
    return f"Combined {chunks} into {combined_file}."


@retry
def upload(args) -> str:
    file_scratch_folder, sink_file = args
    tmp_combined_file = file_scratch_folder / "combined.gpkg"
    sink_file.parent.mkdir(parents=True, exist_ok=True)
    sink_file.unlink(missing_ok=True)
    command = [
        "ogr2ogr",
        "--debug",
        "OFF",
        "-nln",
        sink_file.stem,
        sink_file.as_gdal(),
        tmp_combined_file.as_gdal(),
    ]
    subprocess.run(command, check=True)
    return f"Uploaded {tmp_combined_file} to {sink_file}"


def get_existing_zip(sink_raw_folder: PandaPath) -> Set[PandaPath]:
    sink_raw_folder.mkdir(parents=True, exist_ok=True)
    return set(sink_raw_folder.glob("*.zip"))


def landing__av():
    logging.info(f"Start {__name__}")

    # https://github.com/fsspec/gcsfs/issues/379
    mp.set_start_method("spawn")

    logging.info("Cleaning up scratch folder...")
    SCRATCH_FOLDER.clean_dir()

    logging.info(f"Gathering existing zip files...")
    sink_raw_folder = SINK_FOLDER / "raw"
    existing_zip = get_existing_zip(sink_raw_folder)

    logging.info(f"Reading meta data...")
    zip_sink_source_map = sink_source_map_from_meta(SOURCE_META_URL, sink_raw_folder)
    incoming_zip = set(zip_sink_source_map.keys())

    logging.info(f"Download new zip files...")
    to_download = incoming_zip.difference(existing_zip)
    args = [(sink_path, zip_sink_source_map[sink_path]) for sink_path in to_download]
    with Pool(processes=4) as pool:
        for i, result_msg in enumerate(pool.imap_unordered(download_zip, args), 1):
            logging.info(f"{i}/{len(args)} {result_msg}")

    logging.info(f"Delete outdated zip files...")
    delete_outdated_zip(incoming_zip, existing_zip)

    logging.info(f"Extract data from zip files...")
    args = [
        (zip_path, FILES_TO_EXTRACT, SCRATCH_FOLDER)
        for zip_path in sink_raw_folder.glob("*.zip")
    ]
    with Pool(processes=mp.cpu_count() - 1) as pool:
        for i, result_msg in enumerate(pool.imap_unordered(process_zip, args), 1):
            logging.info(f"{i}/{len(args)} {result_msg}")

    logging.info(f"Combine...")
    args = [SCRATCH_FOLDER / f.stem for f in FILES_TO_EXTRACT.keys()]
    with Pool(processes=min(len(args), mp.cpu_count() - 1)) as pool:
        for i, result_msg in enumerate(pool.imap_unordered(combine, args), 1):
            logging.info(f"{i}/{len(args)} {result_msg}")

    logging.info(f"Upload...")
    args = [(SCRATCH_FOLDER / f.stem, sink) for f, sink in FILES_TO_EXTRACT.items()]
    with Pool(processes=min(len(args), mp.cpu_count() - 1)) as pool:
        for i, result_msg in enumerate(pool.imap_unordered(upload, args), 1):
            logging.info(f"{i}/{len(args)} {result_msg}")

    logging.info("Cleaning up scratch folder...")
    SCRATCH_FOLDER.clean_dir()

    logging.info(f"All done!")


if __name__ == "__main__":
    landing__av()
