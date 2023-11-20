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

The script does the following:
- Fetch the meta.txt file from the source url and create a set of expected zip blob names.
- Delete zip files that do not exist anymore in the new metadata or are outdated.
- Fetch and upload zip files which are new or were updated.
"""

from typing import List, Tuple
from utils_io import (
    LocalStorageBackend,
    GoogleCloudStorageStorageBackend,
    VersionedFile,
    YYYYMMDDFilenamePrefix,
)
from utils import fetch
from datetime import datetime

sink_storage_backend = GoogleCloudStorageStorageBackend(
    bucket_name="folimar-geotest-store001",
    root_directory="landing/ch.swisstopo-vd.amtliche-vermessung/",
)

# sink_storage_backend = LocalStorageBackend(
#     root_directory="./data/landing/ch.swisstopo-vd.amtliche-vermessung/",
# )

# Constants for URLs and storage locations
SOURCE_META_URL = (
    "https://data.geo.admin.ch/ch.swisstopo-vd.amtliche-vermessung/meta.txt"
)
SINK_META_FILE = VersionedFile(
    file_path="meta.txt",
    storage_backend=sink_storage_backend,
    versioning_scheme=YYYYMMDDFilenamePrefix(),
)


def parse_meta(meta_url: str) -> List[Tuple[str, str, str]]:
    """
    Parse metadata and return a list of tuples with the filename,
    version (format yyyymmdd) and url. Only consider urls with string "SHP",
    because we are only interested in shapefiles.
    The filename is constructed by joining the last 4 parts of the url
    by underscores. Example:
    - Meta file line: https://data.geo.admin.ch/ch.swisstopo-vd.amtliche-vermessung/DM01AVCH24D/SHP/AG/4022.zip 2023-11-16
    - Filename: DM01AVCH24D_SHP_AG_4022.zip
    """
    meta_text = fetch(meta_url).text
    items = []
    for line in meta_text.splitlines():
        zip_url, date_str = line.split(" ")
        # Only include urls containing SHP
        if "SHP" in zip_url:
            filename = "_".join(zip_url.rsplit("/", 4)[1:])
            yyyymmdd_version = date_str.replace("-", "")
            items.append((filename, yyyymmdd_version, zip_url))
    return items


def main():
    print(f"{datetime.now():%Y%m%d-%H:%M:%S} Start script {__file__}")
    # Parse meta file and create a list of tuples with filename, version and url.
    meta_items = parse_meta(SOURCE_META_URL)
    total_items = len(meta_items)
    for i, (filename, yyyymmdd_version, url) in enumerate(meta_items, 1):
        # Create versioned file object for each zip file.
        f = VersionedFile(
            file_path=filename,
            storage_backend=sink_storage_backend,
            versioning_scheme=YYYYMMDDFilenamePrefix(),
        )
        # Check if a file with the version from the meta file already exists. If not, download and upload it.
        if yyyymmdd_version not in f.list_versions():
            print(
                f"{i}/{total_items} New version available: {yyyymmdd_version} for {filename}. Download from {url} and save as {f.absolute_path(yyyymmdd_version)}"
            )
            f.write(fetch(url).content, yyyymmdd_version, "wb")
            assert f.exists(yyyymmdd_version)
        else:
            print(
                f"{i}/{total_items} Version {yyyymmdd_version} already exists for {filename}. Skipping download."
            )

    # Upload meta file with todays date as version. This is necessary to determine in
    # future processing, which zip archives to use.
    meta_version = datetime.now().strftime("%Y%m%d")
    meta_text = "\n".join(" ".join(item) for item in meta_items)
    SINK_META_FILE.write(meta_text, meta_version, "w")

    print(f"{datetime.now():%Y%m%d-%H:%M:%S} All done!")


if __name__ == "__main__":
    main()
