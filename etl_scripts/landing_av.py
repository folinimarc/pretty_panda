# -*- coding: utf-8 -*-
# The data of the "Amtliche Vermessung (AV) Switzerland" is provided
# through many (>1200) zip files, each covering a specific area. For each area, two
# zip files exist with identical content differing in the data format. We are only
# interested in the zip files containing shapefiles (SHP) and ignore the ITF ones.
# Each zip file contains many data layers represented by shapefiles. Each zip
# file download url is associated with an asof timestamp, provided in a meta.txt file.
# The zip files are updated individually. We are only interested in a
# small subset of the data layers, but the ones of interest
# should be available in the latest version across whole Switzerland.

# The script does the following:
# - Fetch the meta.txt file from the source url and create a set of expected zip blob names.
# - Delete zip files that do not exist anymore in the new metadata or are outdated.
# - Fetch and upload zip files which are new or were updated.

from utils_gcs import write_blob, list_blobs, delete_blob
from utils_misc import fetch

# Constants for URLs and storage locations
SOURCE_META_URL = (
    "https://data.geo.admin.ch/ch.swisstopo-vd.amtliche-vermessung/meta.txt"
)
SINK_LOCATION = "folimar-geotest-store001/landing/ch.swisstopo-vd.amtliche-vermessung"


def get_expected_zip_blob_names(meta_url: str) -> (set, dict):
    """
    Parse meta file and create a set of expected zip file paths.
    The full zip blob names are constructed from url and timestamp by
    joining the last 4 parts of the url by underscores and prefixing it
    with the timestamp in the format YYYYMMDD and the sink location.
    Example:
    1) Meta file entry (url timestamp):
    https://data.geo.admin.ch/ch.swisstopo-vd.amtliche-vermessung/DM01AVCH24D/ITF/AG/4022.zip 2023-11-16
    2) Sink location: folimar-geotest-store001/landing/ch.swisstopo-vd.amtliche-vermessung
    3) Resulting full blob name: folimar-geotest-store001/landing/ch.swisstopo-vd.amtliche-vermessung/20231116_DM01AVCH24D_ITF_AG_4022.zip

    Return a set of all blob names and a dictionary with blob names as keys and urls as values.
    """
    meta_text = fetch(meta_url).text
    blob_names = set()
    blob_names_url_map = {}
    for line in meta_text.splitlines():
        zip_url, date_str = line.split(" ")
        # Only include urls containing SHP
        if "SHP" in zip_url:
            url_part = "_".join(zip_url.rsplit("/", 4)[1:])
            date_part = date_str.replace("-", "")
            blob_name = f"{SINK_LOCATION}/{date_part}_{url_part}"
            blob_names.add(blob_name)
            blob_names_url_map[blob_name] = zip_url
    return blob_names, blob_names_url_map


def get_existing_zip_blob_names(sink_location: str) -> set:
    """
    Retrieve the list of currently stored zip files.
    """
    return set(b for b in list_blobs(sink_location) if b.endswith(".zip"))


def remove_outdated_zips(existing_zips: set, expected_zips: set) -> None:
    """
    Identify and remove outdated or deleted zip files.
    """
    to_delete = existing_zips - expected_zips
    total_to_delete = len(to_delete)
    for i, blob_name in enumerate(to_delete, 1):
        print(f"{i}/{total_to_delete} Delete: {blob_name}")
        delete_blob(blob_name)


def fetch_and_upload_zips(
    expected_zips: set, existing_zips: set, blobname_url_map: dict
) -> None:
    """
    Fetch and store the new or updated zip files.
    """
    to_fetch = expected_zips - existing_zips
    total_to_fetch = len(to_fetch)
    for i, blob_name in enumerate(to_fetch, 1):
        print(f"{i}/{total_to_fetch} Fetch and upload: {blob_name}")
        url = blobname_url_map[blob_name]
        write_blob(blob_name, fetch(url).content, "wb")


def main():
    # Extract expected zip locations from metadata
    expected_zips, blobname_url_map = get_expected_zip_blob_names(SOURCE_META_URL)

    # Retrieve the list of currently stored zip files
    existing_zips = get_existing_zip_blob_names(SINK_LOCATION)

    # Identify and remove outdated or deleted zip files
    remove_outdated_zips(existing_zips, expected_zips)

    # Fetch and store the new or updated zip files
    fetch_and_upload_zips(expected_zips, existing_zips, blobname_url_map)

    print("All done.")


if __name__ == "__main__":
    main()
