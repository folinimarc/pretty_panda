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
# - Fetch the meta.txt file from the source url
# - Check if previously downloaded zip files exist which do not appear anymore in the meta.txt file. If so, delete them.
# - Compare the meta.txt file with the previously fetched one if it exists.
# - Decide on which zip files need to be downloaded.
# - Download the zip files and save them in the landing zone, overwriting existing ones.
# - Save the meta.txt file in the landing zone, overwriting existing one.

from datetime import datetime
from utils_gcs import read_blob, write_blob, list_blobs, delete_blob
from utils_misc import fetch

# Constants for URLs and storage locations
SOURCE_META_URL = (
    "https://data.geo.admin.ch/ch.swisstopo-vd.amtliche-vermessung/meta.txt"
)
SINK_LOCATION = "folimar-geotest-store001/landing/ch.swisstopo-vd.amtliche-vermessung"
META_FILE_NAME = "meta.txt"


def get_zip_id(url_or_blob_name):
    """
    Extract the ZIP file ID from a URL or blob name.
    Given an URL the ID are the last 4 parts of the url joined by underscores. Example:
    URL: https://data.geo.admin.ch/ch.swisstopo-vd.amtliche-vermessung/DM01AVCH24D/ITF/AG/4022.zip
    ID: DM01AVCH24D_ITF_AG_4022.zip

    Given a blob name, the ID is the last part of the blob name. Example:
    BLOB NAME: folimar-geotest-store001/landing/ch.swisstopo-vd.amtliche-vermessung/DM01AVCH24D_ITF_AG_4022.zip
    ID: DM01AVCH24D_ITF_AG_4022.zip
    """
    if "http" in url_or_blob_name:
        return "_".join(url_or_blob_name.rsplit("/", 4)[1:])
    else:
        return url_or_blob_name.rsplit("/", 1)[-1]


def parse_meta_data(meta_text):
    """Parse meta data into a dictionary mapping ZIP file URLs to their last update dates."""
    meta = {}
    for line in meta_text.splitlines():
        zip_url, date_str = line.split(" ")
        meta[zip_url] = datetime.strptime(date_str, "%Y-%m-%d")
    return meta


def should_download(new_date, existing_date, url):
    """
    Determine whether a ZIP file should be downloaded.
    Fow download consider ZIP files containing shapefiles (SHP) which are newer
    than the existing ones or have not yet been downloaded.
    """
    return "SHP" in url and (existing_date is None or new_date > existing_date)


def fetch_and_save_zip(url):
    """Download a ZIP file and append its data to the meta lines."""
    print(f"Downloading ZIP file from URL: {url}")
    response = fetch(url)
    zip_location = f"{SINK_LOCATION}/{get_zip_id(url)}"
    write_blob(zip_location, response.content, "wb")


def delete_old_zips(existing_zip_ids, new_zip_ids):
    """Delete ZIP files that are not in the new meta data."""
    zips_to_delete = existing_zip_ids - new_zip_ids
    for zip_id in zips_to_delete:
        zip_location = f"{SINK_LOCATION}/{zip_id}"
        print(f"Delete ZIP file which is not in new meta anymore: {zip_location}")
        delete_blob(zip_location)


def upload_meta_file(new_meta_lines):
    """Update the meta file with the new meta data."""
    print("Upload new meta file...")
    new_meta_text = "\n".join(new_meta_lines)
    write_blob(f"{SINK_LOCATION}/{META_FILE_NAME}", new_meta_text, "w")


def main():
    print("Fetch new meta data...")
    new_meta_text = fetch(SOURCE_META_URL).text
    new_meta = parse_meta_data(new_meta_text)

    print("Delete old ZIP files no longer present in the new meta data...")
    existing_zip_ids = {
        get_zip_id(blob_name)
        for blob_name in list_blobs(SINK_LOCATION)
        if blob_name.endswith(".zip")
    }
    new_zip_ids = {get_zip_id(url) for url in new_meta.keys()}
    delete_old_zips(existing_zip_ids, new_zip_ids)

    print("Fetch existing meta data...")
    existing_meta_text = read_blob(f"{SINK_LOCATION}/{META_FILE_NAME}")
    existing_meta = parse_meta_data(existing_meta_text) if existing_meta_text else {}

    print("Fetch and save new or updated ZIP files...")
    # Continuously build the new meta file and upload every 10 zip downloads to make the process fail tolerant.
    # If something goes wrong, the script can be restarted and will continue where it left off.
    new_meta_lines = []
    counter = 0
    for url, new_date in new_meta.items():
        new_meta_lines.append(f"{url} {new_date.strftime('%Y-%m-%d')}")
        existing_date = existing_meta.get(url)
        if should_download(new_date, existing_date, url):
            fetch_and_save_zip(url)
            counter += 1
            if counter % 10 == 0:
                upload_meta_file(new_meta_lines)
        else:
            print(f"SKIP {url}")

    upload_meta_file(new_meta_lines)
    print("Script execution completed.")


if __name__ == "__main__":
    main()
