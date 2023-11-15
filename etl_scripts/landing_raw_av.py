# -*- coding: utf-8 -*-
import requests
from datetime import datetime
from utils_gcs import read_blob


source_meta_url = (
    "https://data.geo.admin.ch/ch.swisstopo-vd.amtliche-vermessung/meta.txt"
)
sink_bucket = "folimar-geotest-store001"
sink_objectkey = "landing/raw/ch.swisstopo-vd.amtliche-vermessung"

# Get all previously downloaded zip download urls with the associated asof timestamp
previous_url_timestamp_dict = {}
previous_meta = read_blob(sink_bucket, f"{sink_objectkey}/meta.txt")
if previous_meta:
    previous_meta_dict = {
        l.split(" ")[0]: l.split(" ")[1] for l in previous_meta.splitlines()
    }

# Create list of zip files to fetch
urls_to_fetch = []
for line in requests.get(source_meta_url).text.splitlines():
    zip_url, date = line.split(" ")
    # If current zip url does not contain the string "SHP" (meaning it contains shapefiles) skip it.
    if not "SHP" in zip_url:
        print(f"SKIP because not SHP: {zip_url}")
        continue
    # If current zip url's timestamp is not newer than the previous one, skip it.
    current_meta_timestamp = datetime.strptime(date, "%Y-%m-%d")
    if zip_url in previous_meta_dict:
        previous_meta_timestamp = datetime.strptime(
            previous_meta_dict[zip_url], "%Y-%m-%d"
        )
        if current_meta_timestamp <= previous_meta_timestamp:
            print(f"SKIP because not newer than previous run: {zip_url}")
            continue
    urls_to_fetch.append(zip_url)
print(f"Fetching {len(urls_to_fetch)} zip files...")

# Download and upload to GCS
