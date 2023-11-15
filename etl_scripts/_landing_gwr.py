# -*- coding: utf-8 -*-
import subprocess
import etl_scripts.utils_gcs as u

# Using ogr2ogr with GDAL virtual file systems to read the remote zip file
# and create a flatgeobuff file on cloud storage.
# https://gdal.org/programs/ogr2ogr.html & https://gdal.org/user/virtual_file_systems.html
source_file = (
    "/vsizip//vsicurl/https://public.madd.bfs.admin.ch/ch.zip/buildings.geojson"
)
# target_file = Path("./gwr.gpkg")

target_bucket = "folimar-geotest-store001"
target_blob = "landing/gwr_buildings.fgb"
target_file = f"/vsigs/{target_bucket}/{target_blob}"

print(f"Loading {source_file} to {target_file}...")

print("Deleting target file if it exists...")
u.delete_blob(target_bucket, target_blob)

command = [
    "ogr2ogr",
    "-progress",
    "-makevalid",
    "-skipfailures",
    "--debug",
    "ON",
    target_file,
    source_file,
]
print(f"Running ogr2ogr command: {' '.join(command)}")
subprocess.run(command)

print("All done.")
