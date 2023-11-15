# -*- coding: utf-8 -*-
import subprocess
import etl_scripts.utils_gcs as u

# Using ogr2ogr with GDAL virtual file systems to read the remote zip file
# and create a flatgeobuff file on cloud storage.
# https://gdal.org/programs/ogr2ogr.html & https://gdal.org/user/virtual_file_systems.html
source_file = "/vsizip//vsicurl/https://data.geo.admin.ch/ch.bfe.solarenergie-eignung-daecher/solarenergie-eignung-daecher/solarenergie-eignung-daecher_2056.gdb.zip/SOLKAT_DACH_20230221.gdb"
source_file_gdb_layer = "SOLKAT_CH_DACH"

target_bucket = "folimar-geotest-store001"
target_blob = "landing/solarenergie_eignung_daecher_2056.fgb"
target_file = f"/vsigs/{target_bucket}/{target_blob}"

print(f"Loading {source_file} to {target_file}...")

print("Deleting target file if it exists...")
u.delete_blob(target_bucket, target_blob)

command = [
    "ogr2ogr",
    "--debug",
    "ON",
    "-progress",
    "-makevalid",
    "-skipfailures",
    "-nlt",
    "PROMOTE_TO_MULTI",
    "--config",
    "OGR_SKIP",
    "FileGDB",
    target_file,
    source_file,
    source_file_gdb_layer,
]
print(f"Running ogr2ogr command: {' '.join(command)}")
subprocess.run(command)

print("All done.")
