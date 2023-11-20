# -*- coding: utf-8 -*-
"""
This script promotes data about solar potential of Switzerland's roof from
landing zone to refined zone by extracting the relevant data and performing
the following transformations:
  - Assert that the filegeodatabase contains the layer of interest (SOLKAT_CH_DACH).
  - Assert that the layer is in the expected coordinate system (EPSG:2056).
  - Find and fix invalid geometries. Drop unfixable geometries.
  - Promote all geometries from polygons to multipolygons.
  - Convert the layer to FlatGeoBuff format and upload to refined zone.

A note on ogr2ogr:
We use ogr2ogr for the file IO and transformations. og2ogr is a powerful
swiss-army-like tool with many options (https://gdal.org/programs/ogr2ogr.html).
We write the versioned file directly via ogr2ogr by using the gdal_path method to
obtain the versioned path, not via the VersionedFile's write method.
There are more efficient ways (e.g. using gsutil with -m flag) but for now this is
simple and good enough.

A note on dependency management:
Whenever this script runs, we save the version of the input file that was used
in the metadata of the output file under a key "compute_dependencies". The value is a
dict with key of input file_name and value of input version.
In order to determine whether the script needs to run, we compare the current input version
to the one saved in the metadata of the output file.
"""

from utils_io import (
    GoogleCloudStorageStorageBackend,
    LocalStorageBackend,
    File,
    VersionedFile,
    YYYYMMDDFilenamePrefix,
)
from utils import (
    is_calculation_required,
    update_dependency_version_log_metafile_with_latest,
)
import subprocess
from datetime import datetime

source_storage_backend = GoogleCloudStorageStorageBackend(
    bucket_name="folimar-geotest-store001",
    root_directory="landing/ch.bfe.solarenergie-eignung/",
)

# sink_storage_backend = GoogleCloudStorageStorageBackend(
#     bucket_name="folimar-geotest-store001",
#     root_directory="refined/ch.bfe.solarenergie-eignung/",
# )

sink_storage_backend = LocalStorageBackend(
    root_directory="./data/refined/ch.bfe.solarenergie-eignung/",
)

SOURCE_FILE = VersionedFile(
    file_path="solarenergie-eignung-daecher_2056.gdb.zip",
    storage_backend=source_storage_backend,
    versioning_scheme=YYYYMMDDFilenamePrefix(),
)
SOURCE_FILE_GDB_LAYERNAME = "SOLKAT_CH_DACH"

SINK_FILE = VersionedFile(
    file_path="solarenergie-eignung-daecher_2056.fgb",
    storage_backend=sink_storage_backend,
    versioning_scheme=YYYYMMDDFilenamePrefix(),
)


def main():
    print(f"{datetime.now():%Y%m%d-%H:%M:%S} Start script {__file__}")
    if is_calculation_required(SINK_FILE, [SOURCE_FILE]):
        input_version = SOURCE_FILE.get_latest_version()
        # Set output version to today's date.
        output_version = datetime.now().strftime("%Y%m%d")
        command = [
            "ogr2ogr",
            "-progress",
            "--debug",
            "OFF",
            "-makevalid",
            "-skipfailures",
            "-t_srs",
            "EPSG:2056",
            "-nlt",
            "PROMOTE_TO_MULTI",
            "--config",
            "OGR_SKIP",
            "FileGDB",
            SINK_FILE.gdal_path(output_version),
            f"/vsizip/{SOURCE_FILE.gdal_path(input_version)}",  # use /vsizip/ to read from zip file
            SOURCE_FILE_GDB_LAYERNAME,
        ]
        print(f"Running shell command: {' '.join(command)}")
        subprocess.run(command)

        update_dependency_version_log_metafile_with_latest(SINK_FILE, [SOURCE_FILE])

    print(f"{datetime.now():%Y%m%d-%H:%M:%S} All done!")


if __name__ == "__main__":
    main()
