# -*- coding: utf-8 -*-
import upath
from typing import List, Dict, Set, Tuple, Iterable, Callable, Any
import json
from datetime import datetime
import shutil
import zipfile
import io
import functools
import time


# --------------------------------------------------
# UPATH MIXINS
# We cannot extend the UPath class directly, because
# it delegates to many subclases. Instead, we feel
# a little hacky and patch the UPath class directly
# and expose it as PandaPath in this module.
#
# Import in your code like this:
# from util import PandaPath
# path = PandaPath("gs://my-bucket/my-file.fgb")
# --------------------------------------------------


class ConvenvienceMixin:
    """
    Methods that make life easier.
    """

    def copy_to(self, target: upath.UPath, bytes=True) -> None:
        """Copy a binary file."""
        target.parent.mkdir(parents=True, exist_ok=True)
        if bytes:
            target.write_bytes(self.read_bytes())
        else:
            target.write_text(self.read_text())

    def clean_dir(self) -> None:
        """Cleans a directory by deleting all files and subdirectories."""
        try:
            shutil.rmtree(self / "*")
        except Exception:
            if self.exists:
                for child in self.glob("*"):
                    if child.is_file():
                        child.unlink()
                    else:
                        child.clean_dir()
                        child.rmdir()
            self.mkdir(parents=True, exist_ok=True)


class GDALMixin:
    """
    Mixin for UPath to add GDAL/OGR related methods.
    """

    def as_gdal(self):
        """
        Return path string in GDAL format using virtual file systems.
        Suitable for gdal and ogr command line tools as well as
        python libraries that use GDAL, e.g. Geopandas, rasterio.
        Currently only handles google cloud storage and zip.
        """
        p = self.as_posix()
        p = p.replace("gs://", "/vsigs/").replace("file://", "")
        if ".zip" in p or ".zip/" in p:
            p = "/vsizip/" + p
        return p


class PandaPath(upath.UPath, ConvenvienceMixin, GDALMixin):
    pass


upath.core.UPath = PandaPath

# --------------------------------------------------
# METADATA
# --------------------------------------------------

ASOF_KEY = "as_of"
ASOF_DATE_FORMAT = "%Y%m%d%H%M%S"
ASOF_DATE_MIN = "19700101000000"


def get_metadata_asof(metadata_file: PandaPath) -> datetime:
    metadata = json.loads(metadata_file.read_text()) if metadata_file.exists() else {}
    return datetime.strptime(metadata.get(ASOF_KEY, ASOF_DATE_MIN), ASOF_DATE_FORMAT)


def set_metadata_asof_now(metadata_file: PandaPath) -> None:
    metadata = {
        ASOF_KEY: datetime.now().strftime(ASOF_DATE_FORMAT),
        "as_of_date_format": ASOF_DATE_FORMAT,
    }
    return metadata_file.write_text(json.dumps(metadata))


# --------------------------------------------------
# MISC
# --------------------------------------------------


def extract_zip_file(zip_file: PandaPath, extract_folder: PandaPath) -> PandaPath:
    """Extract a zip file to a folder."""
    extract_folder = extract_folder / zip_file.stem
    extract_folder.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(zip_file.read_bytes())) as z:
        z.extractall(extract_folder)
    return extract_folder


def retry(operation: Callable) -> Callable:
    """Retry an operation a few times before giving up."""

    @functools.wraps(operation)
    def wrapped(*args, **kwargs) -> Any:
        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                return operation(*args, **kwargs)
            except Exception as e:
                if attempt < max_attempts - 1:
                    time.sleep(2**attempt)
                else:
                    raise e

    return wrapped


def get_ch_2056_processing_extents(
    nx: int, ny: int
) -> Iterable[Tuple[float, float, float, float]]:
    """Generate a grid of bounding boxes that cover Switzerland's BBOX."""
    # Bounding box of Switzerland in EPSG 2056
    bbox_ch = (2485071, 1074261, 2837120, 1299942)  # (xmin, ymin, xmax, ymax)

    # Calculate the width and height of each sub-bounding box
    width = (bbox_ch[2] - bbox_ch[0]) / nx
    height = (bbox_ch[3] - bbox_ch[1]) / ny

    # Generate equally spaced bounding boxes
    for i in range(nx):
        for j in range(ny):
            xmin = bbox_ch[0] + i * width
            ymin = bbox_ch[1] + j * height
            xmax = xmin + width
            ymax = ymin + height
            yield (xmin, ymin, xmax, ymax)
