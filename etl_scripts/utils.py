# -*- coding: utf-8 -*-
import requests
from typing import Any, List
import time

from utils_io import VersionedFile, retry


# --------------------------------------------------
# HTTP
# --------------------------------------------------


@retry
def fetch(url):
    """
    Send GET request to a specified url and retrieve html as string.
    Raises exception for non-200 status codes.
    """
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r


# --------------------------------------------------
# COMPUTE DEPENDENCY CHECKS
# --------------------------------------------------


def update_dependency_version_log_metafile_with_latest(
    file: VersionedFile, dependency_files: List[VersionedFile]
) -> None:
    """
    Update the dependency_version_log metadata of the latest version of file_to_update with the latest versions of the dependency files.
    """
    latest_version = file.get_latest_version()
    assert latest_version, f"No version of found for {file.file_path}."
    metadata = file.read_metadata(latest_version)
    dep_version_log = {}
    for dep in dependency_files:
        latest_dep_version = dep.get_latest_version()
        assert (
            latest_dep_version
        ), f"No version of found for dependency {dep.file_path}."
        dep_version_log[dep.file_path] = latest_dep_version
    metadata.update({"dependency_version_log": dep_version_log})
    file.write_metadata(metadata, latest_version)


def is_calculation_required(
    sink_file: VersionedFile, source_files: List[VersionedFile]
) -> bool:
    """
    Check if the sink_file needs to be recalculated based on the latest
    versions of the dependency files in dependency_version_log metadata.
    """
    # If there is no version of output file, calculation is required.
    latest_sink_version = sink_file.get_latest_version()
    if not latest_sink_version:
        return True
    dep_version_log = sink_file.read_metadata(latest_sink_version).get(
        "dependency_version_log", {}
    )
    for f in source_files:
        # Assert that at least one version of the input file exists.
        latest_version_str = f.get_latest_version()
        assert latest_version_str
        # If there is a new dependency, calculation is required.
        logged_version_str = dep_version_log.get(f.file_path)
        if not logged_version_str:
            return True
        # Compare versions. The versioning_scheme knows how to turn version strings into comparable
        # objects. If dependency version is older, calculation is required.
        latest_version = f.versioning_scheme.sort_key(f.get_latest_version())
        logged_version = f.versioning_scheme.sort_key(logged_version_str)
        if latest_version > logged_version:
            return True
    return False
