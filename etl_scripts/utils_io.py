# -*- coding: utf-8 -*-
import os
import re
import shutil
from abc import ABC, abstractmethod
from typing import Callable, List, Optional, Union, Any
from google.cloud import storage
from datetime import datetime
import json
import functools
import time

# --------------------------------------------
# RETRY DECORATOR
# --------------------------------------------


def retry(operation: Callable) -> Callable:
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


# --------------------------------------------
# STORAGE BACKENDS
# --------------------------------------------


class StorageBackend(ABC):
    @abstractmethod
    def read_file(self, file_path: str, mode: str = "r") -> Optional[str]:
        """Read the contents of a file specified by the full file path."""
        pass

    @abstractmethod
    def write_file(
        self, file_path: str, data: Union[str, bytes], mode: str = "w"
    ) -> None:
        """Write data to a file specified by the full file path."""
        pass

    @abstractmethod
    def list_files(self, directory_path: str = "") -> List[str]:
        """List all files in a directory specified by the directory path."""
        pass

    @abstractmethod
    def file_exists(self, file_path: str) -> bool:
        """Check if a file exists at the specified file path."""
        pass

    @abstractmethod
    def delete_file(self, file_path: str) -> None:
        """Delete a file specified by the full file path."""
        pass

    @abstractmethod
    def create_directory_for_file(self, file_path: str) -> None:
        """Create an empty directory with potentially parent directories at file path."""
        pass

    @abstractmethod
    def create_directory(self, directory_path: str) -> None:
        """Create an empty directory with potentially parent directories."""
        pass

    @abstractmethod
    def delete_directory(self, directory_path: str = "") -> None:
        """Delete a directory and all its contents specified by the directory path."""
        pass

    @abstractmethod
    def gdal_path(self, file_path: str) -> str:
        """Get the GDAL-compatible path for a file or directory."""
        pass

    @abstractmethod
    def absolute_path(self, file_path: str) -> str:
        """Get the absolute path for a file or directory."""
        pass


# Local IO Manager
class LocalStorageBackend(StorageBackend):
    def __init__(self, root_directory: str):
        self.root_directory = root_directory.rstrip("/") + "/"

    def _full_path(self, path: str) -> str:
        """Get the full path by appending the given path to the root directory."""
        return os.path.join(self.root_directory, path.lstrip("/"))

    def read_file(self, file_path: str, mode: str = "r") -> Optional[str]:
        full_path = self._full_path(file_path)
        if not os.path.isfile(full_path):
            return None
        with open(full_path, mode) as file:
            return file.read()

    def write_file(
        self, file_path: str, data: Union[str, bytes], mode: str = "w"
    ) -> None:
        full_path = self._full_path(file_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, mode) as file:
            file.write(data)

    def list_files(self, directory_path: str = "") -> List[str]:
        full_path = self._full_path(directory_path).rstrip("/") + "/"
        if not os.path.isdir(full_path):
            return []
        return [
            f
            for f in os.listdir(full_path)
            if os.path.isfile(os.path.join(full_path, f))
        ]

    def file_exists(self, file_path: str) -> bool:
        full_path = self._full_path(file_path)
        return os.path.isfile(full_path)

    def delete_file(self, file_path: str) -> None:
        full_path = self._full_path(file_path)
        if os.path.isfile(full_path):
            os.remove(full_path)

    def create_directory_for_file(self, file_path: str) -> None:
        """Create a local directory and its parent directories if they do not exist."""
        directory_path = os.path.dirname(file_path) + "/"
        self.create_directory(directory_path)

    def create_directory(self, directory_path: str) -> None:
        """Create a local directory and its parent directories if they do not exist."""
        full_path = self._full_path(directory_path).rstrip("/") + "/"
        os.makedirs(full_path, exist_ok=True)

    def delete_directory(self, directory_path: str = "") -> None:
        full_path = self._full_path(directory_path).rstrip("/") + "/"
        if os.path.isdir(full_path):
            shutil.rmtree(full_path)

    def gdal_path(self, file_path: str) -> str:
        """Get the GDAL-compatible path for a file or directory."""
        self.create_directory_for_file(file_path)
        return os.path.abspath(self._full_path(file_path))

    def absolute_path(self, file_path: str) -> str:
        """Get the absolute path for a file or directory."""
        self.create_directory_for_file(file_path)
        return os.path.abspath(self._full_path(file_path))


# Google Cloud Storage IO Manager
class GoogleCloudStorageStorageBackend(StorageBackend):
    def __init__(self, bucket_name: str, root_directory: str):
        assert (
            "GOOGLE_APPLICATION_CREDENTIALS" in os.environ
        ), "Environment variable 'GOOGLE_APPLICATION_CREDENTIALS' is not set."
        assert bucket_name.strip("/").strip("."), "Bucket name must be specified."
        self.root_directory = root_directory.strip("/")
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def _full_path(self, path: str) -> str:
        """Get the full path by appending the given path to the root directory."""
        return os.path.join(self.root_directory, path.lstrip("/"))

    @retry
    def read_file(self, file_path: str, mode: str = "r") -> Optional[str]:
        full_path = self._full_path(file_path)
        blob = self.bucket.blob(full_path)
        if not blob.exists():
            return None
        with blob.open(mode=mode) as f:
            return f.read()

    @retry
    def write_file(
        self, file_path: str, data: Union[str, bytes], mode: str = "w"
    ) -> None:
        full_path = self._full_path(file_path)
        blob = self.bucket.blob(full_path)
        with blob.open(mode=mode) as f:
            f.write(data)

    @retry
    def list_files(self, directory_path: str = "") -> List[str]:
        full_path = self._full_path(directory_path).rstrip("/") + "/"
        blobs = self.client.list_blobs(self.bucket, prefix=full_path)
        blobname_root_prefix_cutoff = (
            len(self.root_directory) + 1 if self.root_directory else 0
        )
        blobnames_without_root = [
            blob.name[blobname_root_prefix_cutoff:]
            for blob in blobs
            if not blob.name.endswith("/")
        ]
        return blobnames_without_root

    @retry
    def file_exists(self, file_path: str) -> bool:
        full_path = self._full_path(file_path)
        blob = self.bucket.blob(full_path)
        return blob.exists()

    @retry
    def delete_file(self, file_path: str) -> None:
        full_path = self._full_path(file_path)
        blob = self.bucket.blob(full_path)
        if blob.exists():
            blob.delete()

    def create_directory_for_file(self, file_path: str) -> None:
        """Create a pseudo-directory in Google Cloud Storage."""
        directory_path = os.path.dirname(file_path) + "/"
        self.create_directory(directory_path)

    @retry
    def create_directory(self, directory_path: str) -> None:
        """
        Create a pseudo-directory in Google Cloud Storage.
        In GCS, directories are virtual and based on the file blob names.
        To create a directory, a blob must exist with the directory prefix.
        """
        full_path = self._full_path(directory_path).rstrip("/") + "/"
        blob = self.bucket.blob(full_path)
        blob.upload_from_string("")  # Creating an empty blob to represent the directory

    @retry
    def delete_directory(self, directory_path: str = "") -> None:
        full_path = self._full_path(directory_path).rstrip("/") + "/"
        blobs = self.client.list_blobs(self.bucket, prefix=full_path)
        for blob in blobs:
            blob.delete()

    def gdal_path(self, file_path: str) -> str:
        """Get the GDAL-compatible path for a file or directory."""
        self.create_directory_for_file(file_path)
        return "/vsigs/" + self.bucket.name + "/" + self._full_path(file_path)

    def absolute_path(self, file_path: str) -> str:
        """Get the absolute path for a file or directory."""
        # In the context of GCS, the absolute path is the full URI to the object.
        self.create_directory_for_file(file_path)
        return f"gs://{self.bucket.name}/{self._full_path(file_path)}"


# --------------------------------------------
# FILE VERSIONING SCHEMES
# --------------------------------------------


class VersioningScheme(ABC):
    @staticmethod
    @abstractmethod
    def construct_versioned_filename(file_path: str, version: str) -> str:
        """Construct a versioned filename from a file path and version."""
        pass

    @staticmethod
    @abstractmethod
    def extract_version_from_filename(file_path: str) -> Optional[str]:
        """Extract the version from a versioned filename."""
        pass

    @staticmethod
    @abstractmethod
    def assert_valid_version(version: str) -> None:
        """Check if a version is valid. Raise exception if not valid."""
        pass

    @staticmethod
    @abstractmethod
    def sort_key(version: str) -> str:
        """Given a version, return a value that is used for sorting. Used as argument key
        in built-in sorted() funtion. Larger is assumed to be more recent."""
        pass


# Additional utility class for handling versioning
class YYYYMMDDFilenamePrefix(VersioningScheme):
    @staticmethod
    def assert_valid_version(version: str) -> None:
        """Check if a version is valid. Raise exception if not valid."""
        assert (
            len(version) == 8
        ), f"Version must be in YYYYMMDD format. Found: {version}"
        assert (
            datetime.strftime(datetime.strptime(version, "%Y%m%d"), "%Y%m%d") == version
        ), f"Version must be in YYYYMMDD format. Found: {version}"

    @staticmethod
    def construct_versioned_filename(file_path: str, version: str) -> str:
        """Construct a versioned filename from a file path and version."""
        YYYYMMDDFilenamePrefix.assert_valid_version(version)
        directory, filename = os.path.split(file_path)
        versioned_filename = f"{version}__{filename}"
        return os.path.join(directory, versioned_filename)

    @staticmethod
    def extract_version_from_filename(file_path: str) -> Optional[str]:
        """Extract the version from a versioned filename."""
        version_pattern = re.compile(r"(\d{8})__")
        match = version_pattern.search(file_path)
        return match.group(1) if match else None

    @staticmethod
    def sort_key(version: str) -> datetime:
        """Given a version, return a value that is used for sorting. Used as argument key
        in built-in sorted() funtion. Larger is assumed to be more recent."""
        YYYYMMDDFilenamePrefix.assert_valid_version(version)
        return datetime.strptime(version, "%Y%m%d")


# --------------------------------------------
# FILE HANDLER
# --------------------------------------------


class File:
    def __init__(self, file_path: str, storage_backend: StorageBackend):
        self.file_path = file_path
        self.storage_backend = storage_backend

    def read(self, mode: str = "r") -> Optional[str]:
        return self.storage_backend.read_file(self.file_path, mode)

    def write(self, data: Union[str, bytes], mode: str = "w") -> None:
        self.storage_backend.create_directory_for_file(self.file_path)
        self.storage_backend.write_file(self.file_path, data, mode)

    def exists(self) -> bool:
        return self.storage_backend.file_exists(self.file_path)

    def delete(self) -> None:
        self.storage_backend.delete_file(self.file_path)
        self.delete_metadata()

    def gdal_path(self) -> str:
        return self.storage_backend.gdal_path(self.file_path)

    def absolute_path(self) -> str:
        return self.storage_backend.absolute_path(self.file_path)

    def _get_metadata_file_path(self) -> str:
        """Construct the metadata file path for the current file path."""
        return f"{self.file_path}__meta.json"

    def _read_metadata(self, metadata_file_path) -> dict:
        if self.storage_backend.file_exists(metadata_file_path):
            return json.loads(self.storage_backend.read_file(metadata_file_path))
        return {}

    def _write_metadata(self, metadata_file_path, metadata: dict) -> None:
        self.storage_backend.create_directory_for_file(metadata_file_path)
        metadata_content = json.dumps(metadata, indent=4)
        self.storage_backend.write_file(metadata_file_path, metadata_content)

    def _delete_metadata(self, metadata_file_path) -> None:
        if self.storage_backend.file_exists(metadata_file_path):
            self.storage_backend.delete_file(metadata_file_path)

    def read_metadata(self) -> dict:
        """Read the metadata from the metadata file."""
        return self._read_metadata(self._get_metadata_file_path())

    def write_metadata(self, metadata: dict) -> None:
        """Write the metadata to the metadata file."""
        self._write_metadata(self._get_metadata_file_path(), metadata)

    def delete_metadata(self) -> None:
        """Delete the metadata file."""
        self._delete_metadata(self._get_metadata_file_path())


class VersionedFile(File):
    def __init__(
        self,
        file_path: str,
        storage_backend: StorageBackend,
        versioning_scheme: VersioningScheme,
    ):
        super().__init__(file_path, storage_backend)
        self.versioning_scheme = versioning_scheme

    def read(self, version: str, mode: str = "r") -> Optional[str]:
        versioned_path = self.versioning_scheme.construct_versioned_filename(
            self.file_path, version
        )
        return self.storage_backend.read_file(versioned_path, mode)

    def write(self, data: Union[str, bytes], version: str, mode: str = "w") -> None:
        versioned_path = self.versioning_scheme.construct_versioned_filename(
            self.file_path, version
        )
        self.storage_backend.create_directory_for_file(versioned_path)
        self.storage_backend.write_file(versioned_path, data, mode)

    def exists(self, version: str) -> bool:
        versioned_path = self.versioning_scheme.construct_versioned_filename(
            self.file_path, version
        )
        return self.storage_backend.file_exists(versioned_path)

    def delete(self, version: str) -> None:
        versioned_path = self.versioning_scheme.construct_versioned_filename(
            self.file_path, version
        )
        self.storage_backend.delete_file(versioned_path)
        self.delete_metadata()

    def list_versions(self) -> List[str]:
        directory = os.path.dirname(self.file_path).rstrip("/") + "/"
        all_files = self.storage_backend.list_files(directory)
        versions = [
            self.versioning_scheme.extract_version_from_filename(f)
            for f in all_files
            if f.endswith(os.path.basename(self.file_path))
        ]
        filtered_versions = [version for version in versions if version]
        sorted_filtered_versions = sorted(
            filtered_versions, key=self.versioning_scheme.sort_key, reverse=True
        )
        return sorted_filtered_versions

    def get_latest_version(self) -> Optional[str]:
        versions = self.list_versions()
        if versions:
            return versions[0]
        return None

    def gdal_path(self, version: str) -> str:
        versioned_path = self.versioning_scheme.construct_versioned_filename(
            self.file_path, version
        )
        return self.storage_backend.gdal_path(versioned_path)

    def absolute_path(self, version: str) -> str:
        versioned_path = self.versioning_scheme.construct_versioned_filename(
            self.file_path, version
        )
        return self.storage_backend.absolute_path(versioned_path)

    def _get_metadata_file_path(self, version: str) -> str:
        """Construct the metadata file path for a specific version of the file."""
        versioned_path = self.versioning_scheme.construct_versioned_filename(
            self.file_path, version
        )
        return f"{versioned_path}__meta.json"

    def read_metadata(self, version: str) -> dict:
        """Read the metadata from the metadata file for a specific version."""
        return self._read_metadata(self._get_metadata_file_path(version))

    def write_metadata(self, metadata: dict, version: str) -> None:
        """Write the metadata to the metadata file for a specific version."""
        self._write_metadata(self._get_metadata_file_path(version), metadata)

    def delete_metadata(self, version: str) -> None:
        """Delete the metadata file for a specific version."""
        self._delete_metadata(self._get_metadata_file_path(version))
