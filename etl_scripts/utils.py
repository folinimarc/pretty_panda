# -*- coding: utf-8 -*-
import time
import requests

from typing import Optional, List, Callable, Any
import os
import time
import functools
from abc import ABC, abstractmethod
from google.cloud import storage


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
# IO managers
# --------------------------------------------------


class IoManager(ABC):
    @abstractmethod
    def read(self, path: str, mode: str = "r") -> Optional[str]:
        pass

    @abstractmethod
    def write(self, path: str, data: str, mode: str = "w") -> None:
        pass

    @abstractmethod
    def list(self, path: str) -> List[str]:
        pass

    @abstractmethod
    def delete(self, path: str) -> None:
        pass

    @abstractmethod
    def gdal_path(self, path: str) -> str:
        pass

    @abstractmethod
    def absolute_path(self, path: str) -> str:
        pass


class LocalIoManager(IoManager):
    def __init__(self, root: str):
        self.root = root.rstrip("/")

    def read(self, path: str, mode: str = "r") -> Optional[str]:
        prefixed_path = os.path.join(self.root, path)
        if not os.path.exists(prefixed_path):
            return None
        with open(prefixed_path, mode) as file:
            return file.read()

    def write(self, path: str, data: str, mode: str = "w") -> None:
        prefixed_path = os.path.join(self.root, path)
        os.makedirs(os.path.dirname(prefixed_path), exist_ok=True)
        with open(prefixed_path, mode) as file:
            file.write(data)

    def list(self, path: str = "") -> List[str]:
        prefixed_path = os.path.join(self.root, path)
        if not os.path.exists(prefixed_path):
            return []
        return os.listdir(prefixed_path)

    def delete(self, path: str) -> None:
        prefixed_path = os.path.join(self.root, path)
        if os.path.exists(prefixed_path):
            os.remove(prefixed_path)

    def gdal_path(self, path: str) -> str:
        return os.path.join(self.root, path)

    def absolute_path(self, path: str) -> str:
        return os.path.abspath(os.path.join(self.root, path))


class GoogleCloudStorageIoManager(IoManager):
    def __init__(self, bucket_name: str, prefix: str = ""):
        self.prefix = prefix.strip()
        if self.prefix:
            assert not self.prefix.startswith("/"), "Prefix must not start with '/'"
            assert not self.prefix.startswith("."), "Prefix must not be relative"
            assert self.prefix.endswith("/"), "Prefix must end with '/'"
        self.bucket_name = bucket_name
        assert bucket_name.strip("/").strip("."), "Bucket name must be specified."
        assert (
            "GOOGLE_APPLICATION_CREDENTIALS" in os.environ
        ), "Environment variable 'GOOGLE_APPLICATION_CREDENTIALS' is not set."
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def _prefixed_path(self, path: str) -> str:
        return "/".join(
            x for x in [self.prefix.strip("/"), path.strip("/")] if x
        ).strip("/")

    @retry
    def read(self, path: str, mode: str = "r") -> Optional[str]:
        prefixed_path = self._prefixed_path(path)
        blob = self.bucket.blob(prefixed_path)
        if not blob.exists():
            return None
        with blob.open(mode) as f:
            return f.read()

    @retry
    def write(self, path: str, data: str, mode: str = "w") -> None:
        prefixed_path = self._prefixed_path(path)
        blob = self.bucket.blob(prefixed_path)
        with blob.open(mode) as f:
            f.write(data)

    @retry
    def list(self, path: str = "") -> List[str]:
        prefixed_path = self._prefixed_path(path)
        blobs = self.client.list_blobs(self.bucket, prefix=prefixed_path)
        return [blob.name[len(self.prefix) :] for blob in blobs]

    @retry
    def delete(self, path: str) -> None:
        prefixed_path = self._prefixed_path(path)
        blob = self.bucket.blob(prefixed_path)
        if blob.exists():
            blob.delete()

    def gdal_path(self, path: str) -> str:
        prefixed_path = self._prefixed_path(path)
        return f"/vsigs/{self.bucket_name}/{prefixed_path}"

    def absolute_path(self, path: str) -> str:
        prefixed_path = self._prefixed_path(path)
        return f"{self.bucket_name}/{prefixed_path}"
