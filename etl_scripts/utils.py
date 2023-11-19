# -*- coding: utf-8 -*-
import time
import requests

from typing import Optional, List, Callable, Any, Tuple
import os
import time
import functools
from abc import ABC, abstractmethod
from google.cloud import storage
import re


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
