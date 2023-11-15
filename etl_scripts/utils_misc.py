# -*- coding: utf-8 -*-
from functools import wraps
import time
import requests


def with_retry(max_retries=5, retry_wait=3):
    """Decorator that retries a function call a specified number of times"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            n_retries = 0
            while True:
                try:
                    r = func(*args, **kwargs)
                    break
                except Exception as e:
                    if n_retries < max_retries:
                        time.sleep(retry_wait)
                        n_retries += 1
                    else:
                        raise Exception(
                            f"Fetching failed after {max_retries} retries for {func.__name__} with args {args} and kwargs {kwargs}! Original exception: {e}"
                        )
            return r

        return wrapper

    return decorator


@with_retry(max_retries=5, retry_wait=3)
def fetch(url):
    """
    Send GET request to a specified url and retrieve html as string.
    Raises exception for non-200 status codes.
    """
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r
