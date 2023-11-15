# -*- coding: utf-8 -*-
from google.cloud import storage


def read_blob(location, mode="r"):
    """
    Read a blob from GCS using file-like IO.
    Requires env variable GOOGLE_APPLICATION_CREDENTIALS to point to a service account
    credentials json with appropriate permissions.
    """
    bucket_name, blob_name = location.split("/", 1)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if not blob.exists():
        return None
    with blob.open(mode) as f:
        return f.read()


def list_blobs(location):
    """
    Lists blobs in the bucket with the given prefix.
    Requires env variable GOOGLE_APPLICATION_CREDENTIALS to point to a service account
    credentials json with appropriate permissions.
    """
    bucket_name, prefix = location.split("/", 1)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return [f"{bucket_name}/{blob.name}" for blob in blobs]


def delete_blob(location):
    """
    Deletes a blob from the bucket.
    Requires env variable GOOGLE_APPLICATION_CREDENTIALS to point to a service account
    credentials json with appropriate permissions.
    From https://cloud.google.com/storage/docs/samples/storage-delete-file
    """
    bucket_name, blob_name = location.split("/", 1)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if blob.exists():
        blob.delete()


def write_blob(location, data, mode="w"):
    """
    Write a blob from GCS using file-like IO.
    From https://cloud.google.com/appengine/docs/legacy/standard/python/googlecloudstorageclient/read-write-to-cloud-storage
    """
    bucket_name, blob_name = location.split("/", 1)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    with blob.open(mode) as f:
        f.write(data)
