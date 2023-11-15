# -*- coding: utf-8 -*-
from google.cloud import storage


def read_blob(bucket_name, blob_name):
    """
    Read a blob from GCS using file-like IO.
    Requires env variable GOOGLE_APPLICATION_CREDENTIALS to point to a service account
    credentials json with appropriate permissions.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if not blob.exists():
        return None
    with blob.open("r") as f:
        return f.read()


def delete_blobs(bucket_name, prefix):
    """
    Deletes blobs from the bucket with the given prefix.
    Requires env variable GOOGLE_APPLICATION_CREDENTIALS to point to a service account
    credentials json with appropriate permissions.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        blob.delete()


def delete_blob(bucket_name, blob_name):
    """
    Deletes a blob from the bucket.
    Requires env variable GOOGLE_APPLICATION_CREDENTIALS to point to a service account
    credentials json with appropriate permissions.
    From https://cloud.google.com/storage/docs/samples/storage-delete-file
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    if blob.exists():
        blob.delete()


def write_blob(data, mode, bucket_name, blob_name):
    """
    Write a blob from GCS using file-like IO.
    From https://cloud.google.com/appengine/docs/legacy/standard/python/googlecloudstorageclient/read-write-to-cloud-storage
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    with blob.open(mode) as f:
        f.write(data)
    # with blob.open("r") as f:
    #     print(f.read())
