#!/usr/bin/env python3

from google.cloud import storage

def download_openaip():
    bucket_name = "29f98e10-a489-4c82-ae5e-489dbcd4912f"
    ressources_path = "/ressources/"

    storage_client = storage.Client.create_anonymous_client()
    bucket = storage_client.bucket(bucket_name)
    blobs = [b for b in bucket.list_blobs() if b.path.endswith('apt.cup')]
    for blob in blobs:
        blob.download_to_filename(ressources_path + blob.name)

if __name__ == '__main__':
    download_openaip()

