import os
import sys
import json

from google.cloud import storage, bigquery

from utils import get_config
from sdf import SDF

if __name__ == "__main__":
    # Check if env variable is set
    if not os.environ["GOOGLE_APPLICATION_CREDENTIALS"]:
        print("Please set 'GOOGLE_APPLICATION_CREDENTIALS' environment variable")
        os._exit(-1)

    config = get_config(sys.argv[1])
    storage_client = storage.Client()
    bigquery_client = bigquery.Client()

    all_blobs = list(storage_client.list_blobs(
        bucket_or_name=config["bucket_name"],
        prefix=config["input_path"])
    )
    print(f"Found {len(all_blobs)} blobs. Processing...")

    for index, blob in enumerate(all_blobs):
        print(f"Processing {index + 1} of {len(all_blobs)}: {blob.name}")
        sdf = SDF(config, blob, storage_client, bigquery_client)
        sdf.run()
