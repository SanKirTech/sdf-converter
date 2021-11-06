import os
import sys
import json

from google.cloud import storage, bigquery
import boto3
from sdf.aws_sdf import AWS_SDF

from sdf.utils import get_config
from sdf.gcp_sdf import GCP_SDF

class Cloud:
    AWS = "AWS"
    GCP = "GCP"
    AZURE = "AZURE"

def main():
    # Check if env variable is set
    config = get_config(sys.argv[1])
    cloud = config.get("cloud")

    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") and cloud == Cloud.GCP:
        print("Please set 'GOOGLE_APPLICATION_CREDENTIALS' environment variable")
        os._exit(-1)

    if len(sys.argv) < 2:
        print("Provide path to config.json as argument.")
        os._exit(-1)

    if cloud == Cloud.AWS:
        aws_main(config)
    elif cloud == Cloud.GCP:
        gcp_main(config)
    elif cloud == Cloud.AZURE:
        raise NotImplementedError("Planned for future")


def aws_main(config):
    client = boto3.client('s3')
    s3 = boto3.resource('s3')

    bucket_name = config["bucket_name"]
    objects = client.list_objects_v2(
        Bucket=bucket_name,
        Delimiter="/",
        Prefix=config["input_path"]
    )

    contents = objects.get("Contents")
    if contents is None:
        print("Provided bucket_name and input_path contain no objects", file=sys.stderr)
        return

    print(f"Found {len(contents) - 1} blobs. Processing...")

    for index, blob in enumerate(contents):
        if blob.get("Size") == 0:
            continue
        print(f"Processing {index + 1} of {len(contents)}: {blob.name}")
        s3_object = s3.Object(bucket_name, blob.get("Key"))
        sdf = AWS_SDF(config, s3_object, s3)
        sdf.run()


def gcp_main(config):
    storage_client = storage.Client()
    bigquery_client = bigquery.Client()

    all_blobs = list(storage_client.list_blobs(
        bucket_or_name=config["bucket_name"],
        prefix=config["input_path"])
    )
    print(f"Found {len(all_blobs)} blobs. Processing...")

    for index, blob in enumerate(all_blobs):
        print(f"Processing {index + 1} of {len(all_blobs)}: {blob.name}")
        sdf = GCP_SDF(config, blob, storage_client, bigquery_client)
        sdf.run()


if __name__ == "__main__":
    main()
