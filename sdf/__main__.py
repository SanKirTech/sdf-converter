import os
import sys
import json

from google.cloud import storage, bigquery
from azure.storage.blob import BlobServiceClient
import boto3
from sdf.aws_sdf import AWS_SDF

from sdf.utils import get_config, Cloud, AZURE_STORAGE_CONNECTION_STRING
from sdf.gcp_sdf import GCP_SDF
from sdf.azure_sdf import AZURE_SDF


def main():
    # Check if env variable is set
    if len(sys.argv) < 2:
        print("Provide path to config.json as argument.")
        os._exit(-1)

    config = get_config(sys.argv[1])
    cloud = config.get("cloud")

    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") and cloud == Cloud.GCP:
        print("Please set 'GOOGLE_APPLICATION_CREDENTIALS' environment variable")
        os._exit(-1)
    elif cloud == Cloud.AZURE and not os.environ.get(AZURE_STORAGE_CONNECTION_STRING):
        print("Please set '{}' environment variable".format(AZURE_STORAGE_CONNECTION_STRING))
        os._exit(-1)

    if cloud == Cloud.AWS:
        aws_main(config)
    elif cloud == Cloud.GCP:
        gcp_main(config)
    elif cloud == Cloud.AZURE:
        azure_main(config)     
    else:
        raise Exception("Unknown cloud type: {}".format(cloud))


def aws_main(config):
    client = boto3.client('s3')
    s3_resource = boto3.resource('s3')

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

    contents = list(filter(lambda x: x.get("Size") != 0, contents))
    print(f"Found {len(contents)} blobs. Processing...")

    for index, blob in enumerate(contents):
        key = blob.get("Key")
        print(f"Processing {index + 1} of {len(contents)}: {key}")
        s3_object = s3_resource.Object(bucket_name, key)
        sdf = AWS_SDF(config, s3_object, s3_resource, client)
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


def azure_main(config):
    credential = os.environ[AZURE_STORAGE_CONNECTION_STRING]
    service = BlobServiceClient.from_connection_string(credential)
    container_client = service.get_container_client(config.get("container"))
    blobs = list(container_client.list_blobs(name_starts_with=config.get("input_path")))
    print(f"Found {len(blobs)} blobs. Processing...")
    for index, blob in enumerate(blobs):
        print(f"Processing {index + 1} of {len(blobs)}: {blob.name}")
        sdf = AZURE_SDF(config, blob, container_client)
        sdf.run()


if __name__ == "__main__":
    main()
