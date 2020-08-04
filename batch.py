import os
import sys

from google.cloud import storage

from utils import (
    get_config,
    get_filename_without_extension,
    get_output_path,
    get_time,
    process,
)

if __name__ == "__main__":
    # Check if env variable is set
    if not os.environ["GOOGLE_APPLICATION_CREDENTIALS"]:
        print("Please set 'GOOGLE_APPLICATION_CREDENTIALS' environment variable")
        os._exit(-1)

    # Check if config provided
    try:
        config_path = sys.argv[1]
    except IndexError:
        print("Please provide path to config.json as argument.\nExiting...")
        os._exit(-1)

    config = get_config(config_path)
    if not config:
        os._exit(-1)
    input_path = config["input_path"]
    output_path = config["output_path"]
    bucket_name = config["bucket_name"]

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob(input_path)
    file_contents = blob.download_as_string()
    # src_dtls = "gs://"+bucket_name+"/"+input_path

    metadata = {"_rt": get_time(), "_src": "gcs", "_o": "", "src_dtls": blob.public_url}

    filename = get_filename_without_extension(blob.name)
    processed_data = process(file_contents, metadata)

    # Create destination blob
    blob = bucket.blob(get_output_path(input_path, output_path))
    blob.upload_from_string(processed_data, content_type="application/json")

    print("Success!")
