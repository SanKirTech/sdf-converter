import io
import json
import logging
from os.path import join

import pandas
from botocore.exceptions import ClientError


from sdf.utils import get_output_path, get_time, process, custom_json_dump


class AWS_SDF:
    def __init__(self, config, blob, s3_resource, s3_client):
        self.config = config
        self.input_path = config["input_path"]
        self.output_path = config["output_path"]
        self.bucket_name = config["bucket_name"]
        self.blob = blob

        self.src = "aws"

        self.s3_resource = s3_resource
        self.s3_client = s3_client
        self.bucket = self.s3_resource.Bucket(self.bucket_name)
        self.processed_data = None

    def update_storage(self):
        """Stores the error into sink"""

        file_contents = self.blob.get()['Body'].read()

        metadata = {
            "_rt": self.received_timestamp,
            "_src": self.src,
            "_o": "",
            "src_dtls": self.src_details,
        }
        self.processed_data = process(file_contents, metadata)
        self.bucket.put_object(Body=custom_json_dump(self.processed_data), Key=get_output_path(self.blob._key, self.output_path))
        return True

    def update_table(self):
        """Update table with data"""
        data = [
            {
                "src": self.src,
                "src_dtls": self.src_details,
                "record_count": len(self.processed_data),
                "received_timestamp": self.received_timestamp,
                "processed_timestamp": get_time(),
            }
        ]
        data_df = pandas.DataFrame(data)
        recon = None
        existing_csv_data = None
        key = self.config.get("reconciliation")
        try:
            recon = self.s3_resource.Object(self.bucket_name, key)
            existing_csv_data = recon.get()['Body']
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                pass
            else:
                raise

        if existing_csv_data is None:
            recon.put(Body=data_df.to_csv(index=False).encode())
        else:
            recon.put(Body=pandas.read_csv(existing_csv_data).append(data_df).to_csv(index=False).encode())

    def run(self):
        """Entrypoint"""
        res = self.update_storage()
        if res:
            self.update_table()

    @property
    def received_timestamp(self):
        """Convert timestamp to string"""
        return self.blob.last_modified.strftime("%Y-%m-%d %X")

    @property
    def src_details(self):
        return self.blob._bucket_name + '/' + self.blob._key
