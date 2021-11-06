import io
import json
import logging

import pandas

from sdf.utils import get_output_path, get_time


class AWS_SDF:
    def __init__(self, config, blob, s3_resource):
        self.config = config
        self.input_path = config["input_path"]
        self.output_path = config["output_path"]
        self.bucket_name = config["bucket_name"]
        self.blob = blob

        self.src = "aws"

        self.s3_resource = s3_resource
        self.processed_data = None

    def update_storage(self):
        """Stores the error into sink"""
        bucket = self.s3_resource.Bucket(self.bucket_name)
        if self.blob is None:
            logging.error(f"input_path: {self.input_path} does not exist")
            return

        file_contents = self.blob.download_as_string()

        metadata = {
            "_rt": self.received_timestamp,
            "_src": self.src,
            "_o": "",
            "src_dtls": self.blob.public_url,
        }

        self.processed_data = GCP_SDF.process(file_contents, metadata)

        # Create destination blob
        dest_blob = bucket.blob(get_output_path(self.blob.name, self.output_path))
        dest_blob.upload_from_string(
            GCP_SDF.custom_json_dump(self.processed_data), content_type="application/json"
        )
        return True

    def update_table(self):
        """Update table with data"""
        data = [
            {
                "src": self.src,
                "src_dtls": self.blob.public_url,
                "record_count": len(self.processed_data),
                "received_timestamp": self.received_timestamp,
                "processed_timestamp": get_time(),
            }
        ]
        print("Inserting data into recon table")
        self.bigquery_client.insert_rows_json(self.table_name, data)

    @staticmethod
    def process(data, metadata):
        """Adds metadata tags"""
        parsed_data = pandas.read_csv(io.StringIO(data.decode("utf-8")))

        updated_data = list()
        for _, row in parsed_data.iterrows():
            updated_data.append({"_m": metadata, "_p": {"data": dict(row)}})
        return updated_data

    @staticmethod
    def custom_json_dump(processed_data):
        """
        Takes in processed data and returns a custom JSON format with
        each line representing valid JSON.
        """
        return "\n".join(list(map(json.dumps,  processed_data)))

    def run(self):
        """Entrypoint"""
        res = self.update_storage()
        if res:
            self.update_table()

    @property
    def received_timestamp(self):
        """Convert timestamp to string"""
        return self.blob.time_created.strftime("%Y-%m-%d %X")