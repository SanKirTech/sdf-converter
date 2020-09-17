import io
import csv
import json
from google.cloud import storage, bigquery

from utils import get_fields, get_time, get_output_path


class SDF:
    def __init__(self, config):
        self.config = config
        self.input_path = config["input_path"]
        self.output_path = config["output_path"]
        self.bucket_name = config["bucket_name"]
        self.table_name = config["bigquery_table_name"]
        self.src = "gcs"
        self.storage_client = storage.Client()
        self.bigquery_client = bigquery.Client()

    def update_storage(self):
        bucket = self.storage_client.get_bucket(self.bucket_name)
        self.blob = bucket.get_blob(self.input_path)

        # Check if blob has already been processed in the bigquery db
        if list(
            self.bigquery_client.query(
                f"""SELECT src_dtls from `pro-spark.recon.reconciliation` where src_dtls = "{self.blob.public_url}" """
            )
        ):
            print("File already processed. Skipping...")
            return False

        file_contents = self.blob.download_as_string()

        metadata = {
            "_rt": self.received_timestamp,
            "_src": self.src,
            "_o": "",
            "src_dtls": self.blob.public_url,
        }

        self.processed_data = SDF.process(file_contents, metadata)

        # Create destination blob
        dest_blob = bucket.blob(get_output_path(self.input_path, self.output_path))
        dest_blob.upload_from_string(
            SDF.custom_json_dump(self.processed_data), content_type="application/json"
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
        f = io.StringIO(data.decode("utf-8"))
        fields = get_fields(data)
        parsed_data = list(csv.DictReader(f, fields))[1:]

        updated_data = list()
        for row in parsed_data:
            updated_data.append({"_m": metadata, "_p": {"data": row}})

        return updated_data

    @staticmethod
    def custom_json_dump(processed_data):
        """
        Takes in processed data and returns a custom JSON format with
        each line representing valid JSON.
        """
        return "\n".join(list(map(json.dumps,  processed_data)))

    def run(self):
        res = self.update_storage()
        if res:
            self.update_table()

    @property
    def received_timestamp(self):
        return self.blob.time_created.strftime("%Y-%m-%d %X")
