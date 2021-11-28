
import io
from os.path import join

import pandas

from azure.storage.blob import BlobClient, BlobProperties, ContainerClient

from sdf.utils import get_output_path, get_time, process, custom_json_dump


class AZURE_SDF:
    def __init__(self, config, blob: BlobProperties, container_client: ContainerClient):
        self.config = config
        self.input_path = config["input_path"]
        self.output_path = config["output_path"]
        self.bucket_name = config["bucket_name"]
        self.blob: BlobProperties = blob

        self.src = "azure"

        self.container_client = container_client

    def update_storage(self):
        """Stores the error into sink"""

        file_contents = self.container_client.download_blob(self.blob).readall()

        metadata = {
            "_rt": self.received_timestamp,
            "_src": self.src,
            "_o": "",
            "src_dtls": self.src_details,
        }
        self.processed_data = process(file_contents, metadata)
        self.container_client.upload_blob(
            name=get_output_path(self.blob.name, self.output_path),
            data=io.BytesIO(custom_json_dump(self.processed_data).encode()),
            overwrite=True
        )
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
        reconciliation_filename = self.config.get("reconciliation")
        recon: BlobClient = self.container_client.get_blob_client(reconciliation_filename)

        if not recon.exists():
            updated_data = io.BytesIO(data_df.to_csv(index=False).encode())
        else:
            existing_csv_data = io.BytesIO(self.container_client.download_blob(reconciliation_filename).readall())
            updated_data = io.BytesIO(pandas.read_csv(existing_csv_data).append(data_df).to_csv(index=False).encode())

        self.container_client.upload_blob(
            name=reconciliation_filename,
            data=updated_data,
            overwrite=True
        )

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
        return self.blob.container + '/' + self.blob.name
