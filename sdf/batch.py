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

    # TODO: list the CSVs and 
    sdf = SDF(config)
    sdf.run()