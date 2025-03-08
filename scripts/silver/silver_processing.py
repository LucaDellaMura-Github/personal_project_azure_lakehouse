import pandas as pd
import logging
import datetime
from azure.storage.blob import BlobServiceClient
from utils import establish_connection_to_azure
from utils import initialisation
import os 




def transform_data_silver_layer(req: func.HttpRequest) -> func.HttpResponse:
    
    app_env, request_time = initialisation("transform_data_silver_layer")
    blob_service_client, config = establish_connection_to_azure(app_env)
    blob_client = blob_service_client.get_blob_client(container=config["container_name"], blob=config["blob_name"])

    # Download blob to Pandas DataFrame
    with open("temp.parquet", "wb") as f:
        data = blob_client.download_blob().readinto(f)
    df = pd.read_parquet("temp.parquet")