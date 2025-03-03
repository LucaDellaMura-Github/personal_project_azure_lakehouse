import logging
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import requests
import json
import os
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.json as paj
import io
from azure.storage.blob import BlobServiceClient
import datetime
from utils import get_api_key, fetch_data, upload_data_to_azure


def ingest_bronze_data(req: func.HttpRequest) -> func.HttpResponse:
    #entry logging
    request_time = datetime.datetime.now()
    logging.info(f"ingest_bronze_data triggered at {request_time.isoformat()}. Processing request.")
    
    # Determine the environment and pick the appropriate Key Vault URL
    app_env = os.getenv("APP_ENV", "test")
    
    secret = get_api_key(app_env, )
    
    # Fetch movie data
    response_all = fetch_data(secret)
    
    
    # convert JSON into pyarrow table
    table = paj.read_json(io.BytesIO(json.dumps(response_all).encode()))

    # add data auditability columns
  
    ingestion_time = pa.array([datetime.datetime.now().isoformat()] * len(table))  # Current time for all rows as pyarrow requires
    data_source = pa.array(["api.themoviedb.org/3/discover/movie?include_adult=true&include_video=false&language=en-US&page={i}&primary_release_year=2023&sort_by=popularity.desc"] * len(table))  # Static source name for all rows

    # Append metadata columns to the table
    table = table.append_column("ingestion_time", pa.array(ingestion_time))
    table = table.append_column("data_source", pa.array(data_source))

    # convert to parquet
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer)

   
   
    # Reset buffer position to the beginning
    parquet_buffer.seek(0)

    upload_data_to_azure(app_env,parquet_buffer)

    #exit logging
    execution_time = (datetime.datetime.now() - request_time).total_seconds()
    logging.info(f"ingest_bronze_data completed successfully in {execution_time:.2f} seconds")
  

   
        
   
