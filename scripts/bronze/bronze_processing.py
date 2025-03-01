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

def get_api_key(app_env):
    logging.info(f"fetching api key in {app_env}-environment")
    # get key vault url depending on environment
    if app_env.lower() == "prod":
        key_vault_url = os.getenv("PROD_KEY_VAULT_URL")
    else:
        key_vault_url = os.getenv("TEST_KEY_VAULT_URL")

    if not key_vault_url:
        return func.HttpResponse("Key Vault URL not configured.", status_code=500)
    # Authenticate with Azure Key Vault
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url= key_vault_url, credential=credential)
    secret = client.get_secret("apikeytmdb")
    return secret
def fetch_data(secret):
    logging.info(f"fetching data from api")
    response_all = []
    for i in range(1, 51):
        url = f"https://api.themoviedb.org/3/discover/movie?include_adult=true&include_video=false&language=en-US&page={i}&primary_release_year=2023&sort_by=popularity.desc"
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {secret.value}"
        }
       
        response = requests.get(url, headers=headers)
        response_all.append(response.json())  # Append JSON response

    
    # Log the total responses fetched
    logging.info(f"Fetched {len(response_all)} pages of movie data.")
    return response_all

def upload_data_to_azure(app_env,parquet_buffer):
    logging.info(f"upload data in {app_env}-environment")
    # get connection string, container name and blob name to storage depending on environment
    if app_env.lower() == "prod":
        connection_string = os.getenv("PROD_Connection_string")
        container_name = os.getenv("PROD_container_name")
        blob_name = os.getenv("PROD_blob_name")
    else:
        connection_string = os.getenv("TEST_Connection_string")
        container_name = os.getenv("TEST_container_name")
        blob_name = os.getenv("TEST_blob_name")
     # connect to azure storage
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # upload to azure storage
    blob_client.upload_blob(parquet_buffer, overwrite=True)

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
  
    ingestion_time = pa.array([datetime.now().isoformat()] * len(table))  # Current time for all rows as pyarrow requires
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
  

   
        
   
