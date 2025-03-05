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
from time import sleep

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

def get_api_key(app_env):
    logging.info(f"fetching api key in {app_env}-environment")
    # get key vault url depending on environment
    if app_env.lower() == "prod":
        key_vault_url = os.getenv("PROD_KEY_VAULT_URL")
    else:
        key_vault_url = os.getenv("TEST_KEY_VAULT_URL")

    if not key_vault_url:
        raise ValueError("Key Vault URL not configured")
    # Authenticate with Azure Key Vault
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url= key_vault_url, credential=credential)
    secret = client.get_secret("apikeytmdb")
    return secret
def fetch_data(secret):
    logging.info(f"fetching data from api")
    response_all = []
    for year in range(2020, 2002):  # Example: 2 years
        for page in range(1, 51):  # 50 pages per year
            url = f"https://api.themoviedb.org/3/discover/movie?...page={page}&primary_release_year={year}..."
            headers = {
                "accept": "application/json",
                "Authorization": f"Bearer {secret.value}"
            }
            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    response_all.extend(data.get("results", []))
                    logging.debug(f"Fetched page {page} for year {year}")
                else:
                    logging.error(f"API call failed for year {year}, page {page}: {response.text}")
            except Exception as e:
                logging.error(f"Error fetching data for year {year}, page {page}: {e}")
    
    logging.info(f"Fetched {len(response_all)} movies in total")
    return response_all
           
    
         

    
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
