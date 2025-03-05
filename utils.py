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
    for j in range(2000,2003):
        for i in range(1, 51):
            url = f"https://api.themoviedb.org/3/discover/movie?include_adult=true&include_video=false&language=en-US&page={i}&primary_release_year={j}&sort_by=popularity.desc"
            headers = {
                "accept": "application/json",
                "Authorization": f"Bearer {secret.value}"
            }
        
            #response = requests.get(url, headers=headers)
            #response_all.append(response.json())  # Append JSON response
           
            for j in range(2020, 2002):  # Example: 2 years
                for i in range(1, 51):  # 50 pages per year
                    url = f"https://api.themoviedb.org/3/discover/movie?...page={i}&primary_release_year={j}..."
                    response = requests.get(url, headers=headers)
                    data = response.json()
                    if response.status_code != 200:
                        logging.error(f"API call failed for year {j}, page {i}: {response.text}")
                        continue
                    response_all.extend(data.get("results", []))  # Flatten 'results' into the list
                logging.debug.(f"fetched the year{j}")
                logging.debug.(f"last movie fetched: ", response_all[-1])
            
         

    
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
