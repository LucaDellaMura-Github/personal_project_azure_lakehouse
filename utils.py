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
def initialisation(function_name):
    logger =logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:  # Avoid duplicate handlers
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        logger.addHandler(handler)
    #entry logging
    request_time = datetime.datetime.now()
    logging.info(f"{function_name} at {request_time.isoformat()}. Processing request.")
    
    # Determine the environment and pick the appropriate Key Vault URL
    app_env = os.getenv("APP_ENV", "test")
    return app_env, request_time

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
    for year in range(2000, 2024):  
        for page in range(1, 51):  # 50 pages per year
            url = f"https://api.themoviedb.org/3/discover/movie?...page={page}&primary_release_year={year}..."
            headers = {
                "accept": "application/json",
                "Authorization": f"Bearer {secret.value}"
            }
            logging.debug(f"Requesting URL: {url}")
            try:
                response = requests.get(url, headers=headers)
                logging.debug(f"API response status: {response.status_code}")
                logging.debug(f"API response text: {response.text[:500]}")  # Limit to 500 chars for brevity
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
           
def define_config(app_env):
    logging.info(f"defining config dict with the connection variables")
    if app_env.lower() == "prod":
        config = {
        "connection_string" : os.getenv("PROD_Connection_string"),
        "container_name" : os.getenv("PROD_container_name"),
        "container_name_staging" : os.getenv("PROD_container_name_staging"),
        "blob_name" : os.getenv("PROD_blob_name"),
        "blob_name_staging" : os.getenv("PROD_blob_name_staging")}
    else:
        config = {
        "connection_string" : os.getenv("TEST_Connection_string"),
        "container_name" : os.getenv("TEST_container_name"),
        "container_name_staging" :  os.getenv("TEST_container_name_staging"),
        "blob_name" : os.getenv("TEST_blob_name"),
        "blob_name_staging" : os.getenv("TEST_blob_name_staging")}
    return config
         
def establish_connection_to_azure(app_env):
    # get connection string, container name and blob name to storage depending on environment
    config = define_config(app_env)
    logging.info(f"establishing connection to azure")
     # connect to azure storage
    blob_service_client = BlobServiceClient.from_connection_string(config["connection_string"])
    return blob_service_client, config
    
  

def upload_data_to_azure(app_env,parquet_buffer, response_all):
    logging.info(f"upload data in {app_env}-environment")
    
    blob_service_client, config = establish_connection_to_azure(app_env)
   

     # Upload JSON to staging container
    try:
        # Convert dictionary to JSON string
        json_data = json.dumps(response_all)
        
        # Create blob client for staging container
        staging_blob_client = blob_service_client.get_blob_client(
            container=config["container_name_staging"], 
            blob=config["blob_name_staging"]
        )
        
        # Upload JSON data
        staging_blob_client.upload_blob(json_data, overwrite=True)
        logging.info(f"Successfully uploaded JSON data to {config['container_name_staging']}/{config['blob_name_staging']}")
    except Exception as e:
        logging.error(f"Failed to upload JSON data to staging: {str(e)}")
        raise
    
    # Upload parquet to bronze container
    try:
        # Create blob client for bronze container
        bronze_blob_client = blob_service_client.get_blob_client(
            container=config["container_name"], 
            blob=config["blob_name"]
        )
        
        # Upload parquet data
        bronze_blob_client.upload_blob(parquet_buffer, overwrite=True)
        logging.info(f"Successfully uploaded parquet data to {config["container_name"]}/{config["blob_name"]}")
    except Exception as e:
        logging.error(f"Failed to upload parquet data to bronze: {str(e)}")
        raise

  
