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
from io import BytesIO
from azure.storage.blob import BlobServiceClient
import datetime
from utils import get_api_key, fetch_data, upload_data_to_azure
import pandas as pd


logger =logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if not logger.handlers:  # Avoid duplicate handlers
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    logger.addHandler(handler)

def ingest_bronze_data(req: func.HttpRequest) -> func.HttpResponse:
    #entry logging
    request_time = datetime.datetime.now()
    logging.info(f"ingest_bronze_data triggered at {request_time.isoformat()}. Processing request.")
    
    # Determine the environment and pick the appropriate Key Vault URL
    app_env = os.getenv("APP_ENV", "test")
    try:
        secret = get_api_key(app_env, )
    except Exception as error: 
        logging.error(f"something went wrong with getting the api key:, {type(error).__name__}, –, {error}")
        return func.HttpResponse(f"Failed to retrieve api key: {str(error)}", 
        status_code=500
         )


    
    # Fetch movie data
    try:
        response_all = fetch_data(secret)
    except Exception as error:
        logging.error(f"error with fetching all the data: {type(error).__name__}, –, {error}")
        return func.HttpResponse(f"Failed to fetch data key: {str(error)}", 
        status_code=500
         )

      

    
    
    # # convert JSON into pyarrow table
    # try:
    #     table = paj.read_json(io.BytesIO(json.dumps(response_all).encode()))
    # except Exception as error:
    #     logging.debug(f"Problematic data sample: {json.dumps(response_all[0], indent=2)}")
    #     logging.error(f"error with conversion into pa table:{type(error).__name__} –, {error}")
    #     return func.HttpResponse(f"Failed to transform into pa table: {str(error)}", 
    #     status_code=500
    #      )
    try:
        logger.info(f"Processing {len(response_all)} movie records using pandas")
        
        # Convert JSON to pandas DataFrame
        df = pd.DataFrame(response_all)
        
        # Add metadata columns
        current_time = datetime.datetime.now().isoformat()
        df['ingestion_time'] = current_time
        df['data_source'] = "api.themoviedb.org/3/discover/movie?include_adult=true&include_video=false&language=en-US&page={i}&primary_release_year=2023&sort_by=popularity.desc"
        
        # Convert pandas DataFrame to PyArrow table
        table = pa.Table.from_pandas(df)
        
        # Convert to parquet
        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer)
        parquet_buffer.seek(0)
        
    except Exception as error:
        logging.error(f"Error with pandas conversion: {type(error).__name__} – {error}")
        return func.HttpResponse(f"Failed to transform with pandas: {str(error)}", status_code=500)
    


    try:
        upload_data_to_azure(app_env,parquet_buffer)
    except Exception as error:
        logging.error(f"error with uploading to azure: {type(error).__name__}, –, {error}")
        return func.HttpResponse(f"Failed to upload: {str(error)}", 
        status_code=500
         )

        

    #exit logging
    execution_time = (datetime.datetime.now() - request_time).total_seconds()
    logging.info(f"ingest_bronze_data completed successfully in {execution_time:.2f} seconds")

    return func.HttpResponse(f"Data ingested successfully in {execution_time:.2f} seconds", status_code=200)