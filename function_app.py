import azure.functions as func
from utils import define_config
from scripts.bronze.bronze_processing import ingest_bronze_data
from scripts.silver.silver_processing import transform_data_silver_layer

import logging
import os

app = func.FunctionApp()
config = define_config("test")

name_bronce = "BronzeFunction"
@app.function_name(name=name_bronce)
@app.route(route="bronze")

def bronce_layer_processing(req: func.HttpRequest) -> func.HttpResponse:
    try:
        ingest_bronze_data(req)  # function doing all the ingestion work
        return func.HttpResponse("Bronze data ingestion successful!", status_code=200)
    except Exception as e:
        logging.exception("Error in Bronze ingestion")
        return func.HttpResponse(f" Error in Bronze ingestion: {str(e)}", status_code=500)
    

name_silver = "SilverFunction"

@app.function_name(name=name_silver)
@app.blob_trigger(arg_name="myblob", 
                 path=f"{config['container_name']}/{name_silver}",
                 connection=config["connection_string"])
def process_silver_layer(myblob: func.InputStream):
    try:
        logging.info(f"Silver processing triggered by blob: {myblob.name}")
        transform_data_silver_layer(myblob)
        logging.info(f"Silver processing completed for blob: {myblob.name}")
    except Exception as e:
        logging.exception("Error in Silver processing")