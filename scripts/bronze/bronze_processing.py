import logging
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import requests
import json

def ingest_bronze_data(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")
    
    # Authenticate with Azure Key Vault
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url="https://TestLakehouseKeyVault.vault.azure.net/", credential=credential)
    secret = client.get_secret("apikeytmdb")
    
    # Fetch movie data
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
    
    # Handle name parameter
    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
            name = req_body.get('name')
        except ValueError:
            pass
    
    # Return appropriate response
    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
      
        return func.HttpResponse(json.dumps(response_all), mimetype="application/json")
        
   
