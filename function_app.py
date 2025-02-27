import azure.functions as func
from scripts.bronze.bronze_processing import ingest_bronze_data

app = func.FunctionApp()


@app.function_name(name="BronzeFunction")
@app.route(route="bronze")
def main(req: func.HttpRequest) -> func.HttpResponse:
    # Call the bronze layer processing logic
    result = ingest_bronze_data(req)
    return func.HttpResponse(result, status_code=200)