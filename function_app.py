import azure.functions as func
from scripts.bronze.bronze_processing import ingest_bronze_data

app = func.FunctionApp()


@app.function_name(name="BronzeFunction")
@app.route(route="bronze")
# @app.timer_trigger(schedule="0 0 0 0 */12 *", arg_name="myTimer")
def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        ingest_bronze_data(req)  # function doing all the ingestion work
        return func.HttpResponse("Bronze data ingestion successful!", status_code=200)
    except Exception as e:
        return func.HttpResponse(f" Error in Bronze ingestion: {str(e)}", status_code=500)