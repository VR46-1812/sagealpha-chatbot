import azure.functions as func
import logging
from .fetch_csv import run_csv_job

def main(mytimer: func.TimerRequest):
    logging.info("Fetch CSV Timer Trigger Started")
    
    try:
        run_csv_job()
        logging.info("Fetch CSV Completed Successfully")
    except Exception as err:
        logging.error(f"Error in FetchCSVFunction: {err}")
        raise
