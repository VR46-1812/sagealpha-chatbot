import azure.functions as func
import logging
from .download_pdfs import run_pdf_job

def main(mytimer: func.TimerRequest):
    logging.info("Download PDF Timer Trigger Started")
    run_pdf_job()
    logging.info("PDF Download Completed")
