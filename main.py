import functions_framework
from sql_get import collect_transcripts
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@functions_framework.http
def entry_point(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`.
    """
    request_json = request.get_json(silent=True)
    request_args = request.args

    tickers_source = 'tickers.csv' # Default to included file
    months = 1 # Default to 1 month

    if request_json and 'tickers' in request_json:
        tickers_source = request_json['tickers']
    elif request_args and 'tickers' in request_args:
        tickers_source = request_args['tickers']
        
    if request_json and 'months' in request_json:
        months = request_json['months']
    elif request_args and 'months' in request_args:
        months = int(request_args['months'])

    logger.info(f"Triggered Cloud Function. Tickers source: {tickers_source}, Months: {months}")
    
    try:
        collect_transcripts(tickers_source, months)
        return 'Earnings call collection completed successfully.', 200
    except Exception as e:
        logger.exception(f"Error during execution: {e}")
        return f'Error: {str(e)}', 500
