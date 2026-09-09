import logging
import random
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _import_defeatbeta_api_with_retry(max_attempts=5, base_delay=2):
    """defeatbeta_api hits HuggingFace's API unconditionally at import time
    (to print a welcome banner) and raises RuntimeError on any failure,
    including 429s. Under autoscaling, many instances cold-start at once and
    all hit that endpoint together, triggering rate limiting and crash-looping
    every new instance. Retry with jittered backoff so a transient 429
    doesn't take the instance down and so simultaneous cold starts spread out
    their requests instead of retrying in lockstep.
    """
    for attempt in range(max_attempts):
        try:
            import defeatbeta_api  # noqa: F401
            return
        except RuntimeError as e:
            if attempt == max_attempts - 1:
                raise
            delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
            logger.warning(f"defeatbeta_api import failed ({e}); retrying in {delay:.1f}s")
            time.sleep(delay)


_import_defeatbeta_api_with_retry()

import functions_framework
from sql_get import collect_transcripts

@functions_framework.http
def entry_point(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`.
    """
    request_json = request.get_json(silent=True, force=True)
    request_args = request.args

    tickers_source = 'https://docs.google.com/spreadsheets/d/1Ct9Mw17bRTl-fRIixbMCWsKkhrMlP3LVKADJag5zS8o/edit?gid=95784411#gid=95784411'
    months = 1 # Default to 1 month
    start_date = None

    if request_json and 'tickers' in request_json:
        tickers_source = request_json['tickers']
    elif request_args and 'tickers' in request_args:
        tickers_source = request_args['tickers']
        
    if request_json and 'months' in request_json:
        months = request_json['months']
    elif request_args and 'months' in request_args:
        months = int(request_args['months'])
        
    if request_json and 'start_date' in request_json:
        start_date = request_json['start_date']
    elif request_args and 'start_date' in request_args:
        start_date = request_args['start_date']

    logger.info(f"Triggered Cloud Function. Tickers source: {tickers_source}, Months: {months}, Start Date: {start_date}")
    
    try:
        collect_transcripts(tickers_source, months, start_date)
        return 'Earnings call collection completed successfully.', 200
    except Exception as e:
        logger.exception(f"Error during execution: {e}")
        return f'Error: {str(e)}', 500
