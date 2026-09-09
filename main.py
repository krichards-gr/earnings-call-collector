import logging

import requests

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _stub_huggingface_get(*args, **kwargs):
    response = requests.Response()
    response.status_code = 200
    response._content = b'{"update_time": "unavailable (stubbed during import)"}'
    return response


def _import_defeatbeta_api_without_welcome_banner():
    """defeatbeta_api unconditionally calls HuggingFace's API at import time
    just to print a decorative welcome banner, and raises RuntimeError if that
    call fails for any reason, including a 429. Under autoscaling, many Cloud
    Run instances cold-start together, all hit that endpoint at once, and get
    rate limited by HuggingFace for longer than any reasonable retry window,
    crash-looping every instance indefinitely. Since the banner's content is
    never used, stub out the HTTP call for the duration of the import instead
    of retrying it; real data requests later still hit HuggingFace normally.
    """
    original_get = requests.Session.get
    requests.Session.get = _stub_huggingface_get
    try:
        import defeatbeta_api  # noqa: F401
    finally:
        requests.Session.get = original_get


_import_defeatbeta_api_without_welcome_banner()

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
