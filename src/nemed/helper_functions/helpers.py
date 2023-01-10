import os
import logging
from datetime import datetime as dt
logger = logging.getLogger(__name__)


def _check_cache(cache):
    """Check the cache folder directory exists
    Parameters
    ----------
    cache : str
        Folder path location to be used as local cache.
    Returns
    -------
    str
        Existing (or updated, if error occurs) cache directory path.
    """
    try:
        assert(isinstance(cache, str))
    except AssertionError as e:
        logger.exception("Cache input must be a string")
        raise e

    if not os.path.isdir(cache):
        cache = os.path.join(os.getcwd(), "cache_nemed")
        if os.path.isdir(cache):
            logger.warning(f"Input cache path is invalid or not found. Using existing cache directory at {cache}")
        else:
            os.mkdir(cache)
            logger.warning(f"Input cache path is invalid or not found. Creating new cache directory at {cache}")
    return cache


def _validate_variable_type(var, vartype, inputname):
    try:
        assert isinstance(var, vartype), f"`{inputname}` cannot be {var}. Accepted type: {vartype}"
    except AssertionError as e:
        logger.exception(e)
        raise e


def _validate_and_convert_date(date_string, inputname, date_format="%Y/%m/%d %H:%M"):
    try:
        date_obj = dt.strptime(date_string, date_format)
        return date_obj
    except ValueError:
        e = (f"Invalid date string was passed {date_string} as {inputname}. Required date format: {date_format}")
        logger.exception(e)