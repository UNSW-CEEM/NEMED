import os


def _check_cache(cache):
    """Check the cache folder directory exists

    Parameters
    ----------
    cache : str
        Folder directory of local cache location.

    Returns
    -------
    str
        Updated cache if not already specified.
    """
    assert(isinstance(cache, str)), "Cache must be a string"

    if not os.path.isdir(cache):
        print("Creating new cache in current directory.")
        if not os.path.isdir(os.path.join(os.getcwd(), "CACHE")):
            os.mkdir("CACHE")
        cache = os.path.join(os.getcwd(), "CACHE")
    return cache
