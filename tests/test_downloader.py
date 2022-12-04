from nemed.downloader import *
from nemed.helper_functions.helpers import _check_cache
import pandas as pd
import pytest


def test_downloader_download_cdeii_table():
    table = download_cdeii_table()
    assert type(table) == pd.DataFrame
    assert all(table.columns == ['STATIONNAME', 'DUID', 'REGIONID', 'CO2E_EMISSIONS_FACTOR',
       'CO2E_ENERGY_SOURCE', 'CO2E_DATA_SOURCE']), "Incorrect column names"


def test_check_cache():
    cache_fp = _check_cache('.\CACHE')
    assert type(cache_fp) == str
    assert os.path.isdir(cache_fp)


def test_check_cache_2():
    assert os.path.isdir('.\helloworldd') == False
    cache_bogus = _check_cache('\helloworldd')
    assert type(cache_bogus) == str
    assert os.path.isdir(cache_bogus)


def test_download_generators_info():
    table = download_generators_info('.\CACHE')
    assert type(table) == pd.DataFrame
    assert all(table.columns == ['Participant', 'Station Name', 'Region', 'Dispatch Type', 'Category',
       'Classification', 'Fuel Source - Primary', 'Fuel Source - Descriptor',
       'Technology Type - Primary', 'Technology Type - Descriptor',
       'Aggregation', 'DUID', 'Reg Cap (MW)']), "Incorrect column names" 


def test_download_duid_auxload():
    raise NotImplementedError("Need to test for all SS/S gens to have an auxload factor")



# def test_throw_err():
#     assert False
