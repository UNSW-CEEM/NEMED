""" Downloader functions for retrieving data from various sources"""
from nemosis import dynamic_data_compiler, static_table
from nemosis.data_fetch_methods import _read_mms_csv
from nempy.historical_inputs.xml_cache import XMLCacheManager as XML
from .defaults import CDEII_URL
from .helper_functions.mod_xml_cache import overwrite_xmlcachemanager_with_pricesetter_config, convert_xml_to_json,\
    read_json_to_df
import os
import glob
from datetime import datetime, timedelta


def download_cdeii_table():
    """Retrieves the most recent Carbon emissions factor data per generation unit (DUID) published to NEMWEB by AEMO.

    Returns
    -------
    pd.DataFrame
        AEMO CDEII data containing columns=["STATIONNAME","DUID","REGIONID","CO2E_EMISSIONS_FACTOR",
        "CO2E_ENERGY_SOURCE","CO2E_DATA_SOURCE"]. CO2E_EMISSIONS_FACTOR is a measure in t CO2-e/MWh
    """
    table = _read_mms_csv(CDEII_URL, usecols=[4, 5, 7, 8, 9, 10])
    return table


def download_generators_info(cache):
    table = static_table(table_name="Generators and Scheduled Loads", raw_data_location=cache)
    return table


def download_unit_dispatch(start_time, end_time, cache, filter_units=None, record="TOTALCLEARED"):
    """Downloads historical generation dispatch data via NEMOSIS.

    Parameters
    ----------
    start_time : str
        Start Time Period in format 'yyyy/mm/dd HH:MM:SS'
    end_time : str
        End Time Period in format 'yyyy/mm/dd HH:MM:SS'
    cache : str
        Raw data location in local directory
    filter_units : list of str
        List of DUIDs to filter data by, by default None
    record : str
        Must be defined as one of ["INITIALMW", "TOTALCLEARED"], by default "TOTALCLEARED"

    Returns
    -------
    pd.DataFrame
        Returns generation data as per NEMOSIS

    Raises
    ------
    ValueError
        The record parameter must be either 'INITIALMW' or 'TOTALCLEARED'
    """
    if record not in ["INITIALMW", "TOTALCLEARED"]:
        raise ValueError(
            "record parameter must be either 'INITIALMW' or 'TOTALCLEARED'"
        )

    if filter_units:
        table = dynamic_data_compiler(
            start_time=start_time,
            end_time=end_time,
            table_name="DISPATCHLOAD",
            raw_data_location=cache,
            select_columns=["SETTLEMENTDATE", "DUID", record],
            filter_cols=["DUID"],
            filter_values=[filter_units],
            fformat="feather",
        )
    else:
        table = dynamic_data_compiler(
            start_time=start_time,
            end_time=end_time,
            table_name="DISPATCHLOAD",
            raw_data_location=cache,
            select_columns=["SETTLEMENTDATE", "DUID", record],
            fformat="feather",
        )

    table.columns = ["Time", "DUID", "Dispatch"]

    return table


def download_pricesetters_xml(cache, start_year, start_month, start_day, end_year, end_month, end_day):
    """Download XML files from AEMO NEMWEB of price setters for each dispatch interval. Converts the downloaded raw
    files to JSON format and stored in cache.

    Parameters
    ----------
    cache : str
        Raw data location in local directory
    start_year : int
        Year in format 20XX
    start_month : int
        Month from 1..12
    start_day : int
        Day from 1..31
    end_year : int
        Year in format 20XX
    end_month : int
        Month from 1..12
    end_day : int
        Day from 1..31
    """
    overwrite_xmlcachemanager_with_pricesetter_config()
    os.chdir(cache)
    xml_cache_manager = XML(cache)
    xml_cache_manager.populate_by_day(start_year=start_year, start_month=start_month, start_day=start_day,
                                      end_year=end_year, end_month=end_month, end_day=end_day)
    convert_xml_to_json(cache, clean_up=False)


def download_pricesetters(cache, start_year, start_month, start_day, end_year, end_month, end_day,
                          redownload_xml=False):
    """Downloads price setter from AEMO NEMWEB for each dispatch interval if JSON files do not already exist in cache.
    Returns this data in a pandas dataframe.

    Parameters
    ----------
    cache : str
        Raw data location in local directory
    start_year : int
        Year in format 20XX
    start_month : int
        Month from 1..12
    start_day : int
        Day from 1..31
    end_year : int
        Year in format 20XX
    end_month : int
        Month from 1..12
    end_day : int
        Day from 1..31
    redownload_xml : bool, optional
        Setting to True will force new download of XML files irrespective of existing files in cache, by default False.

    Returns
    -------
    pd.DataFrame
        Price Setter dataframe containing columns: [PeriodID, RegionID, Market, Price, DUID, DispatchedMarket, BandNo,
        Increase, RRNBandPrice, BandCost]
    """
    if not redownload_xml:
        # Check if JSON files already exist in cache for downloaded data daterange.
        start = datetime(year=start_year, month=start_month, day=start_day) - timedelta(days=1)
        if end_month == 12:
            end_month = 0
            end_year += 1
        end = datetime(year=end_year, month=end_month, day=end_day)
        download_date = start

        JSON_files = glob.glob(os.path.join(cache, "*.json"))

        while download_date <= end:
            searchfor = str(download_date.year) + str(download_date.month).zfill(2) + str(download_date.day).zfill(2)

            if not any([item.__contains__(searchfor) for item in JSON_files]):
                print("No existing JSON found for date {}".format(download_date))
                redownload_xml = True
                break
            download_date += timedelta(days=1)

    # Download PriceSetter XML if not found in cache
    if redownload_xml:
        print("Redownloading XML data")
        download_pricesetters_xml(cache , start_year, start_month, start_day, end_year, end_month, end_day)

    print("Reading JSON to pandas Dataframe")
    table = read_json_to_df(cache)
    return table
