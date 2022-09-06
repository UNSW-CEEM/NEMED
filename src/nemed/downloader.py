""" Downloader functions for retrieving data from various sources"""
from nemosis import dynamic_data_compiler, static_table
from nemosis.data_fetch_methods import _read_mms_csv
from nempy.historical_inputs.xml_cache import XMLCacheManager as XML
from .defaults import CDEII_URL, REQ_URL_HEADERS
from .helper_functions.mod_xml_cache import overwrite_xmlcachemanager_with_pricesetter_config, convert_xml_to_json,\
    read_json_to_df
import os
import glob
import numpy as np
import pandas as pd
import requests
from datetime import datetime, timedelta

from pathlib import Path

DISPATCH_INT_MIN = 5


def download_cdeii_table():
    """
    Retrieves the most recent Carbon emissions factor data per generation unit (DUID) published to NEMWEB by AEMO.

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


def download_duid_auxload():
    map = download_duid_mapping()
    auxload = download_iasr_existing_gens()
    merged_table = pd.merge(map, auxload, on='Generator', how='left')
    return merged_table


def download_duid_mapping():
    """_summary_

    Returns
    -------
    _type_
        _description_
    """
    filepath = Path(__file__).parent / "./data/duid_mapping.csv"
    table = pd.read_csv(filepath)[['DUID', '2021-22-IASR_Generator']]
    table.columns = ['DUID', 'Generator']
    return table


def download_iasr_existing_gens(select_columns=['Generator', 'Auxiliary Load (%)'], coltype={'Generator': str,
                                'Auxiliary Load (%)': float}):
    filepath = Path(__file__).parent / "./data/existing_gen_data_summary.csv"#"../../data/existing_gen_data_summary.csv"
    table = pd.read_csv(filepath, dtype=coltype)
    table = table[table.columns[table.columns.isin(select_columns)]]
    return table


def download_aemo_cdeii_summary(year, filter_start, filter_end, cache):
    url = f"https://www.aemo.com.au/-/media/files/electricity/nem/settlements_and_payments/settlements/{year}/co2eii_summary_results_{year}.csv?la=en"
    # filepath = Path(__file__).parent / f"../../data/AEMO_CO2EII_{year}.csv"
    filepath = os.path.join(cache, f'AEMO_CO2EII_{year}.csv')

    r = requests.get(url, headers=REQ_URL_HEADERS)
    with open(filepath, 'wb') as f:
        f.write(r.content)

    aemo = pd.read_csv(filepath, header=1, usecols=[6, 7, 8, 9, 10])

    fil_start_dt = datetime.strptime(filter_start, "%Y/%m/%d %H:%M:%S")
    fil_end_dt = datetime.strptime(filter_end, "%Y/%m/%d %H:%M:%S")

    aemo['SETTLEMENTDATE'] = pd.to_datetime(aemo['SETTLEMENTDATE'], format="%Y/%m/%d %H:%M:%S")
    table = aemo[aemo['SETTLEMENTDATE'].between(fil_start_dt, fil_end_dt)]
    return table.reset_index(drop=True)


def download_current_aemo_cdeii_summary(filter_start, filter_end):
    filepath = Path(__file__).parent / f"./data/CO2EII_SUMMARY_RESULTS_RECENT.csv"

    aemo = pd.read_csv(filepath, header=1, usecols=[6,7,8,9,10])
    aemo['SETTLEMENTDATE'] = pd.to_datetime(aemo['SETTLEMENTDATE'],format="%d/%m/%Y %H:%M")
    table = aemo[aemo['SETTLEMENTDATE'].between(filter_start, filter_end)]

    return table


def get_aemo_comparison_data(filter_start, filter_end, filename='AEMO_CO2EII_August_2022_dataset.csv'):
    # Call the download func.

    filepath = Path(__file__).parent / f"../../data/{filename}"
    aemo = pd.read_csv(filepath, header=1, usecols=[6, 7, 8, 9, 10])

    fil_start_dt = datetime.strptime(filter_start, "%Y/%m/%d %H:%M:%S")
    fil_end_dt = datetime.strptime(filter_end, "%Y/%m/%d %H:%M:%S")

    aemo['SETTLEMENTDATE'] = pd.to_datetime(aemo['SETTLEMENTDATE'], format="%d/%m/%Y %H:%M")
    table = aemo[aemo['SETTLEMENTDATE'].between(fil_start_dt, fil_end_dt)]
    return table


def download_unit_dispatch(start_time, end_time, cache, filter_units=None, record="INITIALMW"):
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
    # Get data from DISPATCHLOAD MMS Table
    if record not in ["INITIALMW", "TOTALCLEARED"]:
        raise ValueError(
            "record parameter must be either 'INITIALMW' or 'TOTALCLEARED'"
        )
    if record == "INITIALMW":
        shift_stime = datetime.strptime(start_time, "%Y/%m/%d %H:%M:%S")
        shift_stime = shift_stime + timedelta(minutes=DISPATCH_INT_MIN)
        shift_etime = datetime.strptime(end_time, "%Y/%m/%d %H:%M:%S")
        shift_etime = shift_etime + timedelta(minutes=DISPATCH_INT_MIN)
        get_start_time = datetime.strftime(shift_stime, "%Y/%m/%d %H:%M:%S")
        get_end_time = datetime.strftime(shift_etime, "%Y/%m/%d %H:%M:%S")
    else:
        get_start_time = start_time
        get_end_time = end_time

    if filter_units:
        disp_load = dynamic_data_compiler(
            start_time=get_start_time,
            end_time=get_end_time,
            table_name="DISPATCHLOAD",
            raw_data_location=cache,
            select_columns=["SETTLEMENTDATE", "DUID", record, "INTERVENTION"],
            filter_cols=["DUID"],
            filter_values=[filter_units],
            fformat="feather",
        )
    else:
        disp_load = dynamic_data_compiler(
            start_time=get_start_time,
            end_time=get_end_time,
            table_name="DISPATCHLOAD",
            raw_data_location=cache,
            select_columns=["SETTLEMENTDATE", "DUID", record, "INTERVENTION"],
            fformat="feather",
        )

    disp_load = _check_interventions(disp_load)
    disp_load = disp_load[["SETTLEMENTDATE", "DUID", record]]

    # Get data from other generators in DISPATCH_UNIT_SCADA MMS Table
    if filter_units:
        disp_scada = dynamic_data_compiler(
            start_time=start_time,
            end_time=end_time,
            table_name="DISPATCH_UNIT_SCADA",
            raw_data_location=cache,
            select_columns=["SETTLEMENTDATE", "DUID", "SCADAVALUE"],
            filter_cols=["DUID"],
            filter_values=[filter_units],
            fformat="feather",
        )
    else:
        disp_scada = dynamic_data_compiler(
            start_time=start_time,
            end_time=end_time,
            table_name="DISPATCH_UNIT_SCADA",
            raw_data_location=cache,
            select_columns=["SETTLEMENTDATE", "DUID", "SCADAVALUE"],
            fformat="feather",
        )

    disp_load.columns = ["Time", "DUID", "Dispatch"]
    disp_scada.columns = ["Time", "DUID", "Dispatch"]

    if record == "INITIALMW":
        disp_load["Time"] = disp_load["Time"] - timedelta(minutes=DISPATCH_INT_MIN)

    table = pd.concat([disp_load, disp_scada])

    final = _clean_duplicates(table)

    return final


def _clean_duplicates(table):
    # TODO: Add warning for duplicate dispatch column greater than 1 MW
    table_clean = table.pivot_table(index=["Time", "DUID"], values="Dispatch", aggfunc=np.mean)
    table_clean = table_clean.reset_index()
    table_clean = table_clean.drop_duplicates(subset=["Time", "DUID"])

    # Check for duplicate timestamped data
    # Warn if difference in dispatch column is great than 1 MW
    # Remove duplicates and return a single entry per DUID
    return table_clean

def _check_interventions(table):
    timestamps_w_intervtn = list(table[table["INTERVENTION"]==1]["SETTLEMENTDATE"].unique())

    data_unchanged = table[~table["SETTLEMENTDATE"].isin(timestamps_w_intervtn)]
    data_intervtn_updated = table[(table["SETTLEMENTDATE"].isin(timestamps_w_intervtn)) & (table["INTERVENTION"]==1)]

    updated_table = pd.concat([data_unchanged, data_intervtn_updated],ignore_index=True)
    updated_table.sort_values(by=["SETTLEMENTDATE","DUID"],inplace=True)
    return updated_table.reset_index(drop=True)

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

    start_date_str = str(start_year) + "/" + str(start_month).zfill(2) + "/" + str(start_day).zfill(2)
    end_date_str = str(end_year) + "/" + str(end_month).zfill(2) + "/" + str(end_day).zfill(2)
    convert_xml_to_json(cache, start_date_str, end_date_str, clean_up=False)


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
    start_date_str = str(start_year) + "/" + str(start_month).zfill(2) + "/" + str(start_day).zfill(2)
    end_date_str = str(end_year) + "/" + str(end_month).zfill(2) + "/" + str(end_day).zfill(2)
    table = read_json_to_df(cache, start_date_str, end_date_str)
    return table
