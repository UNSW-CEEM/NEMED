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
    """Retrieves the most recent Carbon emissions factor data per generation unit (DUID) published to NEMWEB by AEMO.

    .. warning::
        This CDEII table is only the most recent data. It is not time-matched to the user requested period!

    Returns
    -------
    pd.DataFrame
        AEMO CDEII data containing columns=["STATIONNAME","DUID","REGIONID","CO2E_EMISSIONS_FACTOR",
        "CO2E_ENERGY_SOURCE","CO2E_DATA_SOURCE"]. CO2E_EMISSIONS_FACTOR is a measure in t CO2-e/MWh
    """
    table = _read_mms_csv(CDEII_URL, usecols=[4, 5, 7, 8, 9, 10])
    return table


def download_generators_info(cache):
    """Retrieves the Generators and Scheduled Loads static table via NEMOSIS (published by AEMO in NEM Registration and
    Exemption List). Data reflects the most recent file uploaded by AEMO.

    .. warning::
        This Generators and Scheduled Load table is only the most recent data. It is not time-matched to the user
        requested period!

    Parameters
    ----------
    cache : str
        Raw data location in local directory.

    Returns
    -------
    pd.DataFrame
        AEMO data containing columns=['Participant', 'Station Name', 'Region', 'Dispatch Type', 'Category',
       'Classification', 'Fuel Source - Primary', 'Fuel Source - Descriptor','Technology Type - Primary',
       'Technology Type - Descriptor', 'Aggregation', 'DUID']
    """
    table = static_table(table_name="Generators and Scheduled Loads", raw_data_location=cache)
    return table


def download_duid_auxload():
    """Retrieves auxilary load data from AEMO's IASR (most recent data)

    .. warning::
        This AuxLoad table is only the most recent data. It is not time-matched to the user requested period!

    Returns
    -------
    pd.DataFrame
        AEMO data containing columns=['DUID', 'Generator', 'Auxiliary Load (%)']
    """
    map = _download_duid_mapping()
    auxload = _download_iasr_existing_gens()
    merged_table = pd.merge(map, auxload, on='Generator', how='left')
    return merged_table


def _download_duid_mapping():
    filepath = Path(__file__).parent / "./data/duid_mapping.csv"
    table = pd.read_csv(filepath)[['DUID', '2021-22-IASR_Generator']]
    table.columns = ['DUID', 'Generator']
    return table


def _download_iasr_existing_gens(select_columns=['Generator', 'Auxiliary Load (%)'], coltype={'Generator': str,
                                 'Auxiliary Load (%)': float}):
    filepath = Path(__file__).parent / "./data/existing_gen_data_summary.csv"
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
    """_summary_

    Parameters
    ----------
    filter_start : str
        Start Time Period to filer from in format 'yyyy/mm/dd HH:MM:SS'
    filter_end : str
        End Time Period to filer from in format 'yyyy/mm/dd HH:MM:SS'


    Returns
    -------
    pd.DataFrame
        Returns AEMO CDEII summary data for FY2122 unless filtered more strictly by input range.
    """
    filepath = Path(__file__).parent / "./data/CO2EII_SUMMARY_RESULTS_FY2122.csv"

    aemo = pd.read_csv(filepath, header=1, usecols=[6, 7, 8, 9, 10])
    aemo['SETTLEMENTDATE'] = pd.to_datetime(aemo['SETTLEMENTDATE'], format="%d/%m/%Y %H:%M")
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
        raise ValueError("record parameter must be either 'INITIALMW' or 'TOTALCLEARED'")

    if record != "INITIALMW":
        raise Exception("Current version does not allow TOTALCLEARED as record in `download_unit_dispatch`")

    shift_stime = datetime.strptime(start_time, "%Y/%m/%d %H:%M:%S")
    shift_stime = shift_stime + timedelta(minutes=DISPATCH_INT_MIN)
    shift_etime = datetime.strptime(end_time, "%Y/%m/%d %H:%M:%S")
    shift_etime = shift_etime + timedelta(minutes=DISPATCH_INT_MIN)
    get_start_time = datetime.strftime(shift_stime, "%Y/%m/%d %H:%M:%S")
    get_end_time = datetime.strftime(shift_etime, "%Y/%m/%d %H:%M:%S")

    if not record == "INITIALMW":
        get_start_time = start_time
        get_end_time = end_time

    # Download Dispatch Load table via NEMOSIS
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

    # Download Dispatch Unit Scada table via NEMOSIS (this includes Non-Scheduled generators)
    if filter_units:
        disp_scada = dynamic_data_compiler(
            start_time=get_start_time,
            end_time=get_end_time,
            table_name="DISPATCH_UNIT_SCADA",
            raw_data_location=cache,
            select_columns=["SETTLEMENTDATE", "DUID", "SCADAVALUE"],
            filter_cols=["DUID"],
            filter_values=[filter_units],
            fformat="feather",
        )
    else:
        disp_scada = dynamic_data_compiler(
            start_time=get_start_time,
            end_time=get_end_time,
            table_name="DISPATCH_UNIT_SCADA",
            raw_data_location=cache,
            select_columns=["SETTLEMENTDATE", "DUID", "SCADAVALUE"],
            fformat="feather",
        )

    # Adjust for value from the beginning of the interval, to match reporting end of interval
    disp_load["Time"] = disp_load["SETTLEMENTDATE"] - timedelta(minutes=DISPATCH_INT_MIN)
    disp_scada["Time"] = disp_scada["SETTLEMENTDATE"] - timedelta(minutes=DISPATCH_INT_MIN)

    # Merge Dispatch Load and Scada tables
    master = pd.merge(left=disp_load[['Time', 'DUID', 'INTERVENTION', record]],
                      right=disp_scada[['Time', 'DUID', 'SCADAVALUE']],
                      on=['Time', 'DUID'],
                      how='outer')

    # Fill in missing data-points and compare conflicting values between INITIALMW and SCADAVALUE
    master['Dispatch'] = np.nan
    master["INITIALMW"] = np.where(master["INITIALMW"].isnull(), master['SCADAVALUE'], master['INITIALMW'])
    master['SCADAVALUE'] = np.where(master["SCADAVALUE"].isnull(), master['INITIALMW'], master['SCADAVALUE'])
    master['Dispatch'] = np.where(abs(master['INITIALMW'] - master['SCADAVALUE']) < 1, master['INITIALMW'],
                                  master['Dispatch'])

    # Report Error Discrepency (if any)
    if not master[master['Dispatch'].isnull()].empty:
        print("ERROR DISCREPENCY between SCADAVALUE and INITIALMW")

    # Final check for intervention periods and duplicates entries
    final = _check_interventions(master)
    final = _clean_duplicates(final)
    return final[['Time', 'DUID', 'Dispatch']]


def _clean_duplicates(table):
    # Take average values where duplicates differ
    table_clean = table.pivot_table(index=["Time", "DUID"], values="Dispatch", aggfunc=np.mean)
    table_clean = table_clean.reset_index()

    # Remove duplicates where Time and DUID match
    table_clean = table_clean.drop_duplicates(subset=["Time", "DUID"])
    return table_clean


def _check_interventions(table):
    # Split table into intervals where intervention has occurred or not
    timestamps_w_intervtn = list(table[table["INTERVENTION"] == 1]["Time"].unique())
    data_unchanged = table[~table["Time"].isin(timestamps_w_intervtn)]
    data_intervtn_updated = table[(table["Time"].isin(timestamps_w_intervtn)) & (table["INTERVENTION"] == 1)]

    # Updates table removing intervention == 0 datapoints for intervals where intervention has occurred
    updated_table = pd.concat([data_unchanged, data_intervtn_updated], ignore_index=True)
    updated_table.sort_values(by=["Time", "DUID"], inplace=True)
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
