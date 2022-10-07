""" Downloader functions for retrieving data from various sources"""
from nemosis import dynamic_data_compiler, static_table
from nemosis.data_fetch_methods import _read_mms_csv
from nempy.historical_inputs.xml_cache import XMLCacheManager as XML
from .defaults import CDEII_URL
from .helper_functions import helpers as hp
from .helper_functions.mod_xml_cache import overwrite_xmlcachemanager_with_pricesetter_config, convert_xml_to_json,\
    read_json_to_df
import os
import glob
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from pathlib import Path

DISPATCH_INT_MIN = 5


def download_cdeii_table():
    """Retrieves the most recent Carbon Emissions Factor data per generation unit (DUID) published to CDEII dataset in
    AEMO NEMWEB.

    .. warning::
        This CDEII table ('Available Generators File') is only the most recent generating units from AEMO's current
        CDEII reporting week. Attempting to retrieve older historical data may lead to missing generating unit emissions
        factors.

    Returns
    -------
    pandas.DataFrame
        AEMO CDEII data containing columns=["STATIONNAME","DUID","REGIONID","CO2E_EMISSIONS_FACTOR",
        "CO2E_ENERGY_SOURCE","CO2E_DATA_SOURCE"]. CO2E_EMISSIONS_FACTOR is a measure in tCO2-e/MWh
    """
    table = _read_mms_csv(CDEII_URL, usecols=[4, 5, 7, 8, 9, 10])
    return table


def download_generators_info(cache):
    """Retrieves the Generators and Scheduled Loads static table via NEMOSIS (published by AEMO in NEM Registration and
    Exemption List file). Data reflects the most recent file uploaded by AEMO.

    .. warning::
        This Generators and Scheduled Load table is only the most recent data. It is not time-matched to the user
        requested period!

    Parameters
    ----------
    cache : str
        Raw data location in local directory.

    Returns
    -------
    pandas.DataFrame
        AEMO data containing columns=['Participant', 'Station Name', 'Region', 'Dispatch Type', 'Category',
       'Classification', 'Fuel Source - Primary', 'Fuel Source - Descriptor','Technology Type - Primary',
       'Technology Type - Descriptor', 'Aggregation', 'DUID']
    """
    hp._check_cache(cache)
    table = static_table(table_name="Generators and Scheduled Loads", raw_data_location=cache)
    return table


def download_duid_auxload():
    """Retrieves auxilary load data from AEMO's IASR (most recent data)

    .. warning::
        This AuxLoad table is only the most recent data. It is not time-matched to the user requested period!

    Returns
    -------
    pandas.DataFrame
        AEMO data containing columns=['DUID', 'Generator', 'Auxiliary Load (%)']
    """
    map = _download_duid_mapping()
    auxload = _download_iasr_existing_gens()
    merged_table = pd.merge(map, auxload, on='Generator', how='left')
    return merged_table


def _download_duid_mapping():
    """A manual record of duid mappings which match unit-level DUIDs to station-level generation names in the IASR
    dataset.

    Returns
    -------
    pandas.DataFrame
        Custom table containing columns=['DUID', 'Generator']
    """
    filepath = Path(__file__).parent / "./data/duid_mapping.csv"
    table = pd.read_csv(filepath)[['DUID', '2021-22-IASR_Generator']]
    table.columns = ['DUID', 'Generator']
    return table


def _download_iasr_existing_gens(select_columns=['Generator', 'Auxiliary Load (%)'], coltype={'Generator': str,
                                 'Auxiliary Load (%)': float}):
    """Retrieves a static data of the 2021 AEMO IASR Gen Data Summary.

    Parameters
    ----------
    select_columns : list(str), optional
        Column names from the IASR to retrieve, by default ['Generator', 'Auxiliary Load (%)']
    coltype : dict, optional
        Data types to use for the selected columns, by default {'Generator': str, 'Auxiliary Load (%)': float}

    Returns
    -------
    pandas.DataFrame
        Table extract of IASR containing columsn specified as `select_columns`
    """
    filepath = Path(__file__).parent / "./data/existing_gen_data_summary.csv"
    table = pd.read_csv(filepath, dtype=coltype)
    table = table[table.columns[table.columns.isin(select_columns)]]
    return table


# def download_aemo_cdeii_summary(year, filter_start, filter_end, cache):
#     url = f"https://www.aemo.com.au/-/media/files/electricity/nem/settlements_and_payments/settlements/{year}/\
#             co2eii_summary_results_{year}.csv?la=en"
#     # filepath = Path(__file__).parent / f"../../data/AEMO_CO2EII_{year}.csv"
#     filepath = os.path.join(cache, f'AEMO_CO2EII_{year}.csv')

#     r = requests.get(url, headers=REQ_URL_HEADERS)
#     with open(filepath, 'wb') as f:
#         f.write(r.content)

#     aemo = pd.read_csv(filepath, header=1, usecols=[6, 7, 8, 9, 10])

#     fil_start_dt = datetime.strptime(filter_start, "%Y/%m/%d %H:%M:%S")
#     fil_end_dt = datetime.strptime(filter_end, "%Y/%m/%d %H:%M:%S")

#     aemo['SETTLEMENTDATE'] = pd.to_datetime(aemo['SETTLEMENTDATE'], format="%Y/%m/%d %H:%M:%S")
#     table = aemo[aemo['SETTLEMENTDATE'].between(fil_start_dt, fil_end_dt)]
#     return table.reset_index(drop=True)


def download_current_aemo_cdeii_summary(filter_start, filter_end, financialyear="1920"):
    """Retrieve the AEMO CDEII daily summary file by financial year.

    Parameters
    ----------
    filter_start : str
        Data download period start, in the format: 'yyyy/mm/dd HH:MM:SS'
    filter_end : str
        Data download period end, in the format: 'yyyy/mm/dd HH:MM:SS'
    financialyear : str, optional
        The financial year to get the cdeii file for, one of ['1920','2122'], by default "1920"

    Returns
    -------
    _type_
        _description_
    """
    assert(financialyear in ['1920', '2122']), "Financial Year must be one of ['1920','2122']"
    filepath = Path(__file__).parent / f"./data/CO2EII_SUMMARY_RESULTS_FY{financialyear}.csv"

    aemo = pd.read_csv(filepath, header=1, usecols=[6, 7, 8, 9, 10])
    aemo['SETTLEMENTDATE'] = pd.to_datetime(aemo['SETTLEMENTDATE'], format="%d/%m/%Y %H:%M")
    table = aemo[aemo['SETTLEMENTDATE'].between(filter_start, filter_end)]
    return table


# def get_aemo_comparison_data(filter_start, filter_end, filename='AEMO_CO2EII_August_2022_dataset.csv'):
#     # Call the download func.

#     filepath = Path(__file__).parent / f"../../data/{filename}"
#     aemo = pd.read_csv(filepath, header=1, usecols=[6, 7, 8, 9, 10])

#     fil_start_dt = datetime.strptime(filter_start, "%Y/%m/%d %H:%M:%S")
#     fil_end_dt = datetime.strptime(filter_end, "%Y/%m/%d %H:%M:%S")

#     aemo['SETTLEMENTDATE'] = pd.to_datetime(aemo['SETTLEMENTDATE'], format="%d/%m/%Y %H:%M")
#     table = aemo[aemo['SETTLEMENTDATE'].between(fil_start_dt, fil_end_dt)]
#     return table


def download_unit_dispatch(start_time, end_time, cache, source_initialmw=False, source_scada=True, overwrite='scada',
                           return_all=True, check=True):
    """Downloads historical generation dispatch data via NEMOSIS.

    Parameters
    ----------
    start_time : str
        Start Time Period in format 'yyyy/mm/dd HH:MM:SS'
    end_time : str
        End Time Period in format 'yyyy/mm/dd HH:MM:SS'
    cache : str
        Raw data location in local directory
    source_initialmw : bool
        Whether to download initialmw column from DISPATCHLOAD table, by default False
    source_scada : bool
        Whether to download scada column from DISPATCH_UNIT_SCADA table, by default True
    overwrite : str
        The data value to overwrite in the returned 'Dispatch' column if there is a discrepency in initialmw and scada.
        Must be one of ['initialmw','scada','average']. If one of source_initialmw or source_scada is False, `overwrite`
        has null effect. By default 'scada'.
    return_all : bool
        Whether to return all columns or only ['Time','DUID','Dispatch'], by default False.
    check : bool
        Whether to check for, and remove duplicates after function is complete, by default True.

    Returns
    -------
    pd.DataFrame
        Returns generation data as per NEMOSIS

    """
    # Check inputs
    hp._check_cache(cache)
    assert(isinstance(start_time, str)), "`start_time` must be a string in format yyyy/mm/dd HH:MM:SS"
    assert(isinstance(end_time, str)), "`end_time` must be a string in format yyyy/mm/dd HH:MM:SS"
    assert(isinstance(overwrite, (str, type(None)))), "`overwrite` must be a string; one of ['initialmw','scada',\
           'average']"
    if overwrite:
        assert(overwrite in ['initialmw', 'scada', 'average']), "`overwrite` must be a string; one of ['initialmw',\
               'scada', 'average']"

    # Adjust timestamps for Scada interval-beginning
    shift_stime = datetime.strptime(start_time, "%Y/%m/%d %H:%M:%S")
    shift_stime = shift_stime + timedelta(minutes=DISPATCH_INT_MIN)
    shift_etime = datetime.strptime(end_time, "%Y/%m/%d %H:%M:%S")
    shift_etime = shift_etime + timedelta(minutes=DISPATCH_INT_MIN)
    get_start_time = datetime.strftime(shift_stime, "%Y/%m/%d %H:%M:%S")
    get_end_time = datetime.strftime(shift_etime, "%Y/%m/%d %H:%M:%S")

    # Download Dispatch Load table via NEMOSIS
    if source_initialmw:
        disp_load = dynamic_data_compiler(
            start_time=get_start_time,
            end_time=get_end_time,
            table_name="DISPATCHLOAD",
            raw_data_location=cache,
            select_columns=["SETTLEMENTDATE", "DUID", "INITIALMW", "INTERVENTION"],
            fformat="feather",
        )
        disp_load["Time"] = disp_load["SETTLEMENTDATE"] - timedelta(minutes=DISPATCH_INT_MIN)

    elif (not source_initialmw) and (not source_scada):
        raise Exception("No source selected. At least one of `source_initialmw` or `source_scada` must be set True")

    # Download Dispatch Unit Scada table via NEMOSIS (this includes Non-Scheduled generators)
    if source_scada:
        disp_scada = dynamic_data_compiler(
            start_time=get_start_time,
            end_time=get_end_time,
            table_name="DISPATCH_UNIT_SCADA",
            raw_data_location=cache,
            select_columns=["SETTLEMENTDATE", "DUID", "SCADAVALUE"],
            fformat="feather",
        )
        disp_scada["Time"] = disp_scada["SETTLEMENTDATE"] - timedelta(minutes=DISPATCH_INT_MIN)

    """ ====> Change this back to fill dispatch col with initialmw and scada and dispatch col, return all """
    # Adjust for value from the beginning of the interval, to match reporting end of interval

    # Merge Dispatch Load and Scada tables
    if source_scada and source_initialmw:
        disp_load2 = _check_interventions(disp_load)
        disp_load2 = _clean_duplicates(disp_load2, value_col="INITIALMW")

        master = pd.merge(left=disp_load2[['Time', 'DUID', 'INITIALMW']],
                          right=disp_scada[['Time', 'DUID', 'SCADAVALUE']],
                          on=['Time', 'DUID'],
                          how='outer')
        master.sort_values(by=['Time', 'DUID'], inplace=True)
        master.reset_index(drop=True, inplace=True)
        # Compare values and fill
        master['Dispatch'] = np.nan
        # Use whichever value is available
        master['Dispatch'] = np.where(master["INITIALMW"].isnull(), master['SCADAVALUE'], master['Dispatch'])
        master['Dispatch'] = np.where(master["SCADAVALUE"].isnull(), master['INITIALMW'], master['Dispatch'])
        # Check both values match, if available
        master['Dispatch'] = np.where(abs(master['INITIALMW'] - master['SCADAVALUE']) < 1, master['SCADAVALUE'],
                                      master['Dispatch'])
    elif source_scada:
        master = disp_scada[['Time', 'DUID', 'SCADAVALUE']]
        master['Dispatch'] = master['SCADAVALUE']
    elif source_initialmw:
        master = disp_load[['Time', 'DUID', 'INTERVENTION', 'INITIALMW']]
        master['Dispatch'] = master['INITIALMW']

    # Report Error Discrepency (if any), overwrite if specified
    if (not overwrite) and (not master[master['Dispatch'].isnull()].empty):
        print("ERROR DISCREPENCY between SCADAVALUE and INITIALMW. No action performed (values likely to be dropped)" +
              " Check data using download_unit_dispatch() and set `return_all`=True, `check`=False")
    elif overwrite and (not master[master['Dispatch'].isnull()].empty):
        print("ERROR DISCREPENCY between SCADAVALUE and INITIALMW. OVERWRITING using {}".format(overwrite))
        master.reset_index(drop=True, inplace=True)
        if overwrite == "initialmw":
            series = master['INITIALMW']
        elif overwrite == "scada":
            series = master['SCADAVALUE']
        elif overwrite == "average":
            series = (master['INITIALMW'] + master['SCADAVALUE']) / 2

        master['Dispatch'] = np.where(abs(master['INITIALMW'] - master['SCADAVALUE']) >= 1, series,
                                      master['Dispatch'])

    # Final check for intervention periods and duplicates entries
    if check:
        # final = _check_interventions(master)
        final = _clean_duplicates(master)
    else:
        final = master

    # Return dataset
    if return_all:
        return final
    else:
        return final[['Time', 'DUID', 'Dispatch']]


def _clean_duplicates(table, value_col="Dispatch"):
    if any(table.duplicated(subset=['Time', 'DUID'])):
        print("Duplicate Timestamped DUIDs found. Updating dataset for duplicates.")
        # Take average values where duplicates differ
        table_clean = table.pivot_table(index=["Time", "DUID"], values=value_col, aggfunc=np.mean)
        table_clean = table_clean.reset_index()

        # Remove duplicates where Time and DUID match
        table_clean = table_clean.drop_duplicates(subset=["Time", "DUID"])
        return table_clean
    else:
        return table


def _check_interventions(table):
    # Split table into intervals where intervention has occurred or not
    timestamps_w_intervtn = list(table[table["INTERVENTION"] == 1]["Time"].unique())

    if timestamps_w_intervtn:
        print("Intervention periods found. Updating dataset for interventions.")
        data_unchanged = table[~table["Time"].isin(timestamps_w_intervtn)]
        data_intervtn_updated = table[(table["Time"].isin(timestamps_w_intervtn)) & (table["INTERVENTION"] == 1)]

        # Updates table removing intervention == 0 datapoints for intervals where intervention has occurred
        updated_table = pd.concat([data_unchanged, data_intervtn_updated], ignore_index=True)
        updated_table.sort_values(by=["Time", "DUID"], inplace=True)
        return updated_table.reset_index(drop=True)
    else:
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
