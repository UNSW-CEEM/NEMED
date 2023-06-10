""" Downloader functions for retrieving data from various sources"""
from nemosis import dynamic_data_compiler, static_table
from nemosis.data_fetch_methods import _read_mms_csv, _dynamic_data_fetch_loop
from nempy.historical_inputs.xml_cache import XMLCacheManager as XML
from .defaults import *
from .helper_functions import helpers as hp
from .helper_functions.mod_xml_cache import overwrite_xmlcachemanager_with_pricesetter_config, convert_xml_to_json,\
    read_json_to_df, modpricesetter_get_file_name, modpricesetter_download_xml_from_nemweb
from .helper_functions.mod_nemosis import overwrite_nemosis_defaults, mod_dynamic_data_fetch_loop

import logging
import os
import json
import glob
from tqdm import tqdm
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import requests
from pathlib import Path
DISPATCH_INT_MIN = 5
logger = logging.getLogger(__name__)


def download_cdeii_table():
    # """LEGACY. DEPRECATED.
    
    # Retrieves the most recent Carbon Emissions Factor data per generation unit (DUID) published to CDEII dataset in
    # AEMO NEMWEB.

    # .. warning::
    #     This CDEII table ('Available Generators File') is only the most recent generating units from AEMO's current
    #     CDEII reporting week. Attempting to retrieve older historical data may lead to missing generating unit emissions
    #     factors.

    # Returns
    # -------
    # pandas.DataFrame
    #     AEMO CDEII data containing columns=["STATIONNAME","DUID","REGIONID","CO2E_EMISSIONS_FACTOR",
    #     "CO2E_ENERGY_SOURCE","CO2E_DATA_SOURCE"]. CO2E_EMISSIONS_FACTOR is a measure in tCO2-e/MWh
    # """
    # table = _read_mms_csv(CDEII_URL, usecols=[4, 5, 7, 8, 9, 10])
    # return table
    raise Exception("DEPRECATED in this version of NEMED.")



def download_generators_info(cache):
    """Retrieves the Generators and Scheduled Loads static table via NEMOSIS (published by AEMO in NEM Registration and
    Exemption List file). Data reflects the most recent file uploaded by AEMO.

    .. warning::
        This Generators and Scheduled Load table is only the most recent data and is a static file

    Parameters
    ----------
    cache : str
        Raw data location in local directory.

    Returns
    -------
    pandas.DataFrame
        AEMO data containing columns=['Participant', 'Station Name', 'Region', 'Dispatch Type', 'Category',
       'Classification', 'Fuel Source - Primary', 'Fuel Source - Descriptor',
       'Technology Type - Primary', 'Technology Type - Descriptor',
       'Aggregation', 'DUID', 'Reg Cap (MW)']
    """
    cache = hp._check_cache(cache)
    table = static_table(table_name="Generators and Scheduled Loads", raw_data_location=cache)
    return table


def download_duid_auxload():
    # """DEPRECATED: Replaced by read_plant_auxload_csv()
    # Retrieves auxilary load data from static AEMO's ISP assumptions.

    # .. info::
    #     This data is obtained from a static datasource for which the majority of plant data is obtained from the 2020
    #     ISP. More recent data published by AEMO does not reflect unit-level, rather only fuel/technology type
    #     assumptions. Refer to NEMED documentation for more details.

    # Returns
    # -------
    # pandas.DataFrame
    #     AEMO data containing columns=['EFFECTIVEFROM', 'DATASOURCE', 'DUID', 'GENERATOR', 'PCT_AUXILIARY_LOAD', 'NOTE']
    # """
    # map = _download_duid_mapping()
    # auxload = _download_iasr_existing_gens()
    # merged_table = pd.merge(map, auxload, on='Generator', how='left')
    # return merged_table
    raise Exception("DEPRECATED in this version of NEMED. See `read_plant_auxload_csv`")


def read_plant_auxload_csv(select_columns=['EFFECTIVEFROM', 'DUID', 'PCT_AUXILIARY_LOAD'],
                            coltype={'EFFECTIVEFROM': str, 'DUID': str, 'PCT_AUXILIARY_LOAD': float}):
    """Reads locally stored .csv in package with auxiliary load data mapped to each DUID. Users can update this .csv
    with custom/missing values should they wish.

    Parameters
    ----------
    select_columns : list, optional
        Columns of the dataset to return, by default ['EFFECTIVEFROM', 'DUID', 'PCT_AUXILIARY_LOAD']
    coltype : dict, optional
        Datatype corresponding to each field, by default {'EFFECTIVEFROM': datetime, 'DUID': str,
        'PCT_AUXILIARY_LOAD': float}

    Returns
    -------
    pandas.DataFrame
        Custom table containing columns=['EFFECTIVEFROM', 'DUID', 'PCT_AUXILIARY_LOAD']
    """
    filepath = Path(__file__).parent / "./data/plant_auxiliary/_plant_auxload_assumptions.csv"
    table = pd.read_csv(filepath)
    table['EFFECTIVEFROM'] = pd.to_datetime(table['EFFECTIVEFROM'], format="%d/%m/%Y")
    return table[table.columns[table.columns.isin(select_columns)]]


def download_plant_emissions_factors(start_date, end_date, cache):
    """Retrieves CO2-equivalent emissions intensity factors (tCO2-e/MWh) for each generator. Metric is reflective of
    sent-out generation. Underlying data is sourced from the 'GENUNITS' table of AEMO MMS at monthly time resolution.

    Parameters
    ----------
    cache : str
        Raw data location in local directory
    start_date : str
        Data download period start, in the format: 'yyyy/mm/dd HH:MM'
    end_date : str
        Data download period end, in the format: 'yyyy/mm/dd HH:MM'

    Returns
    -------
    pandas.DataFrame
        Plant Emissions Factor Data with columns=['file_year', 'file_month', 'GENSETID', 'CO2E_EMISSIONS_FACTOR',
        'CO2E_ENERGY_SOURCE', 'CO2E_DATA_SOURCE'] 

    Raises
    ------
    Exception
        Data Unavailable for dates prior 05-2011
    """
    # Input Validation
    hp._validate_variable_type(start_date, str, "start_date")
    hp._validate_variable_type(end_date, str, "end_date")
    start_date = hp._validate_and_convert_date(start_date, "start_date")
    end_date = hp._validate_and_convert_date(end_date, "end_date")
    cache = hp._check_cache(cache)

    overwrite_nemosis_defaults()

    if start_date < datetime(2011,5,1):
        raise Exception("DATA UNAVAILABLE: GENUNITS table is not found in MMS prior to 05-2011. \
                        Unit emissions factors cannot be obtained.")

    # Data Retrieval
    df = mod_dynamic_data_fetch_loop(start_search=start_date,
                                    start_time=start_date,
                                    end_time=end_date,
                                    table_name="GENUNITS",
                                    raw_data_location=cache,
                                    select_columns=["GENSETID", "CO2E_EMISSIONS_FACTOR", "CO2E_ENERGY_SOURCE", \
                                        'CO2E_DATA_SOURCE'],
                                    date_filter=None,
                                    fformat="feather",
                                    )
    df = pd.concat(df)
    df['file_year'] = df['file_year'].astype(int)
    df['file_month'] = df['file_month'].astype(int)
    df['CO2E_EMISSIONS_FACTOR'] = df['CO2E_EMISSIONS_FACTOR'].astype(float)
    return df.sort_values(['GENSETID','file_year','file_month'])


def download_genset_map(cache, asof_date=None):
    """Download the GENSETID to DUID mapping from DUALLOC MMS Table.

    Parameters
    ----------
    cache : str
        Raw data location in local directory
    asof_date : str, optional
        Date to retrieve DUALLOC table as of, in the format: 'yyyy/mm/dd HH:MM', by default None which will retrieve
        recent data 

    Returns
    -------
    pandas.DataFrame

        =============  =====  ================================================
        Columns:       Type:  Description:
        EFFECTIVEDATE  str    Effective Date as defined by AEMO.
        DUID           str    Dispatchable Unit Identifier as defined by AEMO.
        GENSETID       str    Generator Set Identifier as defined by AEMO.
        =============  =====  ================================================

    Raises
    ------
    Exception
        Parameter `asof_date` exceeds the earliest available DUALLOC table in MMS
    """
    if asof_date != None:
        latest = hp._validate_and_convert_date(asof_date, "asof_date")
    else:
        latest = datetime(datetime.now().year, datetime.now().month, 1) - timedelta(days = 90)
    
    cache = hp._check_cache(cache)
    overwrite_nemosis_defaults()

    if latest < datetime(2020,10,1):
        raise Exception("DATA UNAVAILABLE: DUALLOC table is not found in MMS prior to 10-2020. " + \
                        "Cannot correctly map emissions factors. Retry function without specifying `asof_date`")

    # Data Retrieval
    df = mod_dynamic_data_fetch_loop(start_search=latest,
                                    start_time=latest - timedelta(hours=1),
                                    end_time=latest,
                                    table_name="DUALLOC",
                                    raw_data_location=cache,
                                    select_columns=["EFFECTIVEDATE", "DUID", "GENSETID", "LASTCHANGED"],
                                    date_filter=None,
                                    fformat="feather",
                                    )
    df = pd.concat(df)
    df = df.drop(['file_year','file_month', 'LASTCHANGED'], axis=1)
    filtered = df.sort_values('EFFECTIVEDATE').drop_duplicates(['GENSETID'], keep='last')
    return filtered.sort_values(['GENSETID','EFFECTIVEDATE']).reset_index(drop=True)


def download_dudetailsummary(cache, asof_date=None):
    """Download the DUDETAILSUMMARY MMS table with mapping of Dispatch Type and Region to DUID

    Parameters
    ----------
    cache : str
        Raw data location in local directory
    asof_date : str, optional
        Date to retrieve DUALLOC table as of, in the format: 'yyyy/mm/dd HH:MM', by default None which will retrieve
        recent data 

    Returns
    -------
    pandas.DataFrame

        ============  =====  ================================================
        Columns:      Type:  Description:
        DUID          str    Dispatchable Unit Identifier as defined by AEMO.
        START_DATE    str    Date of data entry as defined by AEMO.
        DISPATCHTYPE  str    Dispatch Type of DUID as 'GENERATOR' or 'LOAD'.
        REGIONID      str    Region of DUID.
        ============  =====  ================================================

    Raises
    ------
    Exception
        Parameter `asof_date` exceeds the earliest available DUALLOC table in MMS
    """
    if asof_date != None:
        latest = hp._validate_and_convert_date(asof_date, "asof_date")
    else:
        latest = datetime(datetime.now().year, datetime.now().month, 1) - timedelta(days = 90)
    
    cache = hp._check_cache(cache)
    overwrite_nemosis_defaults()

    if latest < datetime(2009,7,1):
        raise Exception("DATA UNAVAILABLE: DUDETAILSUMMARY table is not found in MMS prior to 07-2009. " + \
                        "Cannot correctly map emissions factors. Retry function without specifying `asof_date`")

    # Data Retrieval
    df = mod_dynamic_data_fetch_loop(start_search=latest,
                                    start_time=latest - timedelta(hours=1),
                                    end_time=latest,
                                    table_name="DUDETAILSUMMARY",
                                    raw_data_location=cache,
                                    select_columns=["START_DATE", "DUID", "DISPATCHTYPE", "REGIONID", "LASTCHANGED"],
                                    date_filter=None,
                                    fformat="feather",
                                    )
    df = pd.concat(df)
    df = df.drop(['file_year','file_month', 'LASTCHANGED'], axis=1)
    filtered = df.sort_values('START_DATE').drop_duplicates(['DUID'], keep='last')
    return filtered.sort_values(['DUID','START_DATE']).reset_index(drop=True)


def _download_duid_mapping():
    # """LEGACY. TO BE DEPRECATED.
    
    # A manual record of duid mappings which match unit-level DUIDs to station-level generation names in the IASR
    # dataset.

    # Returns
    # -------
    # pandas.DataFrame
    #     Custom table containing columns=['DUID', 'Generator']
    # """
    # filepath = Path(__file__).parent / "./data/duid_mapping.csv"
    # table = pd.read_csv(filepath)[['DUID', '2021-22-IASR_Generator']]
    # table.columns = ['DUID', 'Generator']
    # return table
    raise Exception("DEPRECATED in this version of NEMED. Use `download_genset_map`")


def _download_iasr_existing_gens(select_columns=['Generator', 'Auxiliary Load (%)'], coltype={'Generator': str,
                                 'Auxiliary Load (%)': float}):
    # """LEGACY. TO BE DEPRECATED.
    
    # Retrieves a static data of the 2021 AEMO IASR Gen Data Summary.

    # Parameters
    # ----------
    # select_columns : list(str), optional
    #     Column names from the IASR to retrieve, by default ['Generator', 'Auxiliary Load (%)']
    # coltype : dict, optional
    #     Data types to use for the selected columns, by default {'Generator': str, 'Auxiliary Load (%)': float}

    # Returns
    # -------
    # pandas.DataFrame
    #     Table extract of IASR containing columns specified as `select_columns`
    # """
    # filepath = Path(__file__).parent / "./data/existing_gen_data_summary.csv"
    # table = pd.read_csv(filepath, dtype=coltype)
    # table = table[table.columns[table.columns.isin(select_columns)]]
    # return table
    raise Exception("DEPRECATED in this verion of NEMED")


def download_aemo_cdeii_summary(filter_start, filter_end, cache):
    """Downloads and combines selected AEMO CDEII Summary Files for a specified date range. The files processed here are
    available from:
    https://aemo.com.au/en/energy-systems/electricity/national-electricity-market-nem/market-operations/settlements-and-payments/settlements/carbon-dioxide-equivalent-intensity-index

    Parameters
    ----------
    filter_start : str
        Data download period start, in the format: 'yyyy/mm/dd HH:MM:SS'
    filter_end : str
        Data download period end, in the format: 'yyyy/mm/dd HH:MM:SS'
    cache : str
        Raw data location in local directory

    Returns
    -------
    pd.DataFrame
        AEMO data containing columns = 'SETTLEMENTDATE', 'REGIONID', 'TOTAL_SENT_OUT_ENERGY',
       'TOTAL_EMISSIONS', 'CO2E_INTENSITY_INDEX']

    Raises
    ------
    ValueError
        Where 'filter_start' exceeds the earliest available CDEII data file supported by NEMED.
    """
    cache = hp._check_cache(cache)

    # Check filter start and end in defaults files datarange.
    fil_start_dt = datetime.strptime(filter_start, "%Y/%m/%d %H:%M")
    fil_end_dt = datetime.strptime(filter_end, "%Y/%m/%d %H:%M")

    default_start_yearname, default_end_yearname = list(CDEII_SUMFILES)[0], list(CDEII_SUMFILES)[-1]

    if fil_start_dt < datetime.strptime(CDEII_SUMFILES[default_start_yearname]['start'],
                                        CDEII_SUMFILES_DTFMT[default_start_yearname]):
        raise ValueError("'filter_start' date exceeds the available CDEII data backdated to {}"\
                            .format(CDEII_SUMFILES[default_start_yearname]['start']))
    elif (fil_end_dt > datetime.strptime(CDEII_SUMFILES[default_end_yearname]['end'],
                                        CDEII_SUMFILES_DTFMT[default_end_yearname])):
        extract_current = True
        if fil_start_dt >= datetime.strptime(CDEII_SUMFILES[default_end_yearname]['start'],
                                        CDEII_SUMFILES_DTFMT[default_end_yearname]):
            extract_historical = False
        else:
            extract_historical = True
    else:
        extract_historical = True
        extract_current = False

    aemodata = []
    if extract_historical:
        # Extract CDEII datafiles for historical
        files_sdt = [datetime.strptime(CDEII_SUMFILES[i]['start'], CDEII_SUMFILES_DTFMT[i]) \
                     for i in CDEII_SUMFILES.keys()]
        files_edt = [datetime.strptime(CDEII_SUMFILES[i]['end'], CDEII_SUMFILES_DTFMT[i]) \
                     for i in CDEII_SUMFILES.keys()]

        # Find nearest dataseries start date
        if np.searchsorted(files_sdt, fil_start_dt)==0:
            extract_from_idx = np.searchsorted(files_sdt, fil_start_dt)
        else :
            extract_from_idx = (np.searchsorted(files_sdt, fil_start_dt)-1)
        ## extract_from = list(CDEII_SUMFILES)[extract_from_idx]

        # Find nearest dataseries end date
        if np.searchsorted(files_edt, fil_end_dt)==0:
            extract_to_idx = np.searchsorted(files_edt, fil_end_dt)+1
        else :
            extract_to_idx = (np.searchsorted(files_edt, fil_end_dt))

        # If date falls in current datafile (not historical)
        if extract_to_idx > len(list(CDEII_SUMFILES)) - 1:
            extract_to_idx = (len(list(CDEII_SUMFILES)) - 1)
        ## extract_to = list(CDEII_SUMFILES)[extract_to_idx]

        # Extract Datafile from AEMO
        for idx in range(extract_from_idx, extract_to_idx+1):
            yearname = list(CDEII_SUMFILES)[idx]
            year = CDEII_SUMFILES[yearname]['year']
            urlbase = CDEII_SUMFILES[yearname]['url']
            # Manual request for change in file naming on aemo website
            if year == '2015':
                url = urlbase
            elif urlbase == 'http://nemweb.com.au/Reports/Current/CDEII/':
                url = urlbase + f"CO2EII_SUMMARY_RESULTS_{year}.CSV"
            else:
                url = urlbase + f'{year}/co2eii_summary_results_{yearname}.csv?la=en'

            filepath = os.path.join(cache, f'AEMO_CO2EII_{yearname}.csv')
            if os.path.exists(filepath):
                os.remove(filepath)
            r = requests.get(url, headers=REQ_URL_HEADERS)
            with open(filepath, 'wb') as f:
                f.write(r.content)
            aemo_file = pd.read_csv(filepath, header=1, usecols=[6, 7, 8, 9, 10])
            aemo_file['SETTLEMENTDATE'] = pd.to_datetime(aemo_file['SETTLEMENTDATE'],
                                                         format=CDEII_SUMFILES_DTFMT[yearname])
            aemodata += [aemo_file]

    if extract_current:
        # Extract CDEII datafiles from current file
        print(f"Extracting AEMO CDEII Datafile for: CURRENT")
        url = "https://www.nemweb.com.au/Reports/Current/CDEII/CO2EII_SUMMARY_RESULTS.CSV"
        filepath = os.path.join(cache, f'AEMO_CO2EII_CURRENT.csv')
        r = requests.get(url, headers=REQ_URL_HEADERS)
        with open(filepath, 'wb') as f:
            f.write(r.content)
        aemo_file = pd.read_csv(filepath, header=1, usecols=[6, 7, 8, 9, 10])
        aemo_file['SETTLEMENTDATE'] = pd.to_datetime(aemo_file['SETTLEMENTDATE'], format="%Y/%m/%d %H:%M:%S")
        aemodata += [aemo_file]

    table = pd.concat(aemodata)
    table = table[table['SETTLEMENTDATE'].between(fil_start_dt, fil_end_dt, inclusive="left")]
    if max(table['SETTLEMENTDATE']) < fil_end_dt:
        print("WARNING: 'filter_end' date exceeds available data from CURRENT CDEII. Latest available data is {}"\
            .format(max(table['SETTLEMENTDATE'])))
    return table.sort_values('SETTLEMENTDATE').reset_index(drop=True)


def download_current_aemo_cdeii_summary(filter_start, filter_end, financialyear="1920"):
    # """
    # LEGACY. TO BE DEPRECATED.
    # .. warning::
    #     TO BE DEPRECATED IN FUTURE VERSIONS. Replaced by **download_aemo_cdeii_summary**
   
    # Retrieve the AEMO CDEII daily summary file by financial year.

    # Parameters
    # ----------
    # filter_start : str
    #     Data download period start, in the format: 'yyyy/mm/dd HH:MM:SS'
    # filter_end : str
    #     Data download period end, in the format: 'yyyy/mm/dd HH:MM:SS'
    # financialyear : str, optional
    #     The financial year to get the cdeii file for, one of ['1920','2122'], by default "1920"

    # Returns
    # -------
    # _type_
    #     _description_
    # """
    # assert(financialyear in ['1920', '2122']), "Financial Year must be one of ['1920','2122']"
    # filepath = Path(__file__).parent / f"./data/CO2EII_SUMMARY_RESULTS_FY{financialyear}.csv"

    # aemo = pd.read_csv(filepath, header=1, usecols=[6, 7, 8, 9, 10])
    # aemo['SETTLEMENTDATE'] = pd.to_datetime(aemo['SETTLEMENTDATE'], format="%d/%m/%Y %H:%M")
    # table = aemo[aemo['SETTLEMENTDATE'].between(filter_start, filter_end)]
    # return table
    raise Exception("DEPRECATED in this version of NEMED. Use `download_aemo_cdeii_summary`")


def get_aemo_comparison_data(filter_start, filter_end, filename='AEMO_CO2EII_August_2022_dataset.csv'):
    # # Call the download func.

    # filepath = Path(__file__).parent / f"../../data/{filename}"
    # aemo = pd.read_csv(filepath, header=1, usecols=[6, 7, 8, 9, 10])

    # fil_start_dt = datetime.strptime(filter_start, "%Y/%m/%d %H:%M:%S")
    # fil_end_dt = datetime.strptime(filter_end, "%Y/%m/%d %H:%M:%S")

    # aemo['SETTLEMENTDATE'] = pd.to_datetime(aemo['SETTLEMENTDATE'], format="%d/%m/%Y %H:%M")
    # table = aemo[aemo['SETTLEMENTDATE'].between(fil_start_dt, fil_end_dt)]
    # return table
    raise Exception("DEPRECATED in this version of NEMED.")


def download_unit_dispatch(start_time, end_time, cache, source_initialmw=False, source_scada=True, overwrite='scada',
                           return_all=True, check=True, rm_negative=True):
    """Downloads historical generation dispatch data via NEMOSIS.

    Parameters
    ----------
    start_time : str
        Start Time Period in format 'yyyy/mm/dd HH:MM'
    end_time : str
        End Time Period in format 'yyyy/mm/dd HH:MM'
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
    rm_negative: bool
        Checks for negative dispatch values in SCADA and replaces them with zero, by default True.

    Returns
    -------
    pd.DataFrame
        Returns generation data as per NEMOSIS

    """
    # Check inputs
    cache = hp._check_cache(cache)
    assert(isinstance(start_time, str)), "`start_time` must be a string in format yyyy/mm/dd HH:MM"
    assert(isinstance(end_time, str)), "`end_time` must be a string in format yyyy/mm/dd HH:MM"
    assert(isinstance(overwrite, (str, type(None)))), "`overwrite` must be a string; one of ['initialmw','scada',\
           'average']"
    if overwrite:
        assert(overwrite in ['initialmw', 'scada', 'average']), "`overwrite` must be a string; one of ['initialmw',\
               'scada', 'average']"

    # Adjust timestamps for Scada interval-beginning
    shift_stime = datetime.strptime(start_time, "%Y/%m/%d %H:%M")
    shift_stime = shift_stime + timedelta(minutes=DISPATCH_INT_MIN)
    shift_etime = datetime.strptime(end_time, "%Y/%m/%d %H:%M")
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

    if rm_negative:
        final['Dispatch'] = np.where(final['Dispatch'] < 0, 0, final['Dispatch'])

    # Return dataset
    if return_all:
        return final
    else:
        return final[['Time', 'DUID', 'Dispatch']]


def _clean_duplicates(table, value_col="Dispatch"):
    """Clean duplicate data of Time and DUID from dataframe"""
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
    """Check and account for intervention flag for data from DISPATCHLOAD MMS table. Not required for DISPATCH_UNIT_SCADA table"""
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


def download_pricesetter_files(start_time, end_time, cache):
    """Download NEM Price Setter files from MMS table.
    First caches raw XML files as JSON and then reads and returns data in the form of pandas.DataFrame.
    Processed data only considers the marginal generator for the Energy market.

    For further explaination on NEMPriceSetting refer to: https://aemo.com.au/-/media/files/electricity/nem/it-systems-and-change/nemde-queue/nemde_queue_users_guide.pdf?la=en

    Parameters
    ----------
    cache : str
        Raw data location in local directory
    start_time : str
        Start Time Period in format 'yyyy/mm/dd HH:MM'
    end_time : str
        End Time Period in format 'yyyy/mm/dd HH:MM'

    Returns
    -------
    pandas.DataFrame

        ============  ========  ================================================================================================
        Columns:      Type:     Description:
        PeriodID      datetime  The NEM market dispatch interval.
        RegionID      str       The NEM market region.
        Price         float     The market price for dispatch interval.
        Unit          str       A DUID who contributes to setting the price (in most cases).
        BandNo        int       Trade band number of the unit's contribution to price setting.
        Increase      float     A marginal increase (in MW) in the unit band for a 1MW increase in energy demand for the region.
        RRNBandPrice  float     Unit Band price as referred to the RRN
        BandCost      float     Amount in $/h (Increase column multiplied by RRNBandPrice)
        ============  ========  ================================================================================================

    """
    # Check inputs
    cache = hp._check_cache(cache)
    start_time = hp._validate_and_convert_date(start_time, "start_time")
    end_time = hp._validate_and_convert_date(end_time, "end_time")

    # Adjust data collection date range
    collect_sdt = datetime(start_time.year, start_time.month, start_time.day)
    if (end_time.hour == 0) & (end_time.minute == 0):
        collect_edt = datetime(end_time.year, end_time.month, end_time.day)
    else:
        collect_edt = datetime(end_time.year, end_time.month, end_time.day) + timedelta(days=1)

    daterange_list = pd.date_range(collect_sdt, collect_edt)

    # Check if any files in cache already exist within daterange, remove them from new dateranges to create json for new dates only
    xml_files = glob.glob(os.path.join(cache, "NEMED_PS_DAILY_*.json"))
    exist_file_list = [datetime.strptime(existing_files[-15:-5],"%Y-%m-%d") for existing_files in xml_files]
    new_daterange_only = [x for x in daterange_list if x not in exist_file_list]

    # Download & Process Price Setter Files
    logger.info("Processing Price Setter Files...")
    for date in tqdm(new_daterange_only):
        try:
            _populate_xml_into_daily_json(cache, year=date.year, month=date.month, day=date.day)
        except:
            logger.warning("PriceSetter Download for {} failed. Continuing with remaining dates...".format(date))

    # Read cached JSON Price Setter Files
    table = read_json_to_df(start_time, end_time, cache)
    return table


def _populate_xml_into_daily_json(cache, year, month, day, rm_xml=True):
    """Iterative function to extract all price setter xml files for a single day and combined into a single JSON file"""
    sdate = datetime(year, month, day)
    edate = sdate + timedelta(days=1)
    date_str_list = [datetime.strftime(i,"%Y/%m/%d %H:%M:%S") for i in pd.date_range(sdate, edate, freq='5T', \
        inclusive="right")]

    # Load individual xml files
    dataset = []
    for interval in date_str_list:
        xml_cache_manager = XML(cache)
        overwrite_xmlcachemanager_with_pricesetter_config()

        xml_cache_manager.load_interval(interval)
        d = xml_cache_manager.xml['SolutionAnalysis']['PriceSetting']
        dataset += [d]
    dataset = [item for sublist in dataset for item in sublist]

    # Write JSON daily summary file to cache
    write_file = "NEMED_PS_DAILY_" + sdate.strftime("%Y-%m-%d") + ".json"
    json_path = os.path.join(cache, write_file)
    with open(json_path, 'w') as fp:
        json.dump(dataset, fp)

    if rm_xml:
        # Remove XML files cached by nempy XMLCacheManager
        prev_date = datetime(year, month, day) - timedelta(days=1)
        XML_files = glob.glob(os.path.join(cache, "NEMPriceSetter_{}{}{}*.xml"\
            .format(year,str(month).zfill(2),str(day).zfill(2))))
        XML_files += glob.glob(os.path.join(cache, "NEMPriceSetter_{}{}{}*.xml"\
            .format(prev_date.year,str(prev_date.month).zfill(2),str(prev_date.day).zfill(2))))

        for filepath in XML_files:
            os.remove(filepath)
    return



def download_pricesetters(cache, start_year, start_month, start_day, end_year, end_month, end_day,
                          redownload_xml=False):
    # """LEGACY: Deprecated

    # Downloads price setter from AEMO NEMWEB for each dispatch interval if JSON files do not already exist in cache.
    # Returns this data in a pandas dataframe.

    # Parameters
    # ----------
    # cache : str
    #     Raw data location in local directory
    # start_year : int
    #     Year in format 20XX
    # start_month : int
    #     Month from 1..12
    # start_day : int
    #     Day from 1..31
    # end_year : int
    #     Year in format 20XX
    # end_month : int
    #     Month from 1..12
    # end_day : int
    #     Day from 1..31
    # redownload_xml : bool, optional
    #     Setting to True will force new download of XML files irrespective of existing files in cache, by default False.

    # Returns
    # -------
    # pd.DataFrame
    #     Price Setter dataframe containing columns: [PeriodID, RegionID, Market, Price, DUID, DispatchedMarket, BandNo,
    #     Increase, RRNBandPrice, BandCost]
    # """

    # if not redownload_xml:
    #     # Check if JSON files already exist in cache for downloaded data daterange.
    #     start = datetime(year=start_year, month=start_month, day=start_day) - timedelta(days=1)
    #     if end_month == 12:
    #         end_month = 0
    #         end_year += 1
    #     end = datetime(year=end_year, month=end_month, day=end_day)
    #     download_date = start

    #     JSON_files = glob.glob(os.path.join(cache, "*.json"))

    #     while download_date <= end:
    #         searchfor = str(download_date.year) + str(download_date.month).zfill(2) + str(download_date.day).zfill(2)

    #         if not any([item.__contains__(searchfor) for item in JSON_files]):
    #             print("No existing JSON found for date {}".format(download_date))
    #             redownload_xml = True
    #             break
    #         download_date += timedelta(days=1)

    # # Download PriceSetter XML if not found in cache
    # if redownload_xml:
    #     print("Redownloading XML data")
    #     download_pricesetters_xml(cache , start_year, start_month, start_day, end_year, end_month, end_day)

    # print("Reading JSON to pandas Dataframe")
    # start_date_str = str(start_year) + "/" + str(start_month).zfill(2) + "/" + str(start_day).zfill(2)
    # end_date_str = str(end_year) + "/" + str(end_month).zfill(2) + "/" + str(end_day).zfill(2)
    # table = read_json_to_df(cache, start_date_str, end_date_str)
    # return table
    raise Exception("DEPRECATED in this version of NEMED. Refer to documentation: https://nemed.readthedocs.io/en/latest/")

