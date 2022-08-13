""" Downloader functions for retrieving data from various sources"""
from nemosis import dynamic_data_compiler
from nemosis.data_fetch_methods import _read_mms_csv
from defaults import CDEII_URL


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


def download_unit_dispatch(
    start_time, end_time, cache, filter_units=None, record="TOTALCLEARED"
):
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
