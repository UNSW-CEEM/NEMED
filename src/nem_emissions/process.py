""" Process functions for calculations based on downloaded data """
from datetime import datetime
import pandas as pd
import numpy as np
from downloader import download_cdeii_table, download_unit_dispatch

DISP_INT_LENGTH = 5 / 60


def get_total_emissions_by_DI_DUID(
    start_time, end_time, cache, filter_units=None, filter_regions=None
):
    """Find the total emissions for each generation unit per dispatch interval. Calculates the Total_Emissions column by
    multiplying Energy (Dispatch (MW) * 5/60 (h)) with Plant Emissions Intensity (tCO2-e/MWh)

    Parameters
    ----------
    start_time : str
                                                                                                                                                                                                                                                                    Start Time Period in format 'yyyy/mm/dd HH:MM:SS'
    end_time : str
                                                                                                                                                                                                                                                                    End Time Period in format 'yyyy/mm/dd HH:MM:SS'
    cache : str
                                                                                                                                                                                                                                                                    Raw data location in local directory
    ilter_units : list of str
                                                                                                                                                                                                                                                                    List of DUIDs to filter data by, by default None

    Returns
    -------
    pd.DataFrame
                                                                                                                                    Calculated data table containing columns=["Time","DUID","Plant_Emissions_Intensity","Energy",
                                                                                                                                    "Total_Emissions"]. Plant_Emissions_Intensity is a static metric in tCO2-e/MWh, Energy is in MWh, Total
                                                                                                                                    Emissions in tCO2-e.
    """
    cdeii_df = download_cdeii_table()
    disp_df = download_unit_dispatch(
        start_time, end_time, cache, filter_units, record="TOTALCLEARED"
    )
    result = pd.merge(
        disp_df,
        cdeii_df[["DUID", "REGIONID", "CO2E_EMISSIONS_FACTOR"]],
        how="left",
        on="DUID",
    )
    if filter_regions:
        if not pd.Series(cdeii_df["REGIONID"].unique()).isin(filter_regions).any():
            raise ValueError(
                "filter_region paramaters passed were not found in NEM regions"
            )
        result = result[result["REGIONID"].isin(filter_regions)]
    if result.empty:
        print(
            "WARNING: Emissions Dataframe is empty. Check filter_units and filter_regions parameters do not conflict!"
        )
    result["Energy"] = result["Dispatch"] * DISP_INT_LENGTH
    result["Total_Emissions"] = result["Energy"] * result["CO2E_EMISSIONS_FACTOR"]
    result.rename(
        columns={"CO2E_EMISSIONS_FACTOR": "Plant_Emissions_Intensity"}, inplace=True
    )
    return result[result.columns[~result.columns.isin(["Dispatch"])]]


def get_total_emissions_by_(start_time, end_time, cache, by="interval"):
    # NOTE: ISSUE with timing and aggregation. Needing to shift everything 5 minutes to start of interval for accounting.
    # Currently all in time ending.
    raw_table = get_total_emissions_by_DI_DUID(
        start_time, end_time, cache, filter_units=None, filter_regions=None
    )

    data = raw_table.pivot_table(
        index="Time",
        columns="REGIONID",
        values=["Energy", "Total_Emissions"],
        aggfunc="sum",
    )

    for region in data.columns.levels[1]:
        data[("Intensity_Index", region)] = (
            data["Total_Emissions"][region] / data["Energy"][region]
        )

    result = {}
    agg_map = {"Energy": np.sum, "Total_Emissions": np.sum, "Intensity_Index": np.mean}
    if by == "interval":
        for metric in data.columns.levels[0]:
            result[metric] = data[metric]

    elif by == "hour":
        for metric in data.columns.levels[0]:
            result[metric] = (
                data[metric]
                .groupby(
                    by=[
                        data.index.year,
                        data.index.month,
                        data.index.day,
                        data.index.hour,
                    ]
                )
                .agg(agg_map[metric])
            )
            result[metric] = result[metric].round(2)
            result[metric]["reconstr_time"] = None
            for row in result[metric].index:
                result[metric].loc[row, "reconstr_time"] = datetime(
                    year=row[0], month=row[1], day=row[2], hour=row[3]
                )
            result[metric].set_index("reconstr_time", inplace=True)
            result[metric].index.name = "Time"
    elif by == "day":
        for metric in data.columns.levels[0]:
            result[metric] = (
                data[metric]
                .groupby(
                    by=[
                        data.index.year,
                        data.index.month,
                        data.index.day,
                    ]
                )
                .agg(agg_map[metric])
            )
            result[metric] = result[metric].round(2)
            result[metric]["reconstr_time"] = None
            for row in result[metric].index:
                result[metric].loc[row, "reconstr_time"] = datetime(
                    year=row[0], month=row[1], day=row[2]
                )
            result[metric].set_index("reconstr_time", inplace=True)
            result[metric].index.name = "Time"

    elif by == "month":
        for metric in data.columns.levels[0]:
            result[metric] = (
                data[metric]
                .groupby(
                    by=[
                        data.index.year,
                        data.index.month,
                    ]
                )
                .agg(agg_map[metric])
            )
            result[metric] = result[metric].round(2)
            result[metric]["reconstr_time"] = None
            for row in result[metric].index:
                result[metric].loc[row, "reconstr_time"] = datetime(
                    year=row[0], month=row[1], day=1
                )
            result[metric].set_index("reconstr_time", inplace=True)
            result[metric].index.name = "Time"

    elif by == "year":
        for metric in data.columns.levels[0]:
            result[metric] = (
                data[metric].groupby(by=[data.index.year]).agg(agg_map[metric])
            )
            print("UNTESTED for request spanning multiple years")
            result[metric] = result[metric].round(2)
            result[metric]["reconstr_time"] = None
            for row in result[metric].index:
                result[metric].loc[row, "reconstr_time"] = datetime(
                    year=row, month=1, day=1
                )
            result[metric].set_index("reconstr_time", inplace=True)
            result[metric].index.name = "Time"

    else:
        raise Exception(
            "Error: invalid by argument. Must be one of [interval, hour, day, month, year]"
        )

    return result
