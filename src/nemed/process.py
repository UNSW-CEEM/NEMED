""" Process functions for calculations based on downloaded data """
from datetime import datetime
import pandas as pd
import numpy as np
import os
from .downloader import download_cdeii_table, download_unit_dispatch, download_pricesetters, download_generators_info, \
    download_duid_auxload

DISP_INT_LENGTH = 5 / 60


def get_total_emissions_by_DI_DUID(
    start_time, end_time, cache, filter_units=None, filter_regions=None, generation_sent_out=True, save_debug_file=False
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
    filter_units : list of str
        List of DUIDs to filter data by, by default None

    Returns
    -------
    pd.DataFrame
        Calculated data table containing columns=["Time","DUID","Plant_Emissions_Intensity","Energy",
        "Total_Emissions"]. Plant_Emissions_Intensity is a static metric in tCO2-e/MWh, Energy is in MWh, Total
        Emissions in tCO2-e.
    """
    # Check if cache is an existing directory
    if not os.path.isdir(cache):
        print("Creating new cache in current directory.")
        os.mkdir("CACHE")
        cache = os.path.join(os.getcwd(), "CACHE")

    # Download CDEII and Unit Dispatch Data
    cdeii_df = download_cdeii_table()
    disp_df = download_unit_dispatch(
        start_time, end_time, cache, filter_units, record="INITIALMW"
    )

    # Merge unit generation and emissions factor data
    result = pd.merge(
        disp_df,
        cdeii_df[["DUID", "REGIONID", "CO2E_EMISSIONS_FACTOR"]],
        how="left",
        on="DUID",
    )

    # Filter by region is specified
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

    if generation_sent_out:
        # Use 'sent_out' generation metrics
        # Download Auxillary Load Data
        auxload = download_duid_auxload()
        result = result.merge(auxload, on=["DUID"], how="left")
        result['pct_sent_out'] = (100 - result['Auxiliary Load (%)']) / 100
        result['pct_sent_out'].fillna(1.0, inplace=True)
        result["Energy"] = result["Energy"] * result['pct_sent_out']

    result["Total_Emissions"] = result["Energy"] * result["CO2E_EMISSIONS_FACTOR"]
    result.rename(
            columns={"CO2E_EMISSIONS_FACTOR": "Plant_Emissions_Intensity"}, inplace=True
        )

    # Remove duplicates if still existing
    result.drop_duplicates(subset=["Time", "DUID"], inplace=True)

    if save_debug_file:
        result.to_csv('totalemissionsdebug.csv')

    return result[['Time', 'DUID', 'REGIONID', 'Plant_Emissions_Intensity', 'Energy', 'Total_Emissions']]


def get_total_emissions_by_(start_time, end_time, cache, filter_regions, by="interval", 
                            generation_sent_out=True, save_debug_file=False):
    # Check if cache folder exists
    if not os.path.isdir(cache):
        print("Creating new cache in current directory.")
        os.mkdir("CACHE")
        cache = os.path.join(os.getcwd(), "CACHE")

    # Get emissions for all units by dispatch interval
    raw_table = get_total_emissions_by_DI_DUID(
        start_time, end_time, cache, filter_units=None, filter_regions=filter_regions,
        generation_sent_out=generation_sent_out,
        save_debug_file=save_debug_file
    )

    # Pivot and summate data. Aggregates to a regional level on interval
    data = raw_table.pivot_table(
        index="Time",
        columns="REGIONID",
        values=["Energy", "Total_Emissions"],
        aggfunc="sum",
    )

    # Compute Emissions Intensity Index from total emissions divided by total energy
    for region in data.columns.levels[1]:
        data[("Intensity_Index", region)] = (
            data["Total_Emissions"][region] / data["Energy"][region]
        )

    result = {}
    agg_map = {"Energy": np.sum, "Total_Emissions": np.sum, "Intensity_Index": np.mean}
    # Format result to show on interval resolution
    if by == "interval":
        for metric in data.columns.levels[0]:
            result[metric] = data[metric].round(2)
            result[metric] = result[metric][:-1]

    # Format result to show on hourly resolution
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
            result[metric] = result[metric][:-1]

    # Format result to show on daily resolution
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
            result[metric] = result[metric][:-1]

    # Format result to show on monthly resolution
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
            result[metric] = result[metric][:-1]

    # Format result to show on annual resolution
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
            result[metric] = result[metric][:-1]

    else:
        raise Exception(
            "Error: invalid by argument. Must be one of [interval, hour, day, month, year]"
        )

    return result


def get_marginal_emitter(cache, start_year, start_month, start_day, end_year, end_month, end_day, redownload_xml=True):
    """Retrieves the marginal emitter (DUID) and Technology Type information for dispatch intervals in the given date
    range. This information is a combination of AEMO price setter files, generation information and CDEII information.

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
        _description_
    """
    if not os.path.isdir(cache):
        print("Creating new cache in current directory.")
        os.mkdir("CACHE")
        cache = os.path.join(os.getcwd(), "CACHE")

    gen_info = download_generators_info(cache)
    emissions_factors = download_cdeii_table()
    price_setters = download_pricesetters(cache, start_year, start_month, start_day, end_year, end_month, end_day,
                                          redownload_xml)

    calc_emissions_df = price_setters[["PeriodID", "RegionID", "DUID"]].merge(
        gen_info[[
                "Dispatch Type", "Fuel Source - Primary", "Fuel Source - Descriptor", "Technology Type - Primary",
                "Technology Type - Descriptor", "DUID"
        ]],
        how="left",
        on="DUID",
        sort=False,
    )
    calc_emissions_df = calc_emissions_df.merge(
        emissions_factors[["DUID", "CO2E_EMISSIONS_FACTOR"]], how="left", on="DUID"
    )
    calc_emissions_df["tech_name"] = calc_emissions_df.apply(
        lambda x: tech_rename(
            x["Fuel Source - Descriptor"],
            x["Technology Type - Descriptor"],
            x["Dispatch Type"],
        ),
        axis=1,
    )

    calc_emissions_df["PeriodID"] = pd.to_datetime(calc_emissions_df["PeriodID"])
    calc_emissions_df["PeriodID"] = calc_emissions_df["PeriodID"].apply(lambda x: x.replace(tzinfo=None))
    calc_emissions_df.set_index("PeriodID", inplace=True)

    calc_emissions_df["Date"] = calc_emissions_df.index.date
    calc_emissions_df["Hour"] = calc_emissions_df.index.hour
    calc_emissions_df["Season"] = calc_emissions_df.index.month % 12 // 3 + 1
    calc_emissions_df["Season"].replace(1, "Summer", inplace=True)
    calc_emissions_df["Season"].replace(2, "Autumn", inplace=True)
    calc_emissions_df["Season"].replace(3, "Winter", inplace=True)
    calc_emissions_df["Season"].replace(4, "Spring", inplace=True)

    return calc_emissions_df


def tech_rename(fuel, tech_descriptor, dispatch_type):
    """Name technology type based on fuel, and descriptions of AEMO"""

    name = fuel
    if fuel in ['Solar', 'Wind', 'Black Coal', 'Brown Coal']:
        pass
    elif tech_descriptor == 'Battery':
        if dispatch_type == 'Load':
            name = 'Battery Charge'
        else:
            name = 'Battery Discharge'
    elif tech_descriptor in ['Hydro - Gravity', 'Run of River']:
        name = tech_descriptor
    elif tech_descriptor == 'Pump Storage':
        if dispatch_type == 'Load':
            name = 'Pump Storage Charge'
        else:
            name = 'Pump Storage Discharge'
    elif tech_descriptor == '-' and fuel == '-' and dispatch_type == 'Load':
        name = 'Pump Storage Charge'
    elif tech_descriptor == 'Open Cycle Gas turbines (OCGT)':
        name = 'OCGT'
    elif tech_descriptor == 'Combined Cycle Gas Turbine (CCGT)':
        name = 'CCGT'
    elif fuel in ['Natural Gas / Fuel Oil', 'Natural Gas'] and tech_descriptor == 'Steam Sub-Critical':
        name = 'Gas Thermal'
    elif isinstance(tech_descriptor, str) and 'Engine' in tech_descriptor:
        name = 'Reciprocating Engine'
    return name
