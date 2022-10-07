""" Process functions for calculations based on downloaded data """
from datetime import datetime
import pandas as pd
import numpy as np
from .downloader import download_cdeii_table, download_unit_dispatch, download_pricesetters, download_generators_info, \
    download_duid_auxload
import nemed.helper_functions.helpers as hp

DISP_INT_LENGTH = 5


def get_total_emissions_by_DI_DUID(start_time, end_time, cache, filter_regions=None,
                                   generation_sent_out=True, assume_ramp=True):
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
    hp._check_cache(cache)

    # Download CDEII, Unit Dispatch Data and Generation Information
    cdeii_df = download_cdeii_table()
    cdeii_df = cdeii_df.drop_duplicates(subset=['DUID'])

    disp_df = download_unit_dispatch(start_time, end_time, cache, source_initialmw=False, source_scada=True,
                                     return_all=False, check=False, overwrite="scada")
    geninfo_df = download_generators_info(cache)

    # Filter units and replace negatives
    disp_df = disp_df[disp_df['DUID'].isin(cdeii_df['DUID'])]
    disp_df['Dispatch'] = np.where(disp_df['Dispatch'] < 0, 0, disp_df['Dispatch'])

    # Merge unit generation and dispatch type category
    result = pd.merge(left=disp_df, right=geninfo_df[['DUID', 'Dispatch Type']], on="DUID", how="left")

    # Filter out loads
    result = result[result['Dispatch Type'] == 'Generator']

    # Merge unit generation and emissions factor data
    result = result.merge(right=cdeii_df[["DUID", "REGIONID", "CO2E_EMISSIONS_FACTOR"]], on="DUID", how="left")

    # Filter by region is specified
    if filter_regions:
        if not pd.Series(cdeii_df["REGIONID"].unique()).isin(filter_regions).any():
            raise ValueError("filter_region paramaters passed were not found in NEM regions")
        result = result[result["REGIONID"].isin(filter_regions)]
    if result.empty:
        print("WARNING: Emissions Dataframe is empty. Check filter_units and filter_regions parameters do not \
            conflict!")

    # Calculate Energy (MWh)
    if not assume_ramp:
        result["Energy"] = result["Dispatch"] * (DISP_INT_LENGTH / 60)
        aggregate = result
    else:
        aggregate = pd.DataFrame()
        for duid in result['DUID'].unique():
            sub_df = result[result['DUID'] == duid]
            sub_df = sub_df.sort_values('Time')
            sub_df.reset_index(drop=True, inplace=True)
            sub_df['Dispatch_prev'] = np.nan
            sub_df.loc[1:, 'Dispatch_prev'] = sub_df['Dispatch'][:len(sub_df)-1].to_list()

            sub_df.loc[:, 'Energy'] = (0.5*(sub_df['Dispatch'] - sub_df['Dispatch_prev']) + sub_df['Dispatch_prev']) \
                * (DISP_INT_LENGTH / 60)

            aggregate = pd.concat([aggregate, sub_df], ignore_index=True)

    # Consider auxillary loads for Sent Out Generation metric
    if generation_sent_out:
        # Download Auxilary Load Data
        auxload = download_duid_auxload()

        # Merge data and compute auxilary load factor
        aggregate = aggregate.merge(auxload, on=["DUID"], how="left")
        aggregate['pct_sent_out'] = (100 - aggregate['Auxiliary Load (%)']) / 100
        aggregate['pct_sent_out'].fillna(1.0, inplace=True)

        # Adjust Energy for auxilary load
        aggregate["Energy"] = aggregate["Energy"] * aggregate['pct_sent_out']

    # Compute emissions
    aggregate["Total_Emissions"] = aggregate["Energy"] * aggregate["CO2E_EMISSIONS_FACTOR"]
    aggregate.rename(columns={"CO2E_EMISSIONS_FACTOR": "Plant_Emissions_Intensity"}, inplace=True)

    return aggregate[['Time', 'DUID', 'REGIONID', 'Plant_Emissions_Intensity', 'Energy', 'Total_Emissions']]


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
        The resulting marginal emitter dataset with interval timestamps, DUID, emissions factor and generation
        information
    """
    # Check if cache is an existing directory
    hp._check_cache(cache)

    # Download CDEII, Price Setter Files and Generation Information
    gen_info = download_generators_info(cache)
    emissions_factors = download_cdeii_table()
    price_setters = download_pricesetters(cache, start_year, start_month, start_day, end_year, end_month, end_day,
                                          redownload_xml)

    # Merge generation info to price setter file
    calc_emissions_df = price_setters[["PeriodID", "RegionID", "DUID"]].merge(
        gen_info[["Dispatch Type", "Fuel Source - Primary", "Fuel Source - Descriptor", "Technology Type - Primary",
                  "Technology Type - Descriptor", "DUID"]],
        how="left", on="DUID", sort=False)

    # Merge emissions factors
    calc_emissions_df = calc_emissions_df.merge(emissions_factors[["DUID", "CO2E_EMISSIONS_FACTOR"]], how="left",
                                                on="DUID")

    # Simplify naming conventions
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

    return calc_emissions_df.reset_index()


def tech_rename(fuel, tech_descriptor, dispatch_type):
    """Name technology type based on fuel, and descriptions of AEMO

    Parameters
    ----------
    fuel : str
        Fuel Source - Descriptor field of a specific DUID
    tech_descriptor : str
        Technology Type - Descriptor field of a specific DUID
    dispatch_type : str
        Dispatch Type field of a specific DUID

    Returns
    -------
    str
        Returns a simplified name for the above arguments
    """

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


def aggregate_data_by(data, by):
    """Existing function to aggregate data by time-resolution specified

    Parameters
    ----------
    data : pd.DataFrame
        Input data to aggregate
    by : str
        Time resolution to aggregate to; e.g. ['hour', 'day', 'month']

    Returns
    -------
    pd.DataFrame
        Resulting time resolved data

    Raises
    ------
    Exception
        Invalid by argument
    """
    result = {}
    agg_map = {"Energy": np.sum, "Total_Emissions": np.sum, "Intensity_Index": np.mean}
    # Format result to show on interval resolution
    if by == "interval":
        for metric in data.columns.levels[0]:
            result[metric] = data[metric].round(2)

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
