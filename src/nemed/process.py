""" Process functions for calculations based on downloaded data """
from datetime import datetime
import pandas as pd
import numpy as np
import os
from .downloader import download_cdeii_table, download_unit_dispatch, download_pricesetters, download_generators_info, \
    download_duid_auxload
import nemed.helper_functions.helpers as hp

DISP_INT_LENGTH = 5 / 60


def get_total_emissions_by_DI_DUID(start_time, end_time, cache, filter_units=None, filter_regions=None,
                                   generation_sent_out=True):
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
    disp_df = download_unit_dispatch(
        start_time, end_time, cache, filter_units, record="INITIALMW"
    )
    geninfo_df = download_generators_info(cache)

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
    result["Energy"] = result["Dispatch"] * DISP_INT_LENGTH

    # Consider auxillary loads for Sent Out Generation metric
    if generation_sent_out:
        # Download Auxilary Load Data
        auxload = download_duid_auxload()

        # Merge data and compute auxilary load factor
        result = result.merge(auxload, on=["DUID"], how="left")
        result['pct_sent_out'] = (100 - result['Auxiliary Load (%)']) / 100
        result['pct_sent_out'].fillna(1.0, inplace=True)

        # Adjust Energy for auxilary load
        result["Energy"] = result["Energy"] * result['pct_sent_out']

    # Compute emissions
    result["Total_Emissions"] = result["Energy"] * result["CO2E_EMISSIONS_FACTOR"]
    result.rename(columns={"CO2E_EMISSIONS_FACTOR": "Plant_Emissions_Intensity"}, inplace=True)

    return result[['Time', 'DUID', 'REGIONID', 'Plant_Emissions_Intensity', 'Energy', 'Total_Emissions']]


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


def filter_marginal_table(table, region, condense_same_tech=True):

    region_summary = table[table['RegionID'] == region]
    region_summary = region_summary.drop_duplicates(subset=['PeriodID','CO2E_EMISSIONS_FACTOR','tech_name'])
    region_summary = region_summary[['PeriodID','RegionID','Dispatch Type','tech_name','CO2E_EMISSIONS_FACTOR',\
        'Date','Hour']]

    return None






def aggregate_marginal_data_by(data, by, maintain_dates=True, agg='sum'):

    # Check timestamp in index col, throw error if not
    if (not isinstance(data.index.values[0], (datetime, pd.Timestamp))) and (data.index.name != 'PeriodID'):
        raise ValueError("`data` parsed must have datetime type as index with name 'PeriodID'")

    # Check by value
    if not by in ['interval','halfhour','hour','day']:
        raise ValueError("`by` argument must be one of ['interval', 'halfhour', 'hour', 'day']")

    # Check maintain dates
    if not isinstance(maintain_dates, bool):
        raise TypeError("`maintain_dates` argument must be a logical 'True' or 'False'")

    # Aggregation options
    agg_select = {'sum': np.sum, 'mean': np.mean, 'max': np.max, 'min': np.min}
    if not agg in agg_select.keys():
        raise ValueError("`agg` argument must be one of ['sum', 'mean', 'max', 'min']")

    # Process...
    if maintain_dates:
        # For each region
        all_regions = None
        for region in data['RegionID'].unique():
            sel_data = data[data['RegionID'] == region]
            # By alternatives...
            if by == "interval":
                print("WARNING: Result will appear unchanged since no aggregation occurs with maintain_dates = True"\
                    + " and by = interval")
                result = sel_data.loc[:,'CO2E_EMISSIONS_FACTOR']
            if by == "halfhour":
                result = sel_data[['CO2E_EMISSIONS_FACTOR']].resample('30min', closed='right', label='right')\
                    .agg(agg_select[agg])
            elif by == "hour":
                result = sel_data[['CO2E_EMISSIONS_FACTOR']].resample('H', closed='right').agg(agg_select[agg])
            elif by == "day":
                result = sel_data[['CO2E_EMISSIONS_FACTOR']].resample('D', closed='right').agg(agg_select[agg])

            if isinstance(result, pd.Series):
                result = pd.DataFrame(result)
            result.insert(0,'RegionID', region)
            all_regions = pd.concat([all_regions, result])

        all_regions.reset_index(inplace=True)
        all_regions.columns = ['Time', 'RegionID', agg[0].upper()+agg[1:]+"_Marginal_Emissions"]

    else:
        # For each region
        all_regions = None
        for region in data['RegionID'].unique():
            sel_data = data[data['RegionID'] == region]
            idx_slot = 0

            # IMPLEMENT AGGREGATION purely on time not dates
            if (by == "interval") or (by == "halfhour") or (by == "hour"):
                # Set index dummy year-month-day
                sel_data.index = pd.DatetimeIndex([datetime(year=1800, month=1, day=1, hour=sel_data.index[i].hour, \
                    minute=sel_data.index[i].minute) for i in range(len(sel_data.index))], name='PeriodID')

                # By alternatives
                if by == "interval":
                    result = sel_data.reset_index()
                    result = result[['PeriodID', 'CO2E_EMISSIONS_FACTOR']].groupby(by='PeriodID').agg(agg_select[agg])
                elif by == "halfhour":
                    print(sel_data)
                    result = sel_data[['CO2E_EMISSIONS_FACTOR']].resample('30min', closed='right', origin='end', label='right')\
                        .agg(agg_select[agg])
                elif by == "hour":

                    result = sel_data[['CO2E_EMISSIONS_FACTOR']].resample('H', closed='right', label='right').agg(agg_select[agg])
            

                # Clean time (index) column

            elif (by == "day"):
                sel_data.index = pd.DatetimeIndex([datetime(year=1800, month=1, day=sel_data.index[i].day, hour=sel_data.index[i].hour, \
                    minute=sel_data.index[i].minute) for i in range(len(sel_data.index))], name='PeriodID')

                # sel_data.index = [datetime(year=1000, month=1, day=sel_data.index[i].day, hour=sel_data.index[i].hour,\
                #     minute=sel_data.index[i].minute) for i in range(len(sel_data.index))]
                result = sel_data[['CO2E_EMISSIONS_FACTOR']].resample('D', closed='right').agg(agg_select[agg])

                result.insert(idx_slot, 'Day', [result.index[i].day for i in range(len(result.index))])
                idx_slot += 1

                # mIndex = pd.MultiIndex.from_tuples([(result.index[i].day, result.index[i].hour, \
                #     result.index[i].minute) for i in range(len(result.index))], names=['Day','Hour','Minute'])
                # result.set_index(mIndex, inplace=True)

            # Create hour/min columns
            result.insert(idx_slot, 'Hour', [result.index[i].hour for i in range(len(result.index))])
            idx_slot += 1
            result.insert(idx_slot, 'Minute', [result.index[i].minute for i in range(len(result.index))])
            idx_slot += 1
            result.index = [datetime.strftime(result.reset_index()['PeriodID'][i], "%H:%M") for i in range(len(result.index))]

            result.insert(idx_slot,'RegionID', region)
            result = result.rename(columns = {'CO2E_EMISSIONS_FACTOR': agg[0].upper()+agg[1:]+"_Marginal_Emissions"})
            all_regions = pd.concat([all_regions, result])

    return all_regions

def aggregate_data_by(data, by):
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
