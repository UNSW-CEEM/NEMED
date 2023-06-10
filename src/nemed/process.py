""" Process functions for calculations based on downloaded data """
from datetime import datetime as dt, timedelta
import pandas as pd
import numpy as np
import logging
import os
from .downloader import download_cdeii_table, download_unit_dispatch, download_pricesetter_files, download_generators_info, \
    download_duid_auxload, download_plant_emissions_factors, read_plant_auxload_csv, download_genset_map, download_dudetailsummary
from .helper_functions import helpers as hp
from .defaults import CO2E_DATA_SOURCE_YEARMAP

DISP_INT_LENGTH = 5
logger = logging.getLogger(__name__)


def get_total_emissions_by_DI_DUID(start_time, end_time, cache, filter_regions=None, generation_sent_out=True, \
                                   assume_energy_ramp=True, dropna_co2factors=True, return_all=False):
    """Retrieve the total emissions for each generation unit per dispatch interval.

    Parameters
    ----------
    start_time : str
        Start Time Period in format 'yyyy/mm/dd HH:MM'
    end_time : str
        End Time Period in format 'yyyy/mm/dd HH:MM'
    cache : str
        Raw data location in local directory
    filter_regions : list(str)
        NEM regions to filter for while retrieving the data, as a list, by default None to collect all region data
    generation_sent_out : bool
        Considers 'sent_out' generation (auxilary loads) as opposed to 'as generated' in calculations, by default True
    assume_energy_ramp : bool
        Uses a linear ramp between dispatch scada points as opposed to a stepped function, by default True
    dropna_co2factors : bool
        Removes data (generation) entries which do not have a CO2E_EMISSIONS_FACTOR mapped to them, by default True
    return_all : bool
        Returns the entire table will all columns as opposed to tidied up table, by default False

    Returns
    -------
    pandas.DataFrame
        Data is returned as formatted if `return_all` = False, `generation_sent_out` = True:

        =========================  ========  ===================================================================================================================
        Columns:                   Type:     Description:
        DUID                       str       Generator Identifier.
        Time                       datetime  Timestamp for end of interval.
        Region                     str       The NEM region corresponding to data.
        Plant_Emissions_Intensity  float     The CO2_EMISSIONS_FACTOR [tCO2-e/MWh] corresponding to DUID.
        Energy                     float     The energy [MWh] (as generated) calculated as step or ramp depending on `assume_energy_ramp`.
        PCT_AUXILIARY_LOAD         int       The percentage of auxiliary load corresponding to DUID.
        Energy_SO                  float     The energy [MWh] (sent out) calculated based on Energy and PCT_AUXILIARY_LOAD
        Total_Emissions            float     The emissions [tCO2-e] for the DUID and Time based on Energy_SO and Plant_Emissions_Intensity
        =========================  ========  ===================================================================================================================

    """
    # Check if cache is an existing directory
    hp._check_cache(cache)

    # Adjust to also collect prior DI to calculate energy ramp 
    actual_stime = dt.strptime(start_time, "%Y/%m/%d %H:%M")
    actual_etime = dt.strptime(end_time, "%Y/%m/%d %H:%M")
    if actual_etime < actual_stime:
        raise Exception("end_time cannot be prior start_time")

    prior_start_time = actual_stime - timedelta(minutes=DISP_INT_LENGTH)
    prior_start_time = dt.strftime(prior_start_time, "%Y/%m/%d %H:%M")

    # Segment emissions calculations into smaller chunks
    ts = _generate_timeseries_loop(prior_start_time, end_time)
    res_str = []
    for sdate, edate, st, et in zip(ts['start'], ts['end'], ts['s_str'], ts['e_str']):
        logger.info(f"Processing total emissions from {st} to {et}")
        df = _total_emissions_process(sdate, edate, cache, filter_regions, generation_sent_out, assume_energy_ramp, \
                                      dropna_co2factors)
        name = 'processed_co2_total_{}_{}.parquet'.format(st, et)
        res_str += [name]
        df.to_parquet(os.path.join(cache, name))

    # Load cached results files
    results_df = []
    for name in res_str:
        logger.info(f"Loading results file {name}")
        results_df += [pd.read_parquet(os.path.join(cache, name))]
    
    flatten = pd.concat(results_df, ignore_index=True)
    res = flatten[flatten['Time'].between(start_time, end_time, inclusive="right")]

    logger.info('Completed get_total_emissions_by_DI_DUID')

    if return_all:
        return res
    else:
        if generation_sent_out:
            return res[['DUID', 'Time', 'Region', 'Plant_Emissions_Intensity', 'Energy', 'PCT_AUXILIARY_LOAD', \
                        'Energy_SO', 'Total_Emissions']]
        else:
            return res[['DUID', 'Time', 'Region', 'Plant_Emissions_Intensity', 'Energy', 'Total_Emissions']]


def _generate_timeseries_loop(actual_start, actual_end):
    """Generates a dict of start and end times for looping through the `_total_emissions_process` computation.
    """
    stime = dt.strptime(actual_start, "%Y/%m/%d %H:%M")
    etime = dt.strptime(actual_end, "%Y/%m/%d %H:%M")
    start_series = pd.date_range(stime, etime, freq='MS', normalize=True, inclusive="neither")
    end_series = pd.date_range(stime, etime, freq='MS', normalize=True, inclusive="neither")
    time_segments = {
        'start': [actual_start[0:10]+" 00:00"] + start_series.strftime("%Y/%m/%d %H:%M").to_list(),
        'end': end_series.strftime("%Y/%m/%d %H:%M").to_list() + [actual_end],
        's_str': [stime.strftime("%Y-%m-%d")] + start_series.strftime("%Y-%m-%d").to_list(),
        'e_str': end_series.strftime("%Y-%m-%d").to_list() + [etime.strftime("%Y-%m-%d")]
    }
    return time_segments


def _total_emissions_process(start_time, end_time, cache, filter_regions=None,
                             generation_sent_out=True, assume_energy_ramp=True, dropna_co2factors=True):
    """Process for calculating total emissions based on the parameters defined in `get_total_emissions_by_DI_DUID`.
    """
    # Download Unit Dispatch Data and Generation Information
    disp_df = download_unit_dispatch(start_time, end_time, cache, source_initialmw=False, source_scada=True,
                                     return_all=False, check=False, overwrite="scada", rm_negative=True)

    geninfo_df = download_dudetailsummary(cache)

    # Merge geninfo and filter out loads
    disp_df.set_index('DUID', inplace=True)
    geninfo_df.set_index('DUID', inplace=True)
    filt_df = disp_df.join(geninfo_df[['REGIONID', 'DISPATCHTYPE']], how="left")
    filt_df.reset_index(inplace=True)
    
    filt_df = filt_df[filt_df['DISPATCHTYPE'] == 'GENERATOR']

    # Filter by region if specified
    if filter_regions:
        if not pd.Series(filt_df["REGIONID"].unique()).isin(filter_regions).any():
            raise ValueError("filter_region paramaters passed were not found in NEM regions")
        filt_df = filt_df[filt_df["REGIONID"].isin(filter_regions)]

    # Merge Energy data with Plant Emissions Factors
    filt_df['year'], filt_df['month'] = filt_df['Time'].dt.year, filt_df['Time'].dt.month
    co2factors_df = _get_duid_emissions_intensities(start_time, end_time, cache)
    plt_df = pd.merge(left=filt_df,
                      right=co2factors_df[["file_year", "file_month", "DUID", "CO2E_EMISSIONS_FACTOR"]],
                      left_on=["year", "month", "DUID"],
                      right_on=["file_year", "file_month", "DUID"],
                      how="left")

    # Filter out Data with Null CO2_EMISSIONS_FACTORS
    if dropna_co2factors:
        plt_df = plt_df[~plt_df['CO2E_EMISSIONS_FACTOR'].isna()]

    # Calculate Energy (MWh)
    if not assume_energy_ramp:
        plt_df["Energy"] = plt_df["Dispatch"] * (DISP_INT_LENGTH / 60)
        result = plt_df
    else:
        result = _calculate_energy_ramp(plt_df)
        
    # Calculate Sent-Out Energy (MWh)
    if generation_sent_out:
        result = _calculate_sent_out(result)
        # Compute emissions
        result["Total_Emissions"] = result["Energy_SO"] * result["CO2E_EMISSIONS_FACTOR"]
    else:
        result["Total_Emissions"] = result["Energy"] * result["CO2E_EMISSIONS_FACTOR"]

    result.rename(columns={"CO2E_EMISSIONS_FACTOR": "Plant_Emissions_Intensity",
                           "REGIONID": "Region"}, inplace=True)
    return result


def _get_duid_emissions_intensities(start_time, end_time, cache):
    """Merges emissions factors from GENSETID to DUID and cleans data"""
    co2factors_df = download_plant_emissions_factors(start_time, end_time, cache)
    genset_map = download_genset_map(cache)
    co2factors_df = co2factors_df.merge(right=genset_map[["GENSETID", "DUID"]],
                                        on=["GENSETID"],
                                        how="left")

    # Filter out older assumptions, where duplicate CO2 factors exist for data entry
    co2factors_df['CO2E_DATA_YEAR'] = co2factors_df['CO2E_DATA_SOURCE'].map(CO2E_DATA_SOURCE_YEARMAP)
    co2factors_df = co2factors_df.sort_values(['CO2E_DATA_YEAR'], ascending=True)
    co2factors_df = co2factors_df.drop_duplicates(['file_year', 'file_month', 'CO2E_ENERGY_SOURCE', 'DUID'], keep='last')

    # Find and correct duplicate entries for DUIDs
    co2factors_df = _condense_genset_co2_differences(co2factors_df)

    # Ammend Hydro NaN values to zero
    co2factors_df.loc[(co2factors_df['CO2E_EMISSIONS_FACTOR'].isna()) & \
                    (co2factors_df['CO2E_ENERGY_SOURCE']=='Hydro'), 'CO2E_EMISSIONS_FACTOR'] = 0.0

    return co2factors_df[['file_year', 'file_month', 'DUID', 'CO2E_EMISSIONS_FACTOR', \
                          'CO2E_ENERGY_SOURCE', 'CO2E_DATA_SOURCE']]


def _condense_genset_co2_differences(all_df):
    """Patch duplicate or differing co2 factors for the same year-month-DUID."""
    for duid in all_df[all_df.duplicated(['file_year','file_month','DUID'])]['DUID'].unique():
        correction = all_df[all_df['DUID']==duid].dropna(subset=['CO2E_EMISSIONS_FACTOR', 'CO2E_ENERGY_SOURCE', \
                                                                 'CO2E_DATA_SOURCE'])

        # If year-month-gensetid is duplicated in correction df, average vals and drop correction
        if correction.duplicated(subset=['file_year','file_month','DUID']).any():
            result = []
            for yr in correction['file_year'].unique():
                for mn in correction[correction['file_year'] == yr]['file_month'].unique():
                    subset = correction[(correction['file_year'] == yr) & (correction['file_month'] == mn)]
                    descriptor = [' / '.join(subset['CO2E_ENERGY_SOURCE'].to_list()) \
                        if len(subset['CO2E_DATA_SOURCE'].unique())==1 else subset['CO2E_ENERGY_SOURCE'].iloc[0]][0]
                    emissions = subset['CO2E_EMISSIONS_FACTOR'].mean()
                    subset = subset.drop_duplicates(['file_year','file_month','DUID'], keep='first')
                    subset['CO2E_ENERGY_SOURCE'] = descriptor
                    subset['CO2E_EMISSIONS_FACTOR'] = emissions
                    result += [subset]
            result = pd.concat(result,ignore_index=True)
        else:
            result = correction
        
        # Remove duid entry in main df
        all_df = all_df[~all_df['DUID'].isin([duid])]

        # Add resultant duid entry to main df
        all_df = pd.concat([all_df, result], ignore_index=True)
    return all_df.drop(['GENSETID'], axis=1).reset_index(drop=True)


def _calculate_energy_ramp(dispatch_df):
    """Returns dataframe with energy calculated as ramp between dispatch scada points.
    """
    logger.info('Compiling Energy from Dispatch')
    aggregate = []
    for duid in dispatch_df['DUID'].unique():
        sub_df = dispatch_df[dispatch_df['DUID'] == duid]
        sub_df = sub_df.sort_values('Time')
        sub_df.reset_index(drop=True, inplace=True)
        sub_df['Dispatch_prev'] = np.nan
        sub_df.loc[1:, 'Dispatch_prev'] = sub_df['Dispatch'][:len(sub_df)-1].to_list()
        sub_df.loc[:, 'Energy'] = (0.5*(sub_df['Dispatch'] - sub_df['Dispatch_prev']) + sub_df['Dispatch_prev']) \
            * (DISP_INT_LENGTH / 60)
        aggregate += [sub_df]
    aggregate = pd.concat(aggregate, ignore_index=True)
    return aggregate.reset_index(drop=True)


def _calculate_sent_out(energy_df):
    """Returns dataframe with sent-out generation calculated by considering auxload factor for corresponding DUID.
    """
    logger.info('Compiling Sent Out Generation')
    auxload = read_plant_auxload_csv()

    # Merge data and compute auxilary load factor
    so_df = energy_df.merge(auxload, on=["DUID"], how="left")
    so_df['pct_sent_out'] = (100 - so_df["PCT_AUXILIARY_LOAD"]) / 100
    """add error checking measure for % of no matches in auxload"""
    so_df['pct_sent_out'].fillna(1.0, inplace=True)
    so_df["Energy_SO"] = so_df["Energy"] * so_df['pct_sent_out']
    return so_df


def get_marginal_emitter(start_time, end_time, cache):
    """Retrieves the marginal emissions intensity for each dispatch interval and region. This factor being the weighted
    sum of the generators contributing to price-setting. Although not necessarily common, there may be times where
    multiple technology types contribute to the marginal emissions - note however that the 'DUID' and 'CO2E_ENERGY_SOURCE'
    returned will reflect only the plant which makes the greatest contribution towards price-setting.

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
        Data is returned as:

        ==================  ========  ==================================================================================================
        Columns:            Type:     Description:
        Time                datetime  Timestamp reported as end of dispatch interval.
        Region              str       The NEM region corresponding to the marginal emitter data. 
        Intensity_Index     float     The intensity index [tCO2e/MWh] (as by weighted contributions) of the price-setting generators.
        DUID                str       Unit identifier of the generator with the largest contribution on the margin for that Time-Region.
        CO2E_ENERGY_SOURCE  str       Unit energy source with the largest contribution on the margin for that Time-Region.
        ==================  ========  ==================================================================================================

    """
    # Check if cache is an existing directory
    hp._check_cache(cache)

    # Download CDEII, Price Setter Files and Generation Information
    ## gen_info = download_generators_info(cache)
    logger.warning('Warning: Gen_info table only has most recent NEM registration and exemption list. Does not account for retired generators')
    co2_factors = _get_duid_emissions_intensities(start_time, end_time, cache)
    price_setters = download_pricesetter_files(start_time, end_time, cache)

    # Drop Basslink
    filt_df = price_setters[~price_setters['Unit'].str.contains('T-V-MNSP1')]
    filt_df = filt_df.rename(columns={'Unit': 'DUID', 'PeriodID': 'Time', 'RegionID': 'Region'})
    filt_df['file_year'], filt_df['file_month'] = filt_df['Time'].dt.year, filt_df['Time'].dt.month

    # Merge CO2 Factors
    filt_df = pd.merge(filt_df[["Time", "Region", "DUID", "Increase", "file_year", "file_month"]],
        co2_factors[["DUID", "file_year", "file_month", "CO2E_EMISSIONS_FACTOR", "CO2E_ENERGY_SOURCE"]],
        how="left", on=["DUID", "file_year", "file_month"], sort=False)

    filt_df.drop(['file_year', 'file_month'], axis=1, inplace=True)

    # Weigh CO2 intensity by 'Increase' contributions
    filt_df['weighted_co2_factor'] = filt_df['Increase'] * filt_df['CO2E_EMISSIONS_FACTOR']

    # Aggregate sum of weighted CO2 intensities
    values = filt_df.groupby(by=['Time','Region'], axis=0).sum()
    values = values.reset_index()[['Time','Region','weighted_co2_factor']]
    values.rename(columns={'weighted_co2_factor': 'Intensity_Index'}, inplace=True)

    # Identify Emissions tech/DUID with the largest contribution (increase) value for Time-Region
    source = filt_df.sort_values(['Increase']).drop_duplicates(['Time','Region'], keep="last")[['Time', 'Region', 'DUID', 'CO2E_ENERGY_SOURCE']]
    result = values.merge(source, on=['Time','Region'], how='left')

    return result


def tech_rename(fuel, tech_descriptor, dispatch_type):
    # """LEGACY. DEPRECATED.
    # Name technology type based on fuel, and descriptions of AEMO

    # Parameters
    # ----------
    # fuel : str
    #     Fuel Source - Descriptor field of a specific DUID
    # tech_descriptor : str
    #     Technology Type - Descriptor field of a specific DUID
    # dispatch_type : str
    #     Dispatch Type field of a specific DUID

    # Returns
    # -------
    # str
    #     Returns a simplified name for the above arguments
    # """

    # name = fuel
    # if fuel in ['Solar', 'Wind', 'Black Coal', 'Brown Coal']:
    #     pass
    # elif tech_descriptor == 'Battery':
    #     if dispatch_type == 'Load':
    #         name = 'Battery Charge'
    #     else:
    #         name = 'Battery Discharge'
    # elif tech_descriptor in ['Hydro - Gravity', 'Run of River']:
    #     name = tech_descriptor
    # elif tech_descriptor == 'Pump Storage':
    #     if dispatch_type == 'Load':
    #         name = 'Pump Storage Charge'
    #     else:
    #         name = 'Pump Storage Discharge'
    # elif tech_descriptor == '-' and fuel == '-' and dispatch_type == 'Load':
    #     name = 'Pump Storage Charge'
    # elif tech_descriptor == 'Open Cycle Gas turbines (OCGT)':
    #     name = 'OCGT'
    # elif tech_descriptor == 'Combined Cycle Gas Turbine (CCGT)':
    #     name = 'CCGT'
    # elif fuel in ['Natural Gas / Fuel Oil', 'Natural Gas'] and tech_descriptor == 'Steam Sub-Critical':
    #     name = 'Gas Thermal'
    # elif isinstance(tech_descriptor, str) and 'Engine' in tech_descriptor:
    #     name = 'Reciprocating Engine'
    # return name
    raise Exception("DEPRECATED in this version of NEMED. Refer to documentation: https://nemed.readthedocs.io/en/latest/")


def aggregate_data_by(data, by):
    """Aggregate the total emissions dataset metrics of Sent Out Generation, Total Emissions and Intensity Index.

    Parameters
    ----------
    data : pandas.DataFrame
        Dataframe input must correspond to the output from `get_total_emissions` with the `by` arugment set to None.
    by : str
        One of ['interval', 'hour', 'day', 'month', 'year']

    Returns
    -------
    pandas.DataFrame
        Data is returned as:

        ===============  ========  ==============================================================================================================================================
        Columns:         Type:     Description:
        TimeBeginning    datetime  Timestamp for start of interval or aggregation period. Only returned if `by` parameter is set.
        TimeEnding       datetime  Timestamp for end of interval or aggregation period.
        Region           str       The NEM region corresponding to data. 'NEM' field reflects all regions and is returned if `filter_regions` is None from `get_total_emissions`. 
        Energy           float     The total (sent-out if `generation_sent_out` is True from `get_total_emissions`) energy for the corresponding region and time.
        Total_Emissions  float     The total emissions for the corresponding region and time.
        Intensity_Index  float     The intensity index as above, considering the total emissions divided by (sent-out) energy.
        ===============  ========  ==============================================================================================================================================

    Raises
    ------
    Exception
        Invalid dataframe input.
    """
    aggregate = []
    for region in data['Region'].unique():
        sub_df = data[data['Region']==region]
        if ('TimeEnding' in sub_df.columns):
            sub_df = sub_df.rename(columns={'TimeEnding': 'Time'})
        
        if ('TimeBeginning' in sub_df.columns):
            raise Exception("already aggregated data cannot be passed to `aggregate_data_by` function. " +\
                "The `get_total_emissions` `by` input must be set to None to use this function post-operand")       

        sub_df = _time_aggregations(data=sub_df, by=by)
        sub_df.insert(2, 'Region', region)
        aggregate += [sub_df]
    aggregate = pd.concat(aggregate, ignore_index=True)
    return aggregate


def _time_aggregations(data, by):
    """Aggregations of total emissions dataset from 5-minute DI resolution to hour, day, month, or year
    """
    result = data.copy()
    en_colname = result.columns[result.columns.str.contains('Energy')][0]
    agg_map = {en_colname: np.sum, "Total_Emissions": np.sum}
    if 'Intensity_Index' in result.columns:
        reproduce_II = True
        result.drop(['Intensity_Index'], axis=1, inplace=True)
    else:
        reproduce_II = False

    # Shift data to time-beginning for aggregations
    result['Time'] = result['Time'] - timedelta(minutes=5)
    result.set_index(['Time'], inplace=True)

    # Aggregations
    if by == "interval":
        # Format result to show on interval resolution
        result.drop(["Region"],axis=1,inplace=True)
        result.insert(0, "TimeBeginning", result.index)
        result.insert(1, "TimeEnding", result.index + timedelta(minutes=5))
        result.reset_index(drop=True, inplace=True)
        
    elif by == "hour":
        # Hourly time resolution
        result = result.groupby(by=[result.index.year,
                                    result.index.month,
                                    result.index.day,
                                    result.index.hour]).agg(agg_map)

        result.index.names = ['Y','m','d','H']
        result.reset_index(inplace=True)
        result.insert(0,"TimeBeginning", pd.to_datetime(dict(year=result.Y,
                                                             month=result.m,
                                                             day=result.d,
                                                             hour=result.H)))
        result.drop(['Y','m','d','H'], axis=1, inplace=True)
        result.insert(1,"TimeEnding", result['TimeBeginning'] + timedelta(hours=1))

    elif by == "day":
        # Daily time resolution
        result = result.groupby(by=[result.index.year,
                                    result.index.month,
                                    result.index.day]).agg(agg_map)

        result.index.names = ['Y','m','d']
        result.reset_index(inplace=True)
        result.insert(0,"TimeBeginning", pd.to_datetime(dict(year=result.Y,
                                                             month=result.m,
                                                             day=result.d)))
        result.drop(['Y','m','d'], axis=1, inplace=True)
        result.insert(1, "TimeEnding", result['TimeBeginning'] + timedelta(days=1))

    elif by == "month":
        # Monthly time resolution
        result = result.groupby(by=[result.index.year,
                                    result.index.month]).agg(agg_map)
        result.index.names = ['Y','m']
        result.reset_index(inplace=True)
        result.insert(0,"TimeBeginning", pd.to_datetime(dict(year=result.Y,
                                                             month=result.m,
                                                             day=1)))
        result.insert(1,"TimeEnding", pd.to_datetime(dict(year=result.Y,
                                                          month=result.m + 1,
                                                          day=result['TimeBeginning'].dt.day)))
        result.drop(['Y','m'], axis=1, inplace=True)

    elif by == "year":
        # Annual time resolution
        result = result.groupby(by=[result.index.year]).agg(agg_map)
        result.index.names = ['Y']
        result.reset_index(inplace=True)
        result.insert(0,"TimeBeginning", pd.to_datetime(dict(year=result.Y,
                                                             month=1,
                                                             day=1)))
        result.insert(1,"TimeEnding", pd.to_datetime(dict(year=result.Y + 1,
                                                          month=1,
                                                          day=1)))
        result.drop(['Y'], axis=1, inplace=True)

    else:
        raise Exception(
            "Error: invalid by argument. Must be one of [interval, hour, day, month, year]"
        )

    # Update Intensity Index is found in data input
    if reproduce_II:
        result['Intensity_Index'] = result['Total_Emissions'] / result[en_colname]
        result['Intensity_Index'] = result['Intensity_Index'].fillna(0.0)

    return result.round(3)