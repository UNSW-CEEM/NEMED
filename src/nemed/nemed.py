"""Core user interfacing module"""
from . import process as nd
from . helper_functions import helpers as hp
from datetime import datetime as dt, timedelta
import pandas as pd

def get_total_emissions(start_time, end_time, cache, filter_regions=None, by=None, generation_sent_out=True, assume_energy_ramp=True, return_pivot=False):
    """Retrieve (Aggregated) Regional Emissions data for total emissions (absolute and emissions intensity), as well as sent-out
    energy generation for a defined period and time-resolution (e.g. hour, day, month)

    Parameters
    ----------
    start_time : str
        Start Time Period in format 'yyyy/mm/dd HH:MM:SS'
    end_time : str
        End Time Period in format 'yyyy/mm/dd HH:MM:SS'
    cache : str
        Raw data location in local directory
    filter_regions : list(str)
        NEM regions to filter for while retrieving the data, as a list, by default None to collect all region data
    by : str, one of ['interval', 'hour', 'day', 'month', 'year']
        The time-resolution of output data to aggregate to, by default None to return unaggregated 5-minute time resolution
    generation_sent_out : bool
        Considers 'sent_out' generation (auxilary loads) as opposed to 'as generated' in calculations, by default True
    assume_ramp : bool
        Uses a linear ramp between dispatch scada points as opposed to a stepped function, by default True
    return_pivot : bool
        Changes the structure of the returned dataframe to a pivot with column hierarchy as Data Metric then Region, by default False

    Returns
    -------
    pandas.DataFrame
        Data is returned as formatted if `return_pivot` = False:
        
        ===============  ========  ===================================================================================================================
        Columns:         Type:     Description:
        TimeBeginning    datetime  Timestamp for start of interval or aggregation period. Only returned if `by` parameter is set.
        TimeEnding       datetime  Timestamp for end of interval or aggregation period.
        Region           str       The NEM region corresponding to data. 'NEM' field reflects all regions and is returned if `filter_regions` is None. 
        Energy           float     The total (sent-out if `generation_sent_out` is True) energy for the corresponding region and time.
        Total_Emissions  float     The total emissions for the corresponding region and time.
        Intensity_Index  float     The intensity index as above, considering the total emissions divided by (sent-out) energy.
        ===============  ========  ===================================================================================================================
    """
    # Check if cache folder exists
    hp._check_cache(cache)

    # Get emissions for all units by dispatch interval
    raw_table = nd.get_total_emissions_by_DI_DUID(
        start_time, end_time, cache, filter_regions=filter_regions,
        generation_sent_out=generation_sent_out, assume_energy_ramp=assume_energy_ramp, return_all=True)
    clean_table = raw_table.drop_duplicates(subset=['Time', 'DUID'])

    # Aggregate DUID data to regions
    en_colname = clean_table.columns[clean_table.columns.str.contains('Energy')][0]
    res = clean_table[['Time', 'Region', en_colname, 'Total_Emissions']].groupby(['Time', 'Region'])\
        .sum().reset_index()

    # Create NEM agggregation
    if filter_regions == None:
        nem = res.groupby(['Time']).sum().reset_index()
        nem.insert(1,'Region','NEM')
        res = pd.concat([res,nem],ignore_index=True)

    # Aggregate data to `by`
    if by != None:
        aggregate = nd.aggregate_data_by(data=res, by=by)
    else:
        aggregate = res.rename(columns={'Time': 'TimeEnding'})

    # Calculate Intensity Index considering weighted sum of emissions / energy
    aggregate['Intensity_Index'] = aggregate['Total_Emissions'] / aggregate[en_colname]
    aggregate['Intensity_Index'] = aggregate['Intensity_Index'].fillna(0.0)

    # Return as pivot table
    if return_pivot:
        aggregate = aggregate.pivot(index="TimeEnding",
                                    columns="Region",
                                    values=[en_colname, "Total_Emissions", "Intensity_Index"])
    else:
        aggregate = aggregate.sort_values(['TimeEnding','Region']).reset_index(drop=True)
    
    return aggregate


def get_marginal_emissions(start_time, end_time, cache, redownload_xml=True):
    """Retrieve Raw Marginal Emissions data for for a defined period

    Parameters
    ----------
    start_time : str
        Start Time Period in format 'yyyy/mm/dd HH:MM:SS'
    end_time : str
        End Time Period in format 'yyyy/mm/dd HH:MM:SS'
    cache : str
        Raw data location in local directory
    redownload_xml : bool
        If false, attempts to find existing files in cache before downloading .xml files from AEMO database

    Returns
    -------
    pd.DataFrame
        Data containing marginal (price-setter) DUID for each dispatch interval with emissions factor and other
        useful generation information.
    """
    # Check if cache folder exists
    hp._check_cache(cache)

    # Extract datetime
    input_start = dt.strptime(start_time, "%Y/%m/%d %H:%M:%S")
    input_end = dt.strptime(end_time, "%Y/%m/%d %H:%M:%S")

    sdate = input_start - timedelta(days=1)
    edate = input_end + timedelta(days=1)

    result = nd.get_marginal_emitter(cache, start_year=sdate.year, start_month=sdate.month, start_day=sdate.day,
                                     end_year=edate.year, end_month=edate.month, end_day=edate.day,
                                     redownload_xml=redownload_xml)

    return result[result['PeriodID'].between(input_start+timedelta(minutes=5), input_end)]
