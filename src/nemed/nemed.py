"""Core user interfacing module"""
import nemed.process as nd
import nemed.helper_functions.helpers as hp
from datetime import datetime as dt, timedelta


def get_total_emissions(start_time, end_time, cache, filter_regions, by="interval",
                        generation_sent_out=True, assume_ramp=True):
    """Retrieve Aggregated Emissions data for total and average emissions (emissions intensity), as well as sent-out
    energy generation for a defined period and time-resolution (e.g. interval, day, month)

    Parameters
    ----------
    start_time : str
        Start Time Period in format 'yyyy/mm/dd HH:MM:SS'
    end_time : str
        End Time Period in format 'yyyy/mm/dd HH:MM:SS'
    cache : str
        Raw data location in local directory
    filter_regions : list of str
        NEM regions to filter for while retrieving the data, as a list.
    by : str, one of ['interval', 'hour', 'day', 'month', 'year']
        The time-resolution of output data to aggregate to, by default "interval"
    generation_sent_out : bool
        Considers 'sent_out' generation (auxilary loads) as opposed to 'as generated' in calculations, by default True
    assume_ramp : bool
        Uses a linear ramp between dispatch datapoints as opposed to a stepped function, by default True

    Returns
    -------
    dict
        Dictionary containing keys ['Energy','Total Emissions','Intensity Index'], each containing a dataframe of the
        time series results.
    """
    # Check if cache folder exists
    hp._check_cache(cache)

    # Get emissions for all units by dispatch interval
    raw_table = nd.get_total_emissions_by_DI_DUID(
        start_time, end_time, cache, filter_regions=filter_regions,
        generation_sent_out=generation_sent_out, assume_ramp=assume_ramp)
    clean_table = raw_table.drop_duplicates(subset=['Time', 'DUID'])

    # Pivot and summate data. Aggregates to a regional level on interval
    data = clean_table.pivot_table(
        index="Time",
        columns="REGIONID",
        values=["Energy", "Total_Emissions"],
        aggfunc="sum",
    )

    # Compute Emissions Intensity Index (average emissions) from total emissions divided by total energy
    for region in data.columns.levels[1]:
        data[("Intensity_Index", region)] = (
            data["Total_Emissions"][region] / data["Energy"][region]
        )

    # Aggregate interval-resolution data to defined resolution
    result = nd.aggregate_data_by(data=data, by=by)

    return result


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
