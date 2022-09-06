"""Core user interfacing module"""
import nemed.process as nd
import nemed.helper_functions.helpers as hp


def get_total_emissions(start_time, end_time, cache, filter_regions, by="interval", 
                        generation_sent_out=True, save_debug_file=False):
    # Check if cache folder exists
    hp._check_cache(cache)

    # Get emissions for all units by dispatch interval
    raw_table = nd.get_total_emissions_by_DI_DUID(
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

    # Compute Emissions Intensity Index (average emissions) from total emissions divided by total energy
    for region in data.columns.levels[1]:
        data[("Intensity_Index", region)] = (
            data["Total_Emissions"][region] / data["Energy"][region]
        )

    result = nd.aggregate_data_by(data=data, by=by)

    return result
