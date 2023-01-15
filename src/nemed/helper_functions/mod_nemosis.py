""" Modifies functionality from nemosis to extract emissions factors"""
import logging
import os as _os
import glob as _glob
from nemosis import defaults as _defaults
from nemosis import processing_info_maps as _processing_info_maps
from nemosis import data_fetch_methods
from nemosis.data_fetch_methods import _create_filename, _download_data, _get_read_function, \
    _determine_columns_and_read_csv, _perform_column_selection, _log_file_creation_message, _write_to_format

logger = logging.getLogger(__name__)

def overwrite_nemosis_defaults():
    """Overwrites nemosis defaults with table properties for GENUNITS from AEMO NEMWEB.
    """
    _defaults.table_types.update({"GENUNITS": "MMS", "DUALLOC": "MMS"})
    _defaults.names.update({"GENUNITS": "PUBLIC_DVD_GENUNITS", "DUALLOC": "PUBLIC_DVD_DUALLOC"})
    _defaults.data_url.update({"GENUNITS": "aemo_data_url", "DUALLOC": "aemo_data_url"})
    _defaults.table_columns.update({"GENUNITS": ['GENSETID', 'LASTCHANGED', 'VOLTLEVEL', 'REGISTEREDCAPACITY', \
        'DISPATCHTYPE', 'STARTTYPE', 'MAXCAPACITY', 'GENSETTYPE', 'CO2E_EMISSIONS_FACTOR', 'CO2E_ENERGY_SOURCE', \
        'CO2E_DATA_SOURCE'],
        "DUALLOC": ["EFFECTIVEDATE", "VERSIONNO", "DUID", "GENSETID", "LASTCHANGED"]})


def mod_dynamic_data_fetch_loop(
    start_search,
    start_time,
    end_time,
    table_name,
    raw_data_location,
    select_columns,
    date_filter,
    fformat="feather",
    keep_csv=True,
    caching_mode=False,
    rebuild=False,
    write_kwargs={},
):
    """Modified function from nemosis.data_fetch_methods
    """
    data_tables = []

    table_type = _defaults.table_types[table_name]
    date_gen = _processing_info_maps.date_gen[table_type](start_search, end_time)

    for year, month, day, index in date_gen:
        filename_stub, full_filename, path_and_name = _create_filename(
            table_name, table_type, raw_data_location, fformat, day, month, year, index
        )

        if not (
            _glob.glob(full_filename) or _glob.glob(path_and_name + ".[cC][sS][vV]")
        ) or (not _glob.glob(path_and_name + ".[cC][sS][vV]") and rebuild):
            _download_data(
                table_name,
                table_type,
                filename_stub,
                day,
                month,
                year,
                index,
                raw_data_location,
            )

        if _glob.glob(full_filename) and fformat != "csv" and not rebuild:
            if not caching_mode:
                data = _get_read_function(fformat, table_type, day)(full_filename)
                data.insert(0, 'file_month', month)
                data.insert(0, 'file_year', year)
            else:
                data = None
                logger.info(
                    f"Cache for {table_name} in date range already compiled in"
                    + f" {raw_data_location}."
                )

        elif _glob.glob(path_and_name + ".[cC][sS][vV]"):

            if select_columns != "all":
                read_all_columns = False
            else:
                read_all_columns = True

            if not caching_mode:
                dtypes = "str"
            else:
                dtypes = "all"

            csv_path_and_name = _glob.glob(path_and_name + ".[cC][sS][vV]")[0]

            csv_read_function = _get_read_function(
                fformat="csv", table_type=table_type, day=day
            )
            data = _determine_columns_and_read_csv(
                table_name,
                csv_path_and_name,
                csv_read_function,
                read_all_columns=read_all_columns,
                dtypes=dtypes,
            )
            data.insert(0, 'file_month', month)
            data.insert(0, 'file_year', year)
            if caching_mode:
                data = _perform_column_selection(data, select_columns, full_filename)
                data.insert(0, 'file_month', month)
                data.insert(0, 'file_year', year)
            if data is not None and fformat != "csv":
                _log_file_creation_message(fformat, table_name, year, month, day, index)
                data.drop(['file_month','file_year'], axis=1,inplace=True)
                _write_to_format(data, fformat, full_filename, write_kwargs)

            if not keep_csv:
                _os.remove(_glob.glob(path_and_name + ".[cC][sS][vV]")[0])
        else:
            data = None

        if not caching_mode and data is not None:

            if date_filter is not None:
                data = date_filter(data, start_time, end_time)

            data = _perform_column_selection(data, select_columns, full_filename)
            data.insert(0, 'file_month', month)
            data.insert(0, 'file_year', year)
            data_tables.append(data)
        elif not caching_mode:
            logger.warning(f"Loading data from {full_filename} failed.") # logger.
        
    return data_tables


# def overwrite_nemosis_data_fetch():
#     """Overwrites nemosis dyanmic_data_fetch_loop with added file year-month properties inserted into extracted data
#     table from AEMO NEMWEB.
#     """
#     data_fetch_methods._dynamic_data_fetch_loop = mod_dynamic_data_fetch_loop

