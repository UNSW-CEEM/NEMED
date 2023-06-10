""" Modifies functionality from nempy.historical_inputs.xml_cache"""
from pathlib import Path
import requests
import zipfile
import io
import os
import glob
import json
import xmltodict
import logging
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta
from nempy.historical_inputs.xml_cache import XMLCacheManager
logger = logging.getLogger(__name__)


def modpricesetter_get_file_name(self):
    """Modified function from nempy.historical_inputs.xml_cache
    """
    year, month, day = self._get_market_year_month_day_as_str()
    interval_number = self._get_interval_number_as_str()
    base_name = "NEMPriceSetter_{year}{month}{day}{interval_number}00.xml"
    name = base_name.format(
        year=year, month=month, day=day, interval_number=interval_number
    )
    path_name = Path(self.cache_folder) / name
    name_OCD = name.replace(".xml", "_OCD.xml")
    path_name_OCD = Path(self.cache_folder) / name_OCD
    name_zero = name.replace(".xml", "00.xml")
    path_name_zero = Path(self.cache_folder) / name_zero
    if os.path.exists(path_name):
        return name
    elif os.path.exists(path_name_OCD):
        return name_OCD
    elif os.path.exists(path_name_zero):
        return name_zero
    else:
        return name


def modpricesetter_download_xml_from_nemweb(self):
    """Modified function from nempy.historical_inputs.xml_cache
    """
    year, month, day = self._get_market_year_month_day_as_str()
    base_url = "https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/NEMDE/{year}/NEMDE_{year}_{month}/" + \
        "NEMDE_Market_Data/NEMDE_Files/NemPriceSetter_{year}{month}{day}_xml.zip"
    url = base_url.format(year=year, month=month, day=day)
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(self.cache_folder)


def overwrite_xmlcachemanager_with_pricesetter_config():
    """Overwrites nempy xml_cache with modified functions to pull price setter data from AEMO NEMWEB.
    """
    # XMLCacheManager.get_file_name = modpricesetter_get_file_name(xmlcachemanager)
    # XMLCacheManager._download_xml_from_nemweb = modpricesetter_download_xml_from_nemweb(xmlcachemanager)
    XMLCacheManager.get_file_name = modpricesetter_get_file_name
    XMLCacheManager._download_xml_from_nemweb = modpricesetter_download_xml_from_nemweb

def convert_xml_to_json(start_date_str, end_date_str, cache, clean_up=False):
    """Converts all XML files found in cache to JSON format. Best to use an entirely separate cache folder from other
    nemosis or nempy projects!

    Parameters
    ----------
    cache : str
        Defined folder in directory to use as cache.
    clean_up : bool, optional
        Setting to True will remove XML files once they have been converted to JSON, by default False.
    """
    # Establish daterange
    sdate = datetime.strptime(start_date_str, "%Y/%m/%d")
    edate = datetime.strptime(end_date_str, "%Y/%m/%d")
    date_str_list = [datetime.strftime(i,"%Y%m%d") for i in pd.date_range(sdate,edate)]

    # Find only a subset of XML files in the given daterange
    xml_subset_nested = [glob.glob(os.path.join(cache, "NEMPriceSetter_{}*.xml".format(i))) for i in date_str_list]
    xml_subset = [item for sublist in xml_subset_nested for item in sublist]
    xml_files = glob.glob(os.path.join(cache, "NEMPriceSetter_*.xml"))
    print("Converting selected {} XML files to JSON, of {} cached files".format(len(xml_subset),len(xml_files)))
    print(xml_subset)
    # Read XML files and convert to JSON
    for filename in tqdm(xml_subset):
        handle = open(filename, 'r')
        content = handle.read()
        d = xmltodict.parse(content)
        write_file = filename.replace(".xml", ".json")
        json_path = write_file
        with open(json_path, 'w') as fp:
            json.dump(d['SolutionAnalysis']['PriceSetting'], fp)

    # Remove XML files if clean_up
    if clean_up:
        print("Clearing {} XML files from cache".format(len(xml_files)))
        for filename in xml_files:
            os.remove(os.path.join(cache, filename))


def read_json_to_df(start_dt, end_dt, cache):
    """Reads JSON files found in cache and returns price setter data as pandas dataframe.

    Parameters
    ----------
    cache : str
        Defined folder in directory to use as cache.

    Returns
    -------
    pd.DataFrame
        Price Setter dataframe containing columns: [PeriodID, RegionID, Market, Price, DUID, DispatchedMarket, BandNo,
        Increase, RRNBandPrice, BandCost]
    """
    # Establish files daterange
    collect_sdt = start_dt
    if (end_dt.hour == 0) & (end_dt.minute == 0):
        collect_edt = end_dt
    else:
        collect_edt = end_dt + timedelta(days=1)

    date_str_list = [datetime.strftime(i,"%Y-%m-%d") for i in pd.date_range(collect_sdt, collect_edt)]

    # Find only a subset of JSON files in the given daterange
    JSON_subset = [glob.glob(os.path.join(cache, "NEMED_PS_DAILY_{}*.json".format(i))) for i in date_str_list]
    JSON_subset = [item for sublist in JSON_subset for item in sublist]

    print("Reading selected {} JSON files to pandas, of cached files".format(len(JSON_subset)))
    logger.info("Loading Cached Price Setter Files...")

    all_df = []
    for file in tqdm(JSON_subset):
        with open(file, 'r') as f:
            data = json.loads(f.read())
        df_nested_list = pd.json_normalize(data)
        df_nested_list['@PeriodID'] = pd.to_datetime(df_nested_list['@PeriodID'], format="%Y-%m-%d %H:%M:%S").dt.tz_localize(None)
        df_nested_list = df_nested_list[(df_nested_list['@Market'] == 'Energy') &
                                        (df_nested_list['@DispatchedMarket'] == 'ENOF')]
        all_df += [df_nested_list]

    all_df = pd.concat(all_df)
    all_df.columns = all_df.columns.str.strip('@')
    all_df.drop(['Market','DispatchedMarket'], axis=1, inplace=True)
    all_df = all_df.astype({'RegionID': str, 'Price': float, 'Unit': str, 'BandNo': int, \
                            'Increase': float, 'RRNBandPrice': float, 'BandCost': float})
    all_df = all_df[all_df['PeriodID'].between(start_dt, end_dt, inclusive="right")].sort_values(['PeriodID','RegionID'])
    return all_df.reset_index(drop=True)
