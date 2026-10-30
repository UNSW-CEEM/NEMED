"""Offline tests (no downloads) for the nemosis 3.8 / pandas 2 compatibility fixes."""
import os

import numpy as np
import pandas as pd
import pytest

import nemed
from nemed import nemed as front_end
from nemed import process
from nemed.helper_functions import mod_nemosis, mod_xml_cache


@pytest.mark.parametrize("source, year", [
    ("ISP2024", 2024), ("ISP 2020", 2020), ("NGA2022", 2022), ("NGA 2024", 2024), ("ISP 2018", 2018),
    ("Estimate - NGA 2016", 2016), ("On Exclusion List", None), ("Estimated", None), ("Excluded NMNS", None),
    (None, None), (np.nan, None),
])
def test_data_source_year(source, year):
    assert process._data_source_year(source) == year


def test_prefer_current_factors_keeps_the_genset_with_a_factor():
    # Tamar Valley CCGT: the steam turbine is on the exclusion list (no factor); the gas turbine carries the factor.
    rows = pd.DataFrame([
        {"file_year": 2024, "file_month": 8, "DUID": "TVCC201", "GENSETID": "TVCC_GT1", "CO2E_EMISSIONS_FACTOR": 0.516,
         "CO2E_ENERGY_SOURCE": "Natural Gas (Pipeline)", "CO2E_DATA_SOURCE": "ISP2022"},
        {"file_year": 2024, "file_month": 8, "DUID": "TVCC201", "GENSETID": "TVCC_ST1", "CO2E_EMISSIONS_FACTOR": np.nan,
         "CO2E_ENERGY_SOURCE": "Natural Gas (Pipeline)", "CO2E_DATA_SOURCE": "On Exclusion List"},
    ])
    for order in (rows, rows.iloc[::-1]):
        kept = process._prefer_current_factors(order)
        assert len(kept) == 1 and kept["CO2E_EMISSIONS_FACTOR"].iloc[0] == 0.516


def test_prefer_current_factors_keeps_the_newest_source():
    rows = pd.DataFrame([
        {"file_year": 2026, "file_month": 9, "DUID": "LYA1", "GENSETID": "LYA1", "CO2E_EMISSIONS_FACTOR": f,
         "CO2E_ENERGY_SOURCE": "Brown coal", "CO2E_DATA_SOURCE": src}
        for f, src in [(1.1689, "ISP2024"), (1.1378, "ISP2022"), (1.20, "NTNDP 2014")]
    ])
    kept = process._prefer_current_factors(rows.sample(frac=1, random_state=1))
    assert kept["CO2E_DATA_SOURCE"].tolist() == ["ISP2024"]


def test_month_aggregation_ends_december_in_january():
    times = pd.date_range("2023-12-01 00:05", "2024-01-01 00:00", freq="5min")
    data = pd.DataFrame({"Time": times, "Region": "NSW1", "Energy": 1.0, "Total_Emissions": 0.5})
    out = process._time_aggregations(data, by="month")
    assert out["TimeBeginning"].tolist() == [pd.Timestamp("2023-12-01")]
    assert out["TimeEnding"].tolist() == [pd.Timestamp("2024-01-01")]
    assert out["Energy"].iloc[0] == len(times)


def test_nem_total_with_pandas_2(monkeypatch):
    t = pd.Timestamp("2024-08-01 00:05")
    # As generated (Energy) and sent out (Energy_SO): emissions come from sent-out energy, so the intensity must too.
    per_unit = pd.DataFrame([
        {"DUID": "A", "Time": t, "Region": "NSW1", "Energy": 11.0, "Energy_SO": 10.0, "Total_Emissions": 6.0},
        {"DUID": "B", "Time": t, "Region": "VIC1", "Energy": 5.5, "Energy_SO": 5.0, "Total_Emissions": 4.0},
    ])
    monkeypatch.setattr(front_end.nd, "get_total_emissions_by_DI_DUID", lambda *a, **k: per_unit)
    monkeypatch.setattr(front_end.hp, "_check_cache", lambda cache: cache)
    out = nemed.get_total_emissions("2024/08/01 00:00", "2024/08/01 00:05", "cache")
    nem = out[out["Region"] == "NEM"].iloc[0]
    assert nem["Energy"] == 15.0 and nem["Total_Emissions"] == 10.0
    assert nem["Intensity_Index"] == pytest.approx(10 / 15)


def test_period_ids_accept_both_formats():
    out = mod_xml_cache._period_ids(pd.Series(["2021-01-01 00:05:00", "2024-08-01T00:05:00+10:00"]))
    assert out.tolist() == [pd.Timestamp("2021-01-01 00:05"), pd.Timestamp("2024-08-01 00:05")]
    assert out.dt.tz is None


def test_fetch_loop_reads_every_chunk(tmp_path, monkeypatch):
    """From August 2024 a month is PUBLIC_ARCHIVE#<TABLE>#FILE01#..., FILE02, ...: read chunks until one is missing."""
    for chunk in (1, 2):
        stub = f"PUBLIC_ARCHIVE#GENUNITS#FILE{chunk:02d}#202408010000"
        pd.DataFrame({"GENSETID": [f"G{chunk}"], "CO2E_EMISSIONS_FACTOR": ["0.5"]}).to_parquet(
            tmp_path / f"{stub}.parquet")
    downloads = []
    monkeypatch.setattr(mod_nemosis, "_download_data", lambda *args: downloads.append(args))
    monkeypatch.setitem(mod_nemosis._defaults.table_types, "GENUNITS", "MMS")
    monkeypatch.setattr(mod_nemosis._processing_info_maps, "date_gen",
                        {"MMS": lambda start, end: iter([("2024", "08", None, None)])})
    tables = mod_nemosis.mod_dynamic_data_fetch_loop(
        start_search=None, start_time=None, end_time=None, table_name="GENUNITS",
        raw_data_location=str(tmp_path), select_columns=["GENSETID", "CO2E_EMISSIONS_FACTOR"], date_filter=None,
        fformat="parquet", keep_csv=False)
    assert [t["GENSETID"].iloc[0] for t in tables] == ["G1", "G2"]
    assert all(list(t.columns[:2]) == ["file_year", "file_month"] for t in tables)
    assert len(downloads) == 1  # only the missing third chunk was looked for


def test_fetch_loop_reads_one_file_before_august_2024(tmp_path, monkeypatch):
    stub = "PUBLIC_DVD_GENUNITS_202203010000"
    pd.DataFrame({"GENSETID": ["G"], "CO2E_EMISSIONS_FACTOR": ["0.5"]}).to_parquet(tmp_path / f"{stub}.parquet")
    monkeypatch.setattr(mod_nemosis, "_download_data", lambda *args: pytest.fail("nothing to download"))
    monkeypatch.setitem(mod_nemosis._defaults.names, "GENUNITS", "PUBLIC_DVD_GENUNITS")
    monkeypatch.setitem(mod_nemosis._defaults.table_types, "GENUNITS", "MMS")
    monkeypatch.setattr(mod_nemosis._processing_info_maps, "date_gen",
                        {"MMS": lambda start, end: iter([("2022", "03", None, None)])})
    tables = mod_nemosis.mod_dynamic_data_fetch_loop(
        start_search=None, start_time=None, end_time=None, table_name="GENUNITS",
        raw_data_location=str(tmp_path), select_columns=["GENSETID"], date_filter=None, fformat="parquet")
    assert len(tables) == 1 and os.path.exists(tmp_path / f"{stub}.parquet")
