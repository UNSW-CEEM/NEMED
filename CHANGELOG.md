# Changelog

<!--next-version-placeholder-->

## v0.4.1 nemosis 3.8, pandas 2 and AEMO's chunked archives (30/10/2026)
- Works with AEMO's MMSDM files from August 2024 on, which are split into numbered chunks
  (`PUBLIC_ARCHIVE#<TABLE>#FILE01#...`). The fetch loop now follows nemosis 3.8, which reads every chunk (#20).
- Requires nemosis >= 3.8.1 and pandas >= 2.2; Python 3.10 to 3.12. Fixes the package metadata that modern
  installers refused (`pandas = "^1.2 || ^2.0"`).
- pandas 2: the NEM total no longer fails with "cannot insert Region, already exists"; monthly aggregation no longer
  fails on December (month 13); deprecated `groupby(axis=...)`, `'5T'` and chained `fillna(inplace=True)` removed.
- Emissions factors: data sources from 2020 on (ISP2020, ISP2022, ISP2024, NGA2022, NGA 2024, ...) are now ranked by
  year, and a unit with several gensets keeps the genset that has a factor. Before this, a combined-cycle unit could
  lose its factor to its excluded steam turbine and drop out (Tamar Valley TVCC201: Tasmania read 0 t in August 2024).
- Battery storage is left out of total emissions by default (`exclude_storage=True`), as AEMO's CDEII does: its
  discharge is energy taken from the grid earlier.
- `get_total_emissions` now divides sent-out emissions by sent-out energy. It used to pick the first column containing
  "Energy", which is the as-generated energy, so every intensity it returned was low by roughly the auxiliary load
  (3 to 9 % in coal regions). The returned column is still called `Energy` (sent out when `generation_sent_out`).
- Marginal emissions read NEMDE PeriodIDs in both the old format and the newer ISO 8601 form with a +10:00 offset.
- New `downloader.download_cdeii_billing`: AEMO's daily CDEII figures from the MMSDM table BILLING_CO2E_PUBLICATION,
  for any month in the archive (the yearly summary files after 2022 aren't on NEMweb).
- Checked against AEMO's CDEII by month (sent-out, every region): August 2024 within 4.2 %; March 2022 within 1.9 %
  on the mainland (Tasmania 0.0065 against 0.0059 t/MWh).
- Offline tests for each fix (`tests/test_offline_fixes.py`).

## v0.3.3 Minor bug fix (10/06/2023)
- Fix an issue where some days of price setter data was not read correctly from the AEMO data archive.

## v0.3.2 Bug fixes (15/01/2023)
- Fix function argument errors
- v0.3.0 and v0.3.1 have known issues. Use v0.3.2

## v0.3.1 Major update to Marginal Emissions (15/01/2023)
- Refined extraction of price-setter files for marginal emissions data.
- Updated example for marginal emissions
- Updated readme and documentation

## v0.3.0 Major update to Total Emissions (10/01/2023)
- New method for extracting total emissions data.
- Updated examples for CDEII Comparison and Total emissions.
- Extended usable historical date range, tested to at least as early as 2015.
- Updated readme and documentation

## v0.2.2 Minor fixes (01/11/2022)
- Dependency resolution

## v0.2.1 Minor fixes (16/10/2022)
- Performance improvement in time to download/process price setter files for marginal emissions.
- Update documentation readme

## v0.2.0 Beta Release 1 (08/10/2022)
- Updated methodology for total/average emissions data extraction. Method detailed in the docs.
- Updated examples using FY19-20 data for total/average emissions

## v0.1.1 Initial Beta Release of NEMED (16/09/2022)

- Features to extract and process sent-out generation, total emissions and intensity index (average emissions) per dispatch interval per region
- Features to extract marginal emissions / emitter table via AEMO's price setting files
