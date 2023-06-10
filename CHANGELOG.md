# Changelog

<!--next-version-placeholder-->

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
