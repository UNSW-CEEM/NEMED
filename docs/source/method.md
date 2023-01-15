# Methodology

## Total Emissions
Total Emissions are computed by extracting 5-minute dispatch interval data for each generator in the NEM for respective regions, mapping this data to reported CO2-equivalent emissions intensity metrics per unit (generator)-level, and returning the corresponding emissions: total emissions(tCO2-e), and emisssions intensity (tCO2-e/MWh) per interval. 


### Overview of Method
NEMED reproduces emissions data from a number of AEMO datasets, as such:

1. Download unit dispatch scada data from the AEMO MMS 'DISPATCH_UNIT_SCADA' table (via `NEMOSIS`)
2. Download duid details from MMS 'DUDETAILSUMMARY' table, identifying the dispatch type and region of each plant.
3. Filtering out loads from the dispatch data, and only generators in the corresponding region (if the user wishes to filter for a region).
4. Downloading plant emissions factors from MMS 'GENUNITS' table and correspondingly 'DUALLOC' to map this data which is by GENSETID to DUID.
5. Calculating energy (MWh) from dispatch (MW) by assuming a linear ramp (Note: This process can be toggled by the user).
6. Calculating 'Sent-Out' Energy by considering auxiliary loads for the prior step's data which is 'as-generated'.
    - This step applies auxiliary load factors from AEMO ISP data which are static values, coarse in nature.
    - If desired, users can modify these values as explained below.
7. Aggregating the final dataset by 'hour','day','month', or 'year' if desired. Alternatively the user can simply retrieve the 5-minute time resolution data.
8. Processing the 'Intensity_Index' for the dataset by dividing Total Emissions by the Sent-Out Energy.


#### Auxiliary Load Factors
Almost all data tables in NEMED originate from a dynamic source. However, auxiliary loads are applied from a static .csv file based on data assumptions published in AEMO ISP workbooks. Should you wish to access these assumptions they can be found in the [NEMED Sourcecode Data Folder](https://github.com/UNSW-CEEM/NEMED/tree/master/src/nemed/data/plant_auxiliary). Editing this file (which is discouraged) will apply changes to the results produced by NEMED

### Why NEMED is a good, not perfect tool!
There a numerous factors leading to discrepency between NEMED processed data and AEMO daily summary files of emissions metrics. Notwithstanding that it is impossible to measure emissions exactly in the real world, NEMED is a good shot at reproducing AEMO data but is not perfect as:

**AEMO Sent-out Generation**<br>
The AEMO CDEII data is processed from settlements data and conincides with the preliminary (not final) settlement run of each trading week. The sent-out energy generation is measured through metering every 30-minutes or less. Importantly such data reflects *energy (MWh)* not *power (MW)*.Settlements data however is not publicly available.

**NEMED Sent-out Generation**<br>
NEMED derives the sent-out energy generation from scada data which is publicly available data through the AEMO's MMS database which captures power readings (MW) at each generating unit, reflecting 'as generated'. Two discrepencies are therefore:
- Mismatch between actual measured energy in settlements, and presumed energy from a linear ramp between 5-minute scada points.
- The auxilary load factors considered in NEMED when deriving sent-out generation are static values extracted from the Input Assumptions and Scenario Report (IASR) of AEMO's ISP. The actual auxilary load of a generating unit may vary depending on operation. This is likely the most impactful discrepency in reproducing emissions data from numerous AEMO datasets.

**Generating unit Emissions Factors**<br>
Plant Emissions Factors applied in NEMED are also relatively static, although NEMED finds the corresponding CO2 factors for each plant for a specific historical dispatch interval which is extracted from a dynamic AEMO MMS table. Particular units also at times may be 'On Exclusion List' to which their emissions are not accounted for by the CDEII procedure. Actual emissions factors may also vary based on the operation of the plant, however the assumptions in NEMED are the same assumed values used by AEMO in the CDEII procedure - so this is not likely to led to discrepencies between the two.

**Other possible factors**<br>
The above list is by no means comprehensive, but does justice in exploring the issues around emissions data. Note also that it is possible for there to be data quality issues within the published AEMO CDEII datasets which is why this should be treated as a guiding comparison not absolute truth.  

## Marginal Emissions

Marginal Emissions are computed by extracting the marginally dispatched generators from AEMO's Price Setter files, mapping emissions intensity metrics mentioned above and hence computing marginal emissions (tCO2-e/MWh). This is a relatively simple process in comparison to the above, the most burdensome element here is downloading and processing price setter files.

Similar remarks can be made about the assumptions of plant emissions intensity above.


## More Points of Contention
*"...but I want caviar, not caveats!"*

A brief note on some critical points:

- Storages (Pumped Hydro, Batteries) are considered by AEMO to have plant emissions factors of zero! There are quite a few ways one might consider to handle this more cautiously, but we'll leave it to you :) Check the [nemed.process](https://nemed.readthedocs.io/en/latest/api/process.html) module where by using `get_total_emissions_by_DI_DUID` you can access the full NEMED dataset per DUID before it is aggregated regionally. Then you could overwrite the emissions factors for storages and continue from there (at least the tedious raw data extraction and mapping is already done for you!)

- Total (Average) emissions reflect only the generation within that region. NEMED does not account for interconnector flows or losses between generation and the point at which you might assume a load to be consuming grid-energy and hence have this assosciated average emissions intensity.

- Marginal emissions may reflect price setting generators from other NEM regions. Similarly though, the remarks about losses.

