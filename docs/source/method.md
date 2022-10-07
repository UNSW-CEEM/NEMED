# Methodology

## Total & Average Emissions
Total and Average Emissions are computed by extracting 5-minute dispatch interval data for each generator in the NEM for respective regions, mapping this data to reported CO2-equivalent emissions intensity metrics per unit (generator)-level, and returning the corresponding emissions: total emissions(tCO2-e), or average emissions, also referred to as, emisssions intensity (tCO2-e/MWh) per interval. 


### Overview of Method

1. Extract generator emissions factors from the 'Available Generators file' published in the AEMO CDEII dataset.
2. Download unit dispatch scada data from AEMO MMS (via NEMOSIS)
3. Filter for generators corresponding to the desired region.
4. Calculate energy (MWh) per dispatch interval assuming a linear ramp between scada values (MW).
5. Adjust energy values by the auxilary load factors for each generator. This would now reflect 'sent-out' generation as opposed to 'as generated'.
6. Mulitply the energy values by emissions factors to calculate the total emissions per dispatch interval per generator.
7. Aggregating this data yields a regional level metric for total sent-out generation, total emissions and emissions intensity (or average emissions) by dividing the aggregate total emissions by the aggregate sent-out generation

Step 4 can be enabled/disabled by setting the `assume_ramp` parameter of functions `get_total_emissions...`. If disabled a stepped not ramp function reflects energy. By default `assume_ramp` is set True.

Step 5 can also be disabled by setting `generation_sent_out` to False, meaning the energy values are left unchanged to be 'as generated'. By default this parameter is set True.

### Why NEMED is a good, not perfect tool!

**AEMO Sent-out Generation**<br>
The AEMO CDEII data is retrieved/calculated from settlements data and conincides with the preliminary (not final) settlement run of each trading week. The sent-out energy generation is measured through metering every 30-minutes or less. Settlements data however is not publicly available.

**NEMED Sent-out Generation**<br>
NEMED derives the sent-out energy generation from scada data which is publicly available data through the AEMO's MMS database which captures power readings (MW) at each generating unit, reflecting 'as generated'.

The auxilary load factors considered in NEMED when deriving sent-out generation are static values extracted from the Input Assumptions and Scenario Report (IASR). The actual auxilary load of a generating unit may vary depending on operation.

**Generating unit Emissions Factors**<br>
The Emissions Factors applied in NEMED are also extracted from static datasets by AEMO, based on the IASR. Actual emissions factors may also vary based on the operation of the plant.

**Other possible factors**<br>
The above is by no means a comprehensive list of potential sources for discrepency. Note also that it is not impossible for there to be data quality issues with either the preliminary settlements data or scada data from AEMO.  

## Marginal Emissions

Marginal Emissions are computed by extracting the marginally dispatched generators from AEMO's Price Setter files, mapping emissions intensity metrics mentioned above and hence computing marginal emissions (tCO2-e/MWh).