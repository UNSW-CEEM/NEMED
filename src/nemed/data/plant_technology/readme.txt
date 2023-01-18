Development Notes:

The _plant_auxload_assumptions.csv contains columns:
- EFFECTIVEFROM: The publish data of the ISP workbook on AEMO website.
- DATASOURCE: The ISP publication
- DUID: Generation Identifier
- GENERATOR: Generator name as shown in the corresponding ISP publication
- PCT_AUXILIARY_LOAD: The percentage auxiliary load for the corresponding DUID
- NOTE: as relevant

Currently the 2018 ISP assumptions is the main datasource since auxloads were still reported per generator.
Post-2018 (2020, 2022, etc) auxload factors are even more coarse assumptions based on technology type instead of individual generators

The comparison.xlsx file shows how these factors have changed over time in these publications.