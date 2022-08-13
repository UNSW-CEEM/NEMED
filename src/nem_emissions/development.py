# %%

import pandas as pd
from downloader import *
from process import *

cache = "/media/martian/Münster/Thesis_2022/cache"

""" Objectives
Functions to pull generation data from nemosis
Functions to pull data from nemweb
"""
# %%
# Get CDEII table
ctable = download_cdeii_table()

# %%
# Get generation data
# gendata = download_unit_dispatch(
#     start_time="2021/12/27 00:00:00",
#     end_time="2021/12/28 00:00:00",
#     cache=cache,
#     filter_units=None,
#     record="TOTALCLEARED",
# )

# %%
# Total Emissions
start_time = "2021/12/27 00:00:00"
end_time = "2021/12/28 00:00:00"
cache = "/media/martian/Münster/Thesis_2022/cache"
filter_units = None
totemissions = get_total_emissions_by_DI_DUID(
    start_time, end_time, cache, filter_units=["ER01"], filter_regions=["NSW1"]
)
totemissions
# print("Total Emissions calculated as {}".format(totemissions["Total_Emissions"].sum()))
# print("Total Energy calculated as {}".format(totemissions["Energy"].sum()))


# %%
# Aggregate Total Emissions Data
a = get_total_emissions_by_(start_time, end_time, cache, by="year")

a["Energy"]

# %%
for region in a.columns.levels[1]:
    print(region)
    a[("Intensity_Index", region)] = a["Total_Emissions"][region] / a["Energy"][region]


# %%
pd.concat([pd.DataFrame([list(a.columns.levels[1]) * 3]), a])
# %%
a.droplevel(1, axis=1)

# %%
print("Total Emissions calculated as {}".format(result["Total_Emissions"].sum()))

print("Total Energy calculated as {}".format(result["Energy"].sum()))


# %%
