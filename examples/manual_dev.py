# %%
import os
os.chdir('..\src')
from nemed.process import *
from nemed.downloader import _clean_duplicates
from nemed.downloader import *

from datetime import datetime, timedelta


import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from plotly import tools
import plotly.io as pio
pio.renderers.default = "browser"

os.chdir('..\examples')

# %%
cache = "E:/TEMPCACHE/"
start_time = "2022/01/01 00:00:00" # Start of historical period to collect data for
end_time = "2022/06/02 00:00:00" # End of historical period to collect data for
filter_units = None
filter_regions = None


# %%
# Test dudi mapping
fp = download_duid_mapping()
aux_load = download_iasr_existing_gens()
merged = download_duid_auxload()

# %%
df = get_total_emissions_by_DI_DUID(start_time,end_time,cache,generation_sent_out=True,save_debug_file=True)

# %%
# get_total_emissions_by_DUID



# cdeii_df = download_cdeii_table()
disp_df = download_unit_dispatch(
     start_time, end_time, cache, filter_units=None, record="INITIALMW"
)
# totalcleared = download_unit_dispatch(
#     start_time, end_time, cache, filter_units=None, record="TOTALCLEARED"
# )

# totalcleared = totalcleared.sort_values(by=['Time','DUID'])
disp_df = disp_df.sort_values(by=['Time','DUID'])


# %%
# Figure comparing TotalCleared and InitialMW
# fig = go.Figure()

# fig.add_trace(go.Scatter(x=totalcleared['Time'],y=totalcleared['Dispatch'], \
#     name="totalcleared",mode="lines+markers"))
# fig.add_trace(go.Scatter(x=disp_df['Time'],y=disp_df['Dispatch'], \
#     name="initialMW untouched",mode="lines+markers"))

# fig.show()

# %%
#gendata = get_total_emissions_by_DI_DUID(start_time, end_time, cache, filter_units=None, filter_regions=['NSW1'])
#gendata.to_csv(r'C:\Users\derlu\Desktop\results\checkgenerationdata.csv')
#pd.DataFrame(gendata['Time'].value_counts()).to_csv(r'C:\Users\derlu\Desktop\results\counts.csv')

# %%
# Retrieve Emissions Data - SENT OUT
jan_em_so = get_total_emissions_by_(start_time, end_time, cache, by="day", generation_sent_out=True)
nsw_jan_emso = jan_em_so['Total_Emissions']['NSW1']
nsw_jan_energyso = jan_em_so['Energy']['NSW1']

# %%
# Retrieve Emissions Data - AS GENERATED
jan_em_ag = get_total_emissions_by_(start_time, end_time, cache, by="day", generation_sent_out=False)
nsw_jan_emag = jan_em_ag['Total_Emissions']['NSW1']
nsw_jan_energyag = jan_em_ag['Energy']['NSW1']

#nsw_jan_energy.to_csv(r'C:\Users\derlu\Desktop\results\CDEII\nemosis_dispatchload_w_unitscada.csv')

# %%
# AEMO Reporting
aemo = pd.read_csv('./CO2EII_SUMMARY_RESULTS.csv',header=1,usecols=[6,7,8,9,10])

fil_start_dt = datetime.strptime(start_time,"%Y/%m/%d %H:%M:%S")
fil_end_dt = datetime.strptime(end_time,"%Y/%m/%d %H:%M:%S")

aemo['SETTLEMENTDATE'] = pd.to_datetime(aemo['SETTLEMENTDATE'], format="%d/%m/%Y %H:%M")
sel_aemo = aemo[aemo['SETTLEMENTDATE'].between(fil_start_dt, fil_end_dt)]
sel_aemo_nsw = sel_aemo[sel_aemo['REGIONID']=='NSW1']
sel_aemo_nsw = sel_aemo_nsw.sort_values('SETTLEMENTDATE')
sel_aemo_nem = sel_aemo[sel_aemo['REGIONID']=='NEM']
sel_aemo_nem = sel_aemo_nem.sort_values('SETTLEMENTDATE')

# %%
nemsight = pd.read_csv(r'C:\Users\derlu\Desktop\results\CDEII\nemsight.csv')
nemsight['Time'] = pd.to_datetime(nemsight['Time-ending'], format="%d/%m/%Y %H:%M")
nemsight.set_index('Time',inplace=True)

nemsight_agg = (nemsight.groupby(by=[nemsight.index.year, nemsight.index.month, nemsight.index.day]).agg(np.sum))
nemsight_agg["reconstr_time"] = None
for row in nemsight_agg.index:
    nemsight_agg.loc[row, "reconstr_time"] = datetime(year=row[0], month=row[1], day=row[2], hour=0)
nemsight_agg.set_index("reconstr_time", inplace=True)
nemsight_agg.index.name = "Time"

nemsight_agg['NSW1 Generation'] = nemsight_agg['NSW1 Generation'] * 5/60


# %%
# Energy Comparison Chart

# ndl = pd.read_csv(r'C:\Users\derlu\Desktop\results\CDEII\nemosis_dispatchload.csv')
# ndlws = pd.read_csv(r'C:\Users\derlu\Desktop\results\CDEII\nemosis_dispatchload_w_unitscada.csv')

# ndl2 = ndl.copy()
# ndlws2 = ndlws.copy()
# ndl2['NSW1'] = 0.95 * ndl['NSW1']
# ndlws2['NSW1'] = 0.95 * ndlws['NSW1']




# %%
# Retrieve Merging Tables
# map_df = pd.read_csv(r'E:\PROJECTS\aemo_library\Master_DUID_Mapping.csv')
# aux_load = pd.read_csv(r'E:\PROJECTS\aemo_library\archive\2020-21_IASR\ExistingGenDataSummary.csv')
# aux_load = aux_load[['Generator','Auxiliary Load (%)']]

# # Map Auxillary Load Stat to each DUID
# aux_duid = map_df[['DUID','2021-22-IASR_Generator']].merge(right=aux_load, left_on=['2021-22-IASR_Generator'], \
#     right_on='Generator',how='left')
# aux_duid = aux_duid[['DUID','Auxiliary Load (%)']]



# %%
fig = make_subplots(rows=4, cols=1, shared_xaxes=True, vertical_spacing=0.01,
    specs=[[{"rowspan": 3}],
            [{}],
            [{}],
           [{"rowspan":1}]],
    print_grid=True)
colors = px.colors.qualitative.Dark2
colors_er = px.colors.qualitative.Set2

#fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.02)
fig.update_layout(title="NEM Emissions Data (NEMED) Tool: Comparison of Generation Data against AEMO reporting<br><sub>NSW Region</sub>")
# yaxis={'domain':[0,0.75]}, yaxis2={'domain':[0.75,1]})
fig.update_yaxes(title_text="Total Energy Generation (MWh)", row=1, col=1)
fig.update_yaxes(title_text="Percentage Error <br>wrt. AEMO reporting (%)", row=4, col=1)
fig.update_xaxes(title_text="Date (Day)", row=4, col=1)

fig.add_trace(go.Scatter(x=sel_aemo_nsw['SETTLEMENTDATE'][:-1],y=sel_aemo_nsw['TOTAL_SENT_OUT_ENERGY'],\
    name="AEMO CDEII Report",mode="lines+markers",line_color=colors[7]),row=1,col=1)

fig.add_trace(go.Scatter(x=nsw_jan_energyso.index[:-1],y=nsw_jan_energyso.values,\
    name="Downloaded via NEMOSIS SENT-OUT",mode="lines+markers",line_color=colors[0]),row=1,col=1)
fig.add_trace(go.Scatter(x=nsw_jan_energyag.index[:-1],y=nsw_jan_energyag.values,\
    name="Downloaded via NEMOSIS AS-GENERATED",mode="lines+markers",line_color=colors[1]),row=1,col=1)

# fig.add_trace(go.Scatter(x=nemsight_agg.index[:-1],y=nemsight_agg['NSW1 Generation'],\
#     name="NEMSight 5min DI aggregated",mode="lines+markers",line_color=colors[3]),row=1,col=1)

# Error subplot
error_calc = nsw_jan_energyso.values[:-1] - sel_aemo_nsw['TOTAL_SENT_OUT_ENERGY'][:-1]
error_pct = (error_calc / sel_aemo_nsw['TOTAL_SENT_OUT_ENERGY']) * 100

error_calc_ag = nsw_jan_energyag.values[:-1] - sel_aemo_nsw['TOTAL_SENT_OUT_ENERGY'][:-1]
error_pct2_ag = (error_calc_ag / sel_aemo_nsw['TOTAL_SENT_OUT_ENERGY']) * 100

fig.add_trace(go.Scatter(x=nsw_jan_energyso.index[:-1],y=error_pct,\
    name="Calculation Error (Sent Out)", mode="lines",line_color=colors[0],line_dash='dot'),row=4,col=1)
fig.add_trace(go.Scatter(x=nsw_jan_energyag.index[:-1],y=error_pct2_ag,\
    name="Calculation Error (As generated)", mode="lines",line_color=colors[1],line_dash='dot'),row=4,col=1)

fig.show()
# %%
# %%
# Emissions Comparison Chart
fig = go.Figure()
fig.add_trace(go.Scatter(x=sel_aemo_nsw['SETTLEMENTDATE'],y=sel_aemo_nsw['TOTAL_EMISSIONS'],\
    name="AEMO",mode="lines+markers"))
fig.add_trace(go.Scatter(x=nsw_jan_em.index,y=nsw_jan_em.values,\
    name="calculation",mode="lines+markers"))
fig.show()

# %%
fig = make_subplots(rows=4, cols=1, shared_xaxes=True, vertical_spacing=0.01,
    specs=[[{"rowspan": 3}],
            [{}],
            [{}],
           [{"rowspan":1}]],
    print_grid=True)
colors = px.colors.qualitative.Dark2
colors_er = px.colors.qualitative.Set2

#fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.02)
fig.update_layout(title="NEM Emissions Data (NEMED) Tool: Comparison of Emissions Data against AEMO reporting<br><sub>NSW Region</sub>")
fig.update_yaxes(title_text="Total Emissions (tCO2-e)", row=1, col=1)
fig.update_yaxes(title_text="Percentage Error <br>wrt. AEMO reporting (%)", row=4, col=1)
fig.update_xaxes(title_text="Date (Day)", row=4, col=1)

fig.add_trace(go.Scatter(x=sel_aemo_nsw['SETTLEMENTDATE'][:-1],y=sel_aemo_nsw['TOTAL_EMISSIONS'],\
    name="AEMO CDEII Report",mode="lines+markers",line_color=colors[7]),row=1,col=1)

fig.add_trace(go.Scatter(x=nsw_jan_emso.index[:-1],y=nsw_jan_emso.values,\
    name="Downloaded via NEMOSIS SENT-OUT",mode="lines+markers",line_color=colors[0]),row=1,col=1)
fig.add_trace(go.Scatter(x=nsw_jan_emag.index[:-1],y=nsw_jan_emag.values,\
    name="Downloaded via NEMOSIS AS-GENERATED",mode="lines+markers",line_color=colors[1]),row=1,col=1)

# fig.add_trace(go.Scatter(x=nemsight_agg.index[:-1],y=nemsight_agg['NSW1 Generation'],\
#     name="NEMSight 5min DI aggregated",mode="lines+markers",line_color=colors[3]),row=1,col=1)

# Error subplot
error_calc = nsw_jan_emso.values[:-1] - sel_aemo_nsw['TOTAL_EMISSIONS'][:-1]
error_pct = (error_calc / sel_aemo_nsw['TOTAL_EMISSIONS']) * 100

error_calc_ag = nsw_jan_emag.values[:-1] - sel_aemo_nsw['TOTAL_EMISSIONS'][:-1]
error_pct2_ag = (error_calc_ag / sel_aemo_nsw['TOTAL_EMISSIONS']) * 100

fig.add_trace(go.Scatter(x=nsw_jan_energyso.index[:-1],y=error_pct,\
    name="Calculation Error (Sent Out)", mode="lines",line_color=colors[0],line_dash='dot'),row=4,col=1)
fig.add_trace(go.Scatter(x=nsw_jan_energyag.index[:-1],y=error_pct2_ag,\
    name="Calculation Error (As generated)", mode="lines",line_color=colors[1],line_dash='dot'),row=4,col=1)

fig.show()
# %%
