# Benchmark Results
**Comparing the results of NEMED to AEMO CDEII Reporting for Regional Sent-Out Generation, Total Emissions and Average Emissions Intensity**<br>

This page contains all region data for `Sent-Out Generation`, `Total Emissions` and `Emissions Intensity` comparisons between CDEII and NEMED.
Overall the error is generally consistent with a few exceptions:
- A few instances in Tasmania for 2016, '17, '18 where Sent-Out generation is underestimated, with error impacting subsequent metrics.
- A short but quite low point (underestimation) in '18 for NSW1, QLD1, VIC1 in Total Emissions, also impacting Emissions Intensity.
- Substational issues with Victoria 2022 data for September-October which coincides with the market change from 30MS to 5MS.
These later two discrepencies are of enough significance that they are clearly visible in the aggregated NEM traces.

```{admonition} All Interactive Plots
Click the images to open the plot as an interactive plotly
```

## NEM
### Energy Sent-Out
```{image} charts_benchmark/energy_NEM.png
:class: full-width
:target: ../_static/html_charts/energy_NEM.html
```

### Total Emissions
```{image} charts_benchmark/emissions_NEM.png
:class: full-width
:target: ../_static/html_charts/emissions_NEM.html
```

### Emissions Intensity
```{image} charts_benchmark/intensity_NEM.png
:class: full-width
:target: ../_static/html_charts/intensity_NEM.html
```


## NSW1
### Energy Sent-Out
```{image} charts_benchmark/energy_NSW1.png
:class: full-width
:target: ../_static/html_charts/energy_NSW1.html
```

### Total Emissions
```{image} charts_benchmark/emissions_NSW1.png
:class: full-width
:target: ../_static/html_charts/emissions_NSW1.html
```

### Emissions Intensity
```{image} charts_benchmark/intensity_NSW1.png
:class: full-width
:target: ../_static/html_charts/intensity_NSW1.html
```


## QLD1
### Energy Sent-Out
```{image} charts_benchmark/energy_QLD1.png
:class: full-width
:target: ../_static/html_charts/energy_QLD1.html
```

### Total Emissions
```{image} charts_benchmark/emissions_QLD1.png
:class: full-width
:target: ../_static/html_charts/emissions_QLD1.html
```

### Emissions Intensity
```{image} charts_benchmark/intensity_QLD1.png
:class: full-width
:target: ../_static/html_charts/intensity_QLD1.html
```


## SA1
### Energy Sent-Out
```{image} charts_benchmark/energy_SA1.png
:class: full-width
:target: ../_static/html_charts/energy_SA1.html
```

### Total Emissions
```{image} charts_benchmark/emissions_SA1.png
:class: full-width
:target: ../_static/html_charts/emissions_SA1.html
```

### Emissions Intensity
```{image} charts_benchmark/intensity_SA1.png
:class: full-width
:target: ../_static/html_charts/intensity_SA1.html
```


## TAS1
### Energy Sent-Out
```{image} charts_benchmark/energy_TAS1.png
:class: full-width
:target: ../_static/html_charts/energy_TAS1.html
```

### Total Emissions
```{image} charts_benchmark/emissions_TAS1.png
:class: full-width
:target: ../_static/html_charts/emissions_TAS1.html
```

### Emissions Intensity
```{image} charts_benchmark/intensity_TAS1.png
:class: full-width
:target: ../_static/html_charts/intensity_TAS1.html
```


## VIC1
```{note} 
:class: full-width
It has been suggested there are data quality issues in CDEII database at Sep-Oct '21 upon change from 30MS to 5MS.
Full error range has been clipped but can be viewed in interactive chart if desired.
```
### Energy Sent-Out
```{image} charts_benchmark/energy_VIC1.png
:class: full-width
:target: ../_static/html_charts/energy_VIC1.html
```

### Total Emissions
```{image} charts_benchmark/emissions_VIC1.png
:class: full-width
:target: ../_static/html_charts/emissions_VIC1.html
```

### Emissions Intensity
```{image} charts_benchmark/intensity_VIC1.png
:class: full-width
:target: ../_static/html_charts/intensity_VIC1.html
```