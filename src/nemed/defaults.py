# AEMO Carbon Dioxide Equivalent Intensity Index (CDEII) source for Available Generators
CDEII_URL = (
    "http://www.nemweb.com.au/reports/CURRENT/CDEII/CO2EII_AVAILABLE_GENERATORS.CSV"
)

# Requests parameters
REQ_URL_REF = 'https://aemo.com.au/'
REQ_URL_HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) \
    AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36',
                   'referer': REQ_URL_REF}

# Mapping of date ranges for AEMO CDEII Summary Files
CDEII_SUMFILES = {'2011': {'year': '2011', 'start': '2011/06/19 00:00:00', 'end': '2011/12/31 00:00:00'},
                  '2012': {'year': '2012', 'start': '2012/01/01 00:00:00', 'end': '2012/12/29 00:00:00'},
                  '2013': {'year': '2013', 'start': '2012/12/30 00:00:00', 'end': '2013/12/28 00:00:00'},
                  '2014_pt1': {'year': '2014', 'start': '2013/12/29 00:00:00', 'end': '2014/05/31 00:00:00'},
                  '2014_pt2': {'year': '2014', 'start': '2014/06/01 00:00:00', 'end': '2014/12/27 00:00:00'},
                  '2015': {'year': '2015', 'start': '28/12/2014 00:00', 'end': '26/12/2015 00:00'},
                  '2016': {'year': '2016', 'start': '27/12/2015 00:00', 'end': '31/12/2016 00:00'},
                  '2017': {'year': '2017', 'start': '01/01/2017 00:00', 'end': '30/12/2017 00:00'},
                  '2018': {'year': '2018', 'start': '2017/12/31 00:00:00', 'end': '2018/12/29 00:00:00'},
                  '2019': {'year': '2019', 'start': '2018/12/30 00:00:00', 'end': '2019/12/28 00:00:00'},
                  '2020': {'year': '2020', 'start': '2019/12/29 00:00:00', 'end': '2020/12/26 00:00:00'},
                  '2021': {'year': '2021', 'start': '2020/12/27 00:00:00', 'end': '2021/12/25 00:00:00'},
}
# Mapping of date formats for AEMO CDEII Summary Files
CDEII_SUMFILES_DTFMT = {'2011': '%Y/%m/%d %H:%M:%S',
                        '2012': '%Y/%m/%d %H:%M:%S',
                        '2013': '%Y/%m/%d %H:%M:%S',
                        '2014_pt1': '%Y/%m/%d %H:%M:%S',
                        '2014_pt2': '%Y/%m/%d %H:%M:%S',
                        '2015': '%d/%m/%Y %H:%M',
                        '2016': '%d/%m/%Y %H:%M',
                        '2017': '%d/%m/%Y %H:%M',
                        '2018': '%Y/%m/%d %H:%M:%S',
                        '2019': '%Y/%m/%d %H:%M:%S',
                        '2020': '%Y/%m/%d %H:%M:%S',
                        '2021': '%Y/%m/%d %H:%M:%S',
}