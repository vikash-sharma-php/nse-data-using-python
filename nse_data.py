import requests
import pandas as pd
import logging
from http.client import HTTPConnection  # py3

log = logging.getLogger('urllib3')
log.setLevel(logging.DEBUG)

# logging from urllib3 to console
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
log.addHandler(ch)

# print statements from `http.client.HTTPConnection` to console/stdout
HTTPConnection.debuglevel = 0

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


class NseIndia:

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.37'}
        self.session = requests.Session()
        self.session.get("https://www.nseindia.com", headers=self.headers)

    def pre_market_data(self):
        pre_market_key = {"NIFTY 50": "NIFTY", "Nifty Bank": "BANKNIFTY", "Emerge": "SME", "Securities in F&O": "FO",
                          "Others": "OTHERS", "All": "ALL"}
        key = "NIFTY 50"  # input
        data = self.session.get(f"https://www.nseindia.com/api/market-data-pre-open?key={pre_market_key[key]}",
                                headers=self.headers).json()["data"]
        new_data = []
        for i in data:
            new_data.append(i["metadata"])
        df = pd.DataFrame(data=new_data,
                          columns=['symbol', 'identifier', 'lastPrice', 'change', 'pChange', 'previousClose'])
        # return list(df['symbol'])
        return df

    def live_market_data(self):
        live_market_index = {
            'Broad Market Indices': ['NIFTY 50', 'NIFTY NEXT 50', 'NIFTY MIDCAP 50', 'NIFTY MIDCAP 100',
                                     'NIFTY MIDCAP 150', 'NIFTY SMALLCAP 50', 'NIFTY SMALLCAP 100',
                                     'NIFTY SMALLCAP 250', 'NIFTY MIDSMALLCAP 400', 'NIFTY 100', 'NIFTY 200'],
            'Sectoral Indices': ["NIFTY AUTO", "NIFTY BANK", "NIFTY ENERGY", "NIFTY FINANCIAL SERVICES",
                                 "NIFTY FINANCIAL SERVICES 25/50", "NIFTY FMCG", "NIFTY IT", "NIFTY MEDIA",
                                 "NIFTY METAL", "NIFTY PHARMA", "NIFTY PSU BANK", "NIFTY REALTY",
                                 "NIFTY PRIVATE BANK"],
            'Others': ['Securities in F&O', 'Permitted to Trade'],
            'Strategy Indices': ['NIFTY DIVIDEND OPPORTUNITIES 50', 'NIFTY50 VALUE 20', 'NIFTY100 QUALITY 30',
                                 'NIFTY50 EQUAL WEIGHT', 'NIFTY100 EQUAL WEIGHT', 'NIFTY100 LOW VOLATILITY 30',
                                 'NIFTY ALPHA 50', 'NIFTY200 QUALITY 30', 'NIFTY ALPHA LOW-VOLATILITY 30',
                                 'NIFTY200 MOMENTUM 30'],
            'Thematic Indices': ['NIFTY COMMODITIES', 'NIFTY INDIA CONSUMPTION', 'NIFTY CPSE', 'NIFTY INFRASTRUCTURE',
                                 'NIFTY MNC', 'NIFTY GROWTH SECTORS 15', 'NIFTY PSE', 'NIFTY SERVICES SECTOR',
                                 'NIFTY100 LIQUID 15', 'NIFTY MIDCAP LIQUID 15']}

        indices = "Others"  # input
        key = "Securities in F&O"  # input
        data = self.session.get(
            f"https://www.nseindia.com/api/equity-stockIndices?index={live_market_index[indices][live_market_index[indices].index(key)].upper().replace(' ', '%20').replace('&', '%26')}",
            headers=self.headers).json()["data"]
        df = pd.DataFrame(data=data)
        return df

    def ohl(self):
        data = self.live_market_data()
        df = pd.DataFrame(data=data,
                          columns=['symbol', 'open', 'dayHigh', 'dayLow', 'lastPrice', 'previousClose',
                                   'change', 'pChange', 'lastUpdateTime', 'nearWKH', 'nearWKL', 'perChange365d',
                                   'perChange30d'])

        # You can rename column name in case you want
        # df = df.rename({'symbol': 'X', 'identifier': 'Y'}, axis='columns')
        # return list(df["symbol"])

        oh = df.loc[df['open'] == df['dayHigh']]
        oh.loc[:, 'Buy/Sell'] = 'Sell'

        ol = df.loc[df['open'] == df['dayLow']]
        ol.loc[:, 'Buy/Sell'] = 'Buy'

        result = oh.append(ol)

        # Changing the column order to what I want
        result = result[['symbol', 'Buy/Sell', 'open', 'dayHigh', 'dayLow', 'lastPrice', 'previousClose',
                         'change', 'pChange', 'lastUpdateTime', 'nearWKH', 'nearWKL', 'perChange365d', 'perChange30d']]

        return result

    def holidays(self):
        holiday = ["clearing", "trading"]
        # key = input(f'Select option {holiday}\n: ')
        key = "trading"  # input
        data = self.session.get(f'https://www.nseindia.com/api/holiday-master?type={holiday[holiday.index(key)]}',
                                headers=self.headers).json()
        df = pd.DataFrame(list(data.values())[0])
        return df


nse = NseIndia()

# print(nse.pre_market_data())
# print(nse.live_market_data())
# print(nse.holidays())
print(nse.ohl())
