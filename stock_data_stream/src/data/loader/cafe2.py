# Copyright (c) vnquant. All rights reserved.
import pandas as pd
import logging as logging
import requests
from datetime import datetime
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import sys
# sys.path.insert(0,'/Users/phamdinhkhanh/Documents/vnquant')
from src import configs
from src.data.loader.proto import DataLoadProto
from src.log.logging import logger
from src.utils import utils

URL_VND = configs.URL_VND
API_VNDIRECT = configs.API_VNDIRECT
URL_CAFE = configs.URL_CAFE
HEADERS = configs.HEADERS
REGEX_PATTERN_PRICE_CHANGE_CAFE = configs.REGEX_PATTERN_PRICE_CHANGE_CAFE
STOCK_COLUMNS_CAFEF = configs.STOCK_COLUMNS_CAFEF
STOCK_COLUMNS_CAFEF_FINAL = configs.STOCK_COLUMNS_CAFEF_FINAL

class DataLoaderCAFE2(DataLoadProto):
    def __init__(self, symbols, start, end, *arg, **karg):
        self.symbols = symbols
        self.start = start
        self.end = end
        super(DataLoaderCAFE2, self).__init__(symbols, start, end)

    def download(self):
        stock_datas = []
        symbols = self.pre_process_symbols()
        logger.info('Start downloading data symbols {} from CAFEF, start: {}, end: {}!'.format(symbols, self.start, self.end))

        for symbol in symbols:
            stock_datas.append(self.download_one(symbol))

        data = pd.concat(stock_datas, axis=0)
        # data = data.sort_index(ascending=False)
        
        data['date'] = pd.to_datetime(data['date'])
        data['date'] = data['date'].dt.strftime('%Y-%m-%d')
        current_time = datetime.now().strftime("%H:%M:%S")
        data['spec_time'] = current_time
        return data

    def download_one(self, symbol):
        start_date = utils.convert_text_dateformat(self.start, origin_type = '%d/%m/%Y', new_type = '%Y-%m-%d')
        end_date = utils.convert_text_dateformat(self.end, origin_type = '%d/%m/%Y', new_type = '%Y-%m-%d')
        delta = datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')
        params = {
            "Symbol": symbol, # symbol of stock
            "StartDate": start_date, # start date
            "EndDate": end_date, # end date
            "PageIndex": 1, # page number
            "PageSize":delta.days + 1 # the size of page
        }
        # Note: We set the size of page equal to the number of days from start_date and end_date
        # and page equal to 1, so that we can get a full data in the time interval from start_date and end_date
        res = requests.get(URL_CAFE, params=params)
        data = res.json()['Data']['Data']
        if not data:
            logger.error(f"Data of the symbol {symbol} is not available")
            return None
        data = pd.DataFrame(data)
        data[['code']] = symbol
        stock_data = data[['code', 'Ngay',
                           'GiaDongCua', 'GiaMoCua', 'GiaCaoNhat', 'GiaThapNhat', 'GiaDieuChinh', 'ThayDoi',
                           'KhoiLuongKhopLenh', 'GiaTriKhopLenh', 'KLThoaThuan', 'GtThoaThuan']].copy()

        stock_data.columns = STOCK_COLUMNS_CAFEF

        stock_change = stock_data['change_str'].str.extract(REGEX_PATTERN_PRICE_CHANGE_CAFE, expand=True)
        stock_change.columns = ['change', 'percent_change']
        stock_data = pd.concat([stock_data, stock_change], axis=1)
        stock_data = stock_data[STOCK_COLUMNS_CAFEF_FINAL]

        list_numeric_columns = [
            'close', 'open', 'high', 'low', 'adjust_price',
            'change', 'percent_change',
            'volume_match', 'value_match', 'volume_reconcile', 'value_reconcile'
        ]
        stock_data[list_numeric_columns] = stock_data[list_numeric_columns].astype(float)
        stock_data['date'] = pd.to_datetime(stock_data['date'], dayfirst=True)
        stock_data.fillna(method='ffill', inplace=True)
        stock_data['total_volume'] = stock_data.volume_match + stock_data.volume_reconcile
        stock_data['total_value'] = stock_data.value_match + stock_data.value_reconcile

        # logger.info('data {} from {} to {} have already cloned!' \
        #              .format(symbol,
        #                      utils.convert_text_dateformat(self.start, origin_type = '%d/%m/%Y', new_type = '%Y-%m-%d'),
        #                      utils.convert_text_dateformat(self.end, origin_type='%d/%m/%Y', new_type='%Y-%m-%d')))
        return stock_data
    
# if __name__ == "__main__":  
#     loader2 = DataLoaderCAFE(symbols=["VND"], start="2024-07-20", end="2024-07-22")
#     print(loader2.download())
