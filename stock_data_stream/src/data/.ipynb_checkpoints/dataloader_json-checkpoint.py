# Copyright (c) vnquant. All rights reserved.
from typing import Union, Optional
import requests
from datetime import datetime
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import warnings
import json

warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from src import configs
from src.data.loader import DataLoaderVND, DataLoaderCAFE2
from src.log.logging import logger


URL_VND = configs.URL_VND
API_VNDIRECT = configs.API_VNDIRECT
URL_CAFE = configs.URL_CAFE
HEADERS = configs.HEADERS

class DataLoader_json():
    '''
    The DataLoader class is designed to facilitate the downloading and structuring of stock data from different data sources. 
    It supports customization in terms of data sources, time frames, and data formatting.
    '''
    def __init__(self, 
        symbols: Union[str, list], 
        start: Optional[Union[str, datetime]]=None,
        end: Optional[Union[str, datetime]]=None, 
        data_source: str='CAFE', 
        minimal: bool=True,
        table_style: str='levels',
        *arg, **karg):
        '''
        Args:
            - symbols (Union[str, list]): A single stock symbol as a string or multiple stock symbols as a list of strings.
            - start (Optional[Union[str, datetime]], default=None): The start date for the data. Can be a string in the format 'YYYY-MM-DD' or a datetime object.
            - end (Optional[Union[str, datetime]], default=None): The end date for the data. Can be a string in the format 'YYYY-MM-DD' or a datetime object.
            - data_source (str, default='CAFE'): The data source to be used for downloading stock data. Currently supports 'CAFE' and 'VND'.
            - minimal (bool, default=True): If True, returns a minimal set of columns which are important. If False, returns all available columns.
            - table_style (str, default='levels'): The style of the returned table. Options are 'levels', 'prefix', and 'stack'.
        Return:
            - DataFrame: A pandas DataFrame containing the stock data with columns formatted according to the specified table_style.
        '''
        self.symbols = symbols
        self.start = start
        self.end = end
        self.data_source = data_source
        self.minimal = minimal
        self.table_style = table_style
    
    def download(self):
        if str.lower(self.data_source) == 'vnd':
            loader = DataLoaderVND(self.symbols, self.start, self.end)
            stock_data = loader.download()
        else:
            loader = DataLoaderCAFE2(self.symbols, self.start, self.end)
            stock_data = loader.download()
        
        if self.minimal:
            if str.lower(self.data_source) == 'vnd':
                stock_data = stock_data[['code', 'date', 'high', 'low', 'open', 'close', 'adjust_close', 'volume_match', 'value_match']]
            else:
                stock_data = stock_data[['code', 'date', 'high', 'low', 'open', 'close', 'adjust_price', 'volume_match', 'value_match']]
        data = loader.download()
        records = data.to_dict(orient='records')
        result = {record['code']: [] for record in records}

        for record in records:
            result[record['code']].append(record)

        # json_data = json.dumps(result)
        return result
        
