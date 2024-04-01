from urllib.parse import urlencode, quote_plus
import requests as rq
import pandas as pd
import json
import os
import configparser
import tqdm
import logging
from my_log import setup_logger


setup_logger()
logger = logging.getLogger('main')


class PdpData:
    def __init__(self, dir_name, url):
        self.config = configparser.RawConfigParser()
        self.config.read('config.ini')
        self.api_key = self.config['DEFAULT']['ApiKey']
        self.url = url
        self.dir_name = dir_name
        self.num_rows = 280
        self.total_cnt = 0
        self.params = {}
        self.progress_bar = True

        # os.makedirs(os.path.join(os.getcwd(), 'Hosp_Info'), exist_ok=True)
        os.makedirs(os.path.join(os.getcwd(), self.dir_name), exist_ok=True)

    def set_params(self, params):
        """Set params of a request URI"""
        self.params = params

    # new version; default response is json
    def _get_raw_data(self):
        logger.debug(f'url     = {self.url}')
        logger.debug(f'svc_key = {self.api_key}')
        logger.debug(f'params  = {self.params}')

        svc_key = f'?{quote_plus("serviceKey")}={self.api_key}&'
        parsed_params = {}
        for p_key, p_value in self.params.items():
            parsed_params[quote_plus(p_key)] = pr
            n_value
        # encoding
        encoded_params = svc_key + urlencode(parsed_params)
        # reqesting
        request = self.url + encoded_params
        logger.debug(f'request= {request}')
        response = rq.get(request)
        return response

    # add page_no to the params
    def _add_header_to_params(self, page_no):
        params_header = {
            'page': f'{page_no}',
            'perPage': f'{self.num_rows}',
            # 'returnType': 'XML'
        }
        self.params.update(params_header)

    # return a range iterable of total pages
    # firstly, find totalCnt
    # calculate total number of pages from totalCnt
    def _page_range(self):
        self._add_header_to_params(1)
        response = self._get_raw_data()
        resp_dict = json.loads(response.text)
        current_cnt = resp_dict['currentCount']
        total_cnt = resp_dict['totalCount']
        logger.info(f"totalCount={total_cnt}\tcurrentCount={current_cnt}")

        total_pages = range((total_cnt - 1) // current_cnt + 1)
        if self.progress_bar:
            total_pages = tqdm.tqdm(total_pages)

        return total_pages

    # get data from the portal and
    # convert data to df
    def _get_data_to_df(self, page):
        self._add_header_to_params(page)
        response = self._get_raw_data()
        logger.debug(f'\nResponse:\n{response.text}')
        resp_dict = json.loads(response.text)
        logger.debug(f'\nResponse:\n{resp_dict}')
        data_df = pd.DataFrame(resp_dict['data'])
        return data_df

    # get the data and store it
    # self.dir_name is used as a HDF file name as well as 
    # the directory name
    # The HDF file has multple tables named st_key
    def fetch_to_hdf(self, st_key):
        """Fetch data and store it under st_key(store key) in a HDF file"""
        # HDF file path will be DIR_NAME/DIR_NAME.h5
        hdf_file = os.path.join(self.dir_name, self.dir_name + '.h5')
        with pd.HDFStore(hdf_file) as store:
            page_range = self._page_range()
            for page in page_range:
                data_df = self._get_data_to_df(page)
                store.append(st_key, data_df)

    # store the data in csv file format only
    def fetch_to_csv(self, file_name):
        """Fetch data and store it into a csv file"""
        page_range = self._page_range()
        for page in page_range:
            data_df = self._get_data_to_df(page)
            data_df.to_csv(os.path.join(self.dir_name, file_name),
                           mode='a',
                           index=False,
                           encoding='euc-kr')


if __name__ == '__main__':
    # vaccine center location
    url = 'https://api.odcloud.kr/api/15077586/v1/centers'
    dir_name = 'Test2'
    pdp2 = PdpData(dir_name, url)

    # pdp2.get_and_store_csv(dir_name + '_' + 'VaccineLocation')
    pdp2.fetch_to_csv(dir_name + '_' + 'VaccineLocation.csv')
