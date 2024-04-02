import configparser
from urllib.parse import urlencode, quote_plus
from urllib3 import Timeout, PoolManager
import requests as rq
import pandas as pd
import lxml.etree as etree
import os
from pathlib import PurePath
import shutil
import tqdm
from time import sleep
import logging
from my_log import setup_logger


setup_logger()
logger = logging.getLogger('main')

class CustomResponse:
    def __init__(self, response):
        self.response = response

    @property
    def content(self):
        return self.response.data

    @property
    def text(self):
        return self.response.data.decode('utf-8')

    @property
    def status_code(self):
        return self.response.status


class PdpData:
    def __init__(self, url, dir_name, num_rows=200):
        self.config = configparser.RawConfigParser()
        self.config.read('config.ini')
        self.api_key = self.config['DEFAULT']['ApiKey']
        self.url = url
        self.dir_name = dir_name
        self.num_rows = num_rows
        self.params = {}
        self.progress_bar = True
        self.wait_time = 0
        # file paths will be DIR_NAME/DIR_NAME.h5 or csv
        os.makedirs(os.path.join(os.getcwd(), self.dir_name), exist_ok=True)

    def set_params(self, params):
        """Set params of a request URI"""
        self.params = params

    def _backup_file(self, file_path):
        """Back up the file and, if any, remove an old backup file"""
        if not os.path.exists(file_path):
            return
        
        os.makedirs('back_up', exist_ok=True)
        backup_file = PurePath(file_path).with_suffix('.bak')
        backup_path = PurePath('back_up', backup_file)

        # back up the file
        shutil.move(file_path, backup_path)

    # old version; default response is XML
    def _get_raw_data(self, bjson=False):
        svc_key = f'?{quote_plus("ServiceKey")}={self.api_key}&'
        parsed_params = {}
        for p_key, p_value in self.params.items():
            parsed_params[quote_plus(p_key)] = p_value
        if bjson is True:
            parsed_params[quote_plus('_type')] = 'json'
        # encoding
        encoded_params = svc_key + urlencode(parsed_params)
        # reqesting
        request = self.url + encoded_params
        # logger.debug(f'request= {request}')

        # response = rq.get(request)

        timeout = Timeout(connect=10.0, read=None)
        http = PoolManager(timeout=timeout)
        response = http.request('GET', request)
        response = CustomResponse(response)
        return response

    # add page_no to the params
    def _add_header_to_params(self, page_no):
        params_header = {
            'pageNo': f'{page_no}',
            'numOfRows': f'{self.num_rows}',
        }
        self.params.update(params_header)


    # pretty print a xml element tree
    def _prettyprint(self, element, **kwargs):
        xml = etree.tostring(element, pretty_print=True, **kwargs)
        return xml.decode()

    # return a range iterable of total pages
    # firstly, find totalCnt
    # calculate total number of pages from totalCnt
    def _page_range(self):
        self._add_header_to_params(1)
        logger.debug(f'url     = {self.url}')
        logger.debug(f'svc_key = {self.api_key}')
        logger.debug(f'params  = {self.params}')

        response = self._get_raw_data()
        root = etree.fromstring(response.content) # returns a element class
        logger.debug(self._prettyprint(root, encoding='utf-8'))

        total_cnt = int(root.find(".//totalCount").text)
        logger.info(f"total count {total_cnt} ...")

        total_pages = range((total_cnt - 1) // self.num_rows + 1)
        if self.progress_bar:
            total_pages = tqdm.tqdm(total_pages)

        return total_pages

    # get data from the portal and
    # convert data to df
    def _get_data_to_df(self, page):
        """Send a request and get a response / Return a df of the resp"""
        self._add_header_to_params(page + 1)
        response = self._get_raw_data()
        # logger.debug(self.prettyprint(root, encoding='utf-8'))
        data_df = pd.read_xml(response.content, xpath="//item")
        return data_df

    # get the data and store it
    # self.dir_name is used as a HDF file name as well as 
    # the directory name
    # The HDF file has multple tables named st_key
    def fetch_to_hdf(self,
                     file_name: str or PurePath,
                     param_columns: dict = None):
        """Fetch data and store it under st_key(store key) in a HDF file"""
        # HDF file path will be DIR_NAME/DIR_NAME.h5
        hdf_file = os.path.join(self.dir_name, file_name + '.h5')
        # self._backup_file(hdf_file_path)
        with pd.HDFStore(hdf_file) as store:
            for page in self._page_range():
                data_df = self._get_data_to_df(page)
                if param_columns:
                    data_df[list(param_columns.keys())] = list(param_columns.values())
                store.append('data', data_df)
                sleep(self.wait_time)

    def fetch_to_csv(self,
                     file_name: str or PurePath,
                     param_columns: dict = None):
        """Fetch data and store it into a csv file"""
        # self._backup_file(file_name)
        csv_file = os.path.join(self.dir_name, file_name + '.csv')
        for page in self._page_range():
            data_df = self._get_data_to_df(page)

            if param_columns:
                data_df[list(param_columns.keys())] = list(param_columns.values())

            if page == 0:
                header = True
            else:
                header = False

            data_df.to_csv(csv_file,
                           mode='a',
                           index=False,
                           encoding='utf-8-sig',
                           header=header)
            sleep(self.wait_time)

    def fetch_to_df(self, param_columns: dict = None):
        """Fetch data and returns DataFrame"""
        result_df = pd.DataFrame()
        for page in self._page_range():
            data_df = self._get_data_to_df(page)
            if param_columns:
                data_df[list(param_columns.keys())] = list(param_columns.values())
            result_df = pd.concat([result_df, data_df])
            sleep(self.wait_time)
        return result_df


if __name__ == '__main__':
    url = 'http://apis.data.go.kr/B551182/hospInfoServicev2/getHospBasisList'
    # params
    """
        'sidoCd': '110000', # 110000:서울
        'zipCd': '2040' # 2010:종합병원, 2030:병원, 2040:요양병원, 2070:의원
        'clCd': '28', # 01:상급종합병원, 05:전문병원, 11:종합병원, 21:병원, 28:요양병원, 31:의원
        'dgsbjtCd': '04', # 진료과목
     """
    cl_cds = {
        'Univ': '01',
        'Spec': '05',
        'Gen': '11',
        'Hosp': '21',
        'NursingHosp': '28',
        'Clinic': '31',
    }
    # dg_sbjt_cd = ['05', '06', '09', '21']  # 05 정형  06 신경  09 마취  21 재활

    dir_name = 'Test'
    hosp_type = 'Univ'
    params = {
        'sidoCd': '310000',
        'clCd': cl_cds[hosp_type],
    }
    param_columns = {'sido': 'Gyeongi', 'cl': 'Univ'}
    pdp = PdpData(url, dir_name, 2)
    pdp.set_params(params)
    print(f"saving {hosp_type} ...")
    # df = pdp.fetch_to_df(param_columns)
    # print(df)
    pdp.fetch_to_csv('GG_Univ', param_columns)
    # pdp.fetch_to_hdf('GG_Univ', param_columns)
