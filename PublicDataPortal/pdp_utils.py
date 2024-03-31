import configparser
from urllib.parse import urlencode, quote_plus
import requests as rq
import pandas as pd
import xml.etree.ElementTree as ET
import os
import csv
import shutil
import tqdm
import logging


class PdpData:
    def __init__(self, url, file_path, num_rows=200):
        self.config = configparser.RawConfigParser()
        self.config.read('config.ini')
        self.API_KEY = self.config['DEFAULT']['ApiKey']
        self.url = url
        self.dir_name = os.path.dirname(file_path)
        self.file_stem = os.path.splitext(os.path.basename(file_path))[0]
        self.num_rows = num_rows
        self.total_cnt = 0
        self.params = {}
        # file paths will be DIR_NAME/DIR_NAME.h5 or csv
        os.makedirs(os.path.join(os.getcwd(), self.dir_name), exist_ok=True)
        self.hdf_file_path = os.path.join(self.dir_name, self.file_stem + '.h5')
        self._backup_file(self.hdf_file_path)
        self.csv_file_path = os.path.join(self.dir_name, self.file_stem + '.csv')
        self._backup_file(self.csv_file_path)
        self.raw_csv_file_path = os.path.join(self.dir_name, self.file_stem + '_raw.csv')
        self._backup_file(self.raw_csv_file_path)

        self.logger = self._init_logger(self.file_stem, logging.WARNING)

    def _init_logger(self, name, log_level):
        # create logger
        logger = logging.getLogger('log_' + name)
        logger.setLevel(log_level)
        # create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        # create formatter
        # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        formatter = logging.Formatter('%(levelname)s - %(message)s')
        # add formatter to ch
        ch.setFormatter(formatter)
        # add ch to logger
        logger.addHandler(ch)
        return logger

    # old version; default response is XML
    def _get_raw_data(self, bjson=False):
        self.logger.debug(f'url     = {self.url}')
        self.logger.debug(f'svc_key = {self.API_KEY}')
        self.logger.debug(f'params  = {self.params}')

        svc_key = f'?{quote_plus("ServiceKey")}={self.API_KEY}&'
        parsed_params = {}
        for p_key, p_value in self.params.items():
            parsed_params[quote_plus(p_key)] = p_value
        if bjson is True:
            parsed_params[quote_plus('_type')] = 'json'
        # encoding
        encoded_params = svc_key + urlencode(parsed_params)
        # reqesting
        request = self.url + encoded_params
        self.logger.debug(f'request= {request}')
        response = rq.get(request)
        return response

    # converting XML format data to a DataFrame
    def _xml_to_df(self, response):
        root = ET.fromstring(response.text)
        items_dict = []
        for item_elem in root.iter('item'):
            item_dict = {}
            for nodes in item_elem:
                item_dict.update({nodes.tag: nodes.text})
            items_dict.append(item_dict)

        items_df = pd.DataFrame(items_dict)
        return items_df

    # add page_no to the params
    def _add_header_to_params(self, page_no):
        params_header = {
            'pageNo': f'{page_no}',
            'numOfRows': f'{self.num_rows}',
        }
        self.params.update(params_header)

    # get total_cnt from the portal and set the self.total_cnt
    # by getting data of 1 page and 1 row
    def _set_total_cnt(self):
        """Set self.total_cnt"""
        self._add_header_to_params(1)
        response = self._get_raw_data()
        self.logger.debug(f'set_total_cnt(): response={response} \n')
        self.logger.debug(response.text)
        root = ET.fromstring(response.text)
        for elem in root.iter('totalCount'):
            self.total_cnt = int(elem.text)
        self.logger.info(f"total count {self.total_cnt} ...")

    def _backup_file(self, file_path):
        """Back up the file and, if any, remove an old backup file"""
        dir_name = os.path.dirname(file_path)
        base_name = os.path.basename(file_path)
        # if file_path does not exist, nothing to back up
        if base_name not in os.listdir(dir_name):
            return
        # else, proceed to back-up
        backup_dir = os.path.join(dir_name, 'back-up')
        backup_name = base_name + '.bak'
        backup_path = os.path.join(backup_dir, backup_name)
        os.makedirs(backup_dir, exist_ok=True)
        # if any, remove an old back up file
        if backup_name in os.listdir(backup_dir):
            os.remove(backup_path)
        # back up the file
        shutil.move(file_path, backup_path)

    def set_params(self, params):
        """Set params of a request URI"""
        self.params = params

    def _get_resp_df(self, page):
        """Send a request and get a response / Return a df of the resp"""
        self._add_header_to_params(page + 1)
        response = self._get_raw_data()
        data_df = self._xml_to_df(response)
        return data_df

    def fetch_to_hdf(self,
                     st_key,
                     concat_dict=None,
                     pbar=False,
                     min_itemsize=None):
        """Fetch data and store it under st_key(store key) in a HDF file"""
        # concat_dict is the dict which is concatenated to the response data
        # min_itemsize={'values': 100}
        # self.dir_name is used for a HDF file as well as the directory
        # ;HDF file path will be DIR_NAME/DIR_NAME_name.h5
        self._set_total_cnt()
        # whether or not progress bar is shown
        iterables = range((self.total_cnt - 1) // self.num_rows + 1)
        if pbar:
            iterables = tqdm.tqdm(iterables)
        with pd.HDFStore(self.hdf_file_path) as store:
            for page in iterables:
                data_df = self._get_resp_df(page)
                if concat_dict is not None:
                    concat_df = pd.DataFrame(concat_dict)
                    data_df = pd.concat([data_df, concat_df], axis=1)
                self.logger.info(f"\t{page}")
                store.append(st_key, data_df, min_itemsize=min_itemsize)

    def _remove_multiple_header(self):
        """CSV file could have multiple headers, so remove them"""
        with open(self.raw_csv_file_path, newline='', encoding='euc-kr') as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if i == 0:
                    column_row = row
                    data_df = pd.DataFrame(columns=column_row)
                elif i % (self.num_rows + 1) == 0:
                    if len(row) > len(column_row):
                        column_row = row
                else:
                    df = pd.DataFrame([row], columns=column_row)
                    data_df = pd.concat([data_df, df])
        data_df.to_csv(self.csv_file_path,
                       mode='w',
                       index=False,
                       encoding='euc-kr')

    def fetch_to_csv(self, concat_dict=None, pbar=False):
        """Fetch data and store it into a csv file"""
        self._set_total_cnt()
        # whether or not progress bar is shown
        iterables = range((self.total_cnt - 1) // self.num_rows + 1)
        if pbar:
            iterables = tqdm.tqdm(iterables)
        for i, page in enumerate(iterables):
            data_df = self._get_resp_df(page)
            if concat_dict is not None:
                concat_df = pd.DataFrame(concat_dict)
                data_df = pd.concat([data_df, concat_df], axis=1)
            # because each csv file could have different headers,
            # header(columns) info needs to be updated and accordingly aligned
            current_header = list(data_df.columns)
            if i == 0:
                header = current_header
            else:
                tmp_hd = list(header)
                tmp_hd.extend(x for x in current_header if x not in header)
                header = tmp_hd
                data_df = data_df.reindex(columns=header)
            self.logger.info(f"\t{page}")
            self.logger.debug(f"{data_df}")
            data_df.to_csv(self.raw_csv_file_path,
                           mode='a',
                           index=False,
                           encoding='euc-kr')
        self._remove_multiple_header()

    def fetch_to_df(self, concat_dict=None, pbar=False):
        """Fetch data and returns DataFrame"""
        self._set_total_cnt()
        # whether or not progress bar is shown
        iterables = range((self.total_cnt - 1) // self.num_rows + 1)
        if pbar:
            iterables = tqdm.tqdm(iterables)
        result_df = pd.DataFrame()
        for page in iterables:
            data_df = self._get_resp_df(page)
            if concat_dict is not None:
                concat_df = pd.DataFrame(concat_dict)
                data_df = pd.concat([data_df, concat_df], axis=1)
            self.logger.info(f"\t{page}")
            self.logger.debug(f"{data_df}")
            result_df = pd.concat([result_df, data_df])
        return result_df


if __name__ == '__main__':
    url = 'http://apis.data.go.kr/B551182/hospInfoService1/getHospBasisList1'
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
    pdp = PdpData(url, dir_name, hosp_type)
    pdp.set_params(params)
    print(f"saving {hosp_type} ...")
    # pdp.fetch_to_csv()
    df = pdp.fetch_to_df()
    print(df)
