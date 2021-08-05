from urllib.parse import urlencode, quote_plus
import requests as rq
import pandas as pd
import json
import os
import shutil
import tqdm
import logging


class PdpData2:
    def __init__(self, name, url):
        self.API_KEY =***REMOVED***
        self.url = url
        self.dir_name = name
        self.num_rows = 280
        self.total_cnt = 0
        self.params = {}
        self.logger = self._init_logger(name, logging.WARNING)

        # os.makedirs(os.path.join(os.getcwd(), 'Hosp_Info'), exist_ok=True)
        os.makedirs(os.path.join(os.getcwd(), self.dir_name), exist_ok=True)

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

    # new version; default response is json
    def _get_raw_data(self):
        self.logger.debug(f'url     = {self.url}')
        self.logger.debug(f'svc_key = {self.API_KEY}')
        self.logger.debug(f'params  = {self.params}')

        svc_key = f'?{quote_plus("serviceKey")}={self.API_KEY}&'
        parsed_params = {}
        for p_key, p_value in self.params.items():
            parsed_params[quote_plus(p_key)] = p_value
        # encoding
        encoded_params = svc_key + urlencode(parsed_params)
        # reqesting
        request = self.url + encoded_params
        self.logger.debug(f'request= {request}')
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

    def set_params(self, params):
        self.params = params

    # get the data and store it
    # self.dir_name is used for a HDF file as well as the directory
    # The HDF file has multple tables named file_name
    # ; file_name is the name of each table and
    # each table well be converted to the CSV file of the same name
    def fetch_to_hdf(self, file_name):
        # HDF file path will be DIR_NAME/DIR_NAME.h5
        hdf_file = os.path.join(self.dir_name, self.dir_name + '.h5')
        with pd.HDFStore(hdf_file) as store:
            prev_cnt = 0
            page = 1
            while True:
                self._add_header_to_params(page)
                page += 1
                response = self._get_raw_data()
                # self.logger.debug(f'\nResponse:\n{response.text}')
                resp_dict = json.loads(response.text)
                # get counts and data
                self.logger.debug(f'\nResponse:\n{resp_dict}')
                total_cnt = resp_dict['totalCount']
                current_cnt = resp_dict['currentCount']
                data_df = pd.DataFrame(resp_dict['data'])
                self.logger.info(
                    f"totalCount={total_cnt}\tcurrentCount={current_cnt}")
                # storing
                store.append(file_name, data_df)
                if current_cnt == total_cnt or prev_cnt == current_cnt:
                    break
                else:
                    prev_cnt = current_cnt
        # make CSV files out of each table
        # df = pd.read_hdf(hdf_file, file_name)
        # df.to_csv(os.path.join(self.dir_name, file_name + '.csv'),
                  # index=False,
                  # encoding='euc-kr')

    # store the data in csv file format only
    def fetch_to_csv(self, file_name):
        prev_cnt = 0
        page = 1
        while True:
            self._add_header_to_params(page)
            page += 1
            response = self._get_raw_data()
            # self.logger.debug(f'\nResponse:\n{response.text}')
            resp_dict = json.loads(response.text)
            # get counts and data
            self.logger.debug(f'\nResponse:\n{resp_dict}')
            total_cnt = resp_dict['totalCount']
            current_cnt = resp_dict['currentCount']
            data_df = pd.DataFrame(resp_dict['data'])
            self.logger.info(
                f"totalCount={total_cnt}\tcurrentCount={current_cnt}")
            # storing
            data_df.to_csv(os.path.join(self.dir_name, file_name),
                           mode='a',
                           index=False,
                           encoding='euc-kr')
            if current_cnt == total_cnt or prev_cnt == current_cnt:
                break
            else:
                prev_cnt = current_cnt


if __name__ == '__main__':
    # vaccine center location
    url = 'https://api.odcloud.kr/api/15077586/v1/centers'
    dir_name = 'Test2'
    pdp2 = PdpData2(dir_name, url)

    # pdp2.get_and_store_csv(dir_name + '_' + 'VaccineLocation')
    pdp2.fetch_to_csv(dir_name + '_' + 'VaccineLocation.csv')
