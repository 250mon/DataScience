"""건강보험심사평가원_병원정보서비스"""
import pandas as pd
import os
import itertools
from pdp_utils import PdpData


"""
    'sidoCd': '110000', # 110000:서울  310000:
    'zipCd': '2040' # 2010:종합병원, 2030:병원, 2040:요양병원, 2070:의원
    'clCd': '28', # 01:상급종합병원, 05:전문병원, 11:종합병원, 21:병원, 28:요양병원, 31:의원
    'dgsbjtCd': '04', # 진료과목
"""
sido_cds = {
    'Seoul': '110000',
    'GyeongGi': '310000',
}
cl_cds = {
    'Univ': '01',
    'Spec': '05',
    'Gen': '11',
    'Hosp': '21',
    'NursingHosp': '28',
    'Clinic': '31',
}
# dg_sbjt_cd = ['05', '06', '09', '21']  # 05 정형  06 신경  09 마취  21 재활

# global variables
hi_dir = './Hospital_Info'
url = 'http://apis.data.go.kr/B551182/hospInfoServicev2/getHospBasisList'
num_rows = 500

sidos = ['Seoul', 'GyeongGi']
hosp_types = ['Univ', 'Hosp', 'Clinic']

for sido, h_type in itertools.product(sidos, hosp_types):
    params = {
        'sidoCd': sido_cds[sido],
        # 'clCd': cl_cds[h_type],
    }
    pdp = PdpData(url, hi_dir, num_rows=num_rows)
    pdp.set_params(params)

    # param_col = {'sido': sido, 'type': h_type}
    param_col = {'sido': sido}

    # CSV
    pdp.fetch_to_csv('hosp_info', param_col)

    # HDF
    # pdp.fetch_to_hdf('hosp_info', param_col)

# validate the data
# data_df = pd.read_csv(os.path.join(hi_dir, 'hosp_info.csv'))
# print(data_df.nunique())
# print(data_df.isnull().any())

# validate the data
# data_df = pd.read_hdf(os.path.join(hi_dir, 'hosp_info.h5'), 'data')
# print(data_df.nunique())
# print(data_df.isnull().any())
