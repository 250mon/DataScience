"""건강보험심사평가원_병원정보서비스"""
import pandas as pd
import os
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
# datasets_dir = ''
datasets_dir = 'E:\\Datasets\\PublicDataPortal\\HealthCare\\'

# hosp_type = 'Univ'
hosp_type = 'Hosp'
# hosp_type = 'Clinic'
sido = 'Seoul'
# sido = 'GyeongGi'
url = 'http://apis.data.go.kr/B551182/hospInfoService1/getHospBasisList1'
num_rows = 500

hi_name_stem = 'HospInfo' + '_' + sido
hi_dir_name = datasets_dir + hi_name_stem
hi_file_stem = hi_dir_name + '_' + hosp_type
hi_hdf_path = os.path.join(hi_dir_name, hi_file_stem + '.h5')
hi_csv_path = os.path.join(hi_dir_name, hi_file_stem + '.csv')

params = {
    'sidoCd': sido_cds[sido],
#     'zipCd': '2070',
    'clCd': cl_cds[hosp_type],
}
pdp = PdpData(url, hi_csv_path, num_rows=num_rows)
pdp.set_params(params)

# CSV
pdp.fetch_to_csv(pbar=True)

# validate the data
data_df = pd.read_csv(hi_csv_path)
print(data_df.nunique())
print(data_df.isnull().any())
exit(0)

# HDF
# the file path will be dir_name/dir_name.h5
pdp.fetch_to_hdf(st_key='data', pbar=True, min_itemsize={'values':200})

# validate the data
data_df = pd.read_hdf(hi_hdf_path, "data")
print(data_df.nunique())
print(data_df.isnull().any())

# HDF to CSV
data_df.to_csv(hi_csv_path, encoding='euc-kr')
exit(0)
