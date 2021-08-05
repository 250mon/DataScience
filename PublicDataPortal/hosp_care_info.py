"""건강보험심사평가원_병원진료정보서비스"""
import pandas as pd
import os
from tqdm import tqdm
from pdp_utils import PdpData


# global variables
# datasets_dir = ''
datasets_dir = 'E:\\Datasets\\PublicDataPortal\\HealthCare\\'

hosp_type = 'Univ'
# hosp_type = 'Clinic'
# sido = 'Seoul'
sido = 'GyeongGi'
url = 'http://apis.data.go.kr/B551182/hospDiagInfoService1/getClinicTop5List1'

hi_name_stem = 'HospInfo' + '_' + sido
hi_dir_name = datasets_dir + hi_name_stem
hi_csv_path = os.path.join(hi_dir_name, hi_name_stem + '_' + hosp_type + '.csv')

hci_name_stem = 'HospCareInfo' + '_' + sido
hci_dir_name = datasets_dir + hci_name_stem
hci_file_stem = hci_name_stem + '_' + hosp_type
hci_csv_path = os.path.join(hci_dir_name, hci_file_stem + '.csv')

# Section 1 Fetching hci data
# Fetching data and save it into a CSV file
def fetch_hci_to_df(hi_df, pdp):
    hci_df = pd.DataFrame()
    with tqdm(total=hi_df.shape[0]) as pbar:
        for i, ykiho in enumerate(hi_df['ykiho']):
            params = {'ykiho': ykiho}
            pdp.set_params(params)
            # the file path will be dir_name/dir_name.h5
            ret_df = pdp.fetch_to_df(concat_dict={'ykiho': [ykiho]})
            hci_df = pd.concat([hci_df, ret_df])
            pbar.update(1)
    return hci_df

# Reading HospInfo (CSV)
hi_df = pd.read_csv(hi_csv_path, encoding='euc-kr')
# print(hi_df.shape, hi_df['ykiho'].nunique())

# Fetching
pdp = PdpData(url, hci_csv_path)
hci_df = fetch_hci_to_df(hi_df, pdp)

# Raw data has 3 issues
# * Not all rows have the same fields; some has 'homepage url' and some not
# * Header is repeated in every other row
# * Columns are not ordered properly

# Checking the data integrity
print(hci_df.shape)
print(hci_df.head(2))
