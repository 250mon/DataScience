import pandas as pd
import numpy as np
import os
import re


# global variables
# datasets_dir = ''
datasets_dir = 'E:\\Datasets\\PublicDataPortal\\'
# file_name = 'HospDetail_GyeongGi\\HospDetail_GyeongGi_Clinic.csv'
file_name = 'HealthCare\\HospDetail_Seoul\\HospDetail_Seoul_Clinic.csv'

file_path = os.path.join(datasets_dir, file_name)
data_df = pd.read_csv(file_path, encoding='euc-kr')

# check for duplicates and nulls
print("Duplicates: ", data_df.ykiho.duplicated().sum())
print("Nulls: ", data_df.isnull().sum())

# subset null coords
df_null_coords = data_df[data_df.loc[:, 'XPos'].isnull()]

# Extract Pure Address Part
addr_regex = re.compile(r'(.+로\s[0-9-]+|.+길\s[0-9-]+)(.*)')
df_null_coord_addrs = df_null_coords['addr'].str.extract(addr_regex)
# print(df_null_coord_addrs.iloc[:, 0:1])

# Geocoding; converting address to coordinates using Kakao
from urllib.parse import urlencode, quote_plus
import requests as rq
import pandas as pd
import json

# input as addr, output as Series of a pair of coordinates
def get_coord(params):
    REST_API_KEY = 'e8457c0e55d458752ae1fc4f48277ca6'
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    header = {"Authorization": f"KakaoAK {REST_API_KEY}"}
    
    parsed_params = {}
    for p_key, p_value in params.items():
        parsed_params[quote_plus(p_key)] = p_value
    
    encoded_params = urlencode(parsed_params)    
    response = rq.get(url, params=encoded_params, headers=header)
    resp_dict = json.loads(response.text)
    if len(resp_dict['documents']) > 0:
        coord = (resp_dict['documents'][0]['x'], resp_dict['documents'][0]['y'])
    else:
        coord = (np.nan, np.nan)
    # print((resp_dict['documents'][0]['x'], resp_dict['documents'][0]['y']))
    return pd.Series(coord)

df_new_coords = df_null_coord_addrs.iloc[:, 0].apply(lambda x: get_coord({'query': x}))
df_new_coords.columns = ['XPos', 'YPos']
df_new_coords = df_new_coords.astype('float64')
# print(df_new_coords)
# print(df_new_coords.dtypes)
df_fix_coords = df_null_coords.copy()    # work around SettingWithCopyWarning
df_fix_coords.loc[:, ['XPos', 'YPos']] = df_new_coords

data_df_final = data_df.dropna(subset=['XPos'])
data_df_final = pd.concat([data_df_final, df_fix_coords])
print("df_final nulls: ", data_df_final.isnull().sum())

file_root, file_ext = os.path.splitext(file_path)
data_df_final.to_csv(file_root + '_fix' + file_ext, encoding='euc-kr')
