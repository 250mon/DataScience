"""Getting Detailed Hospital Info"""
import pandas as pd
import os
from tqdm import tqdm
import time
from pdp_utils import PdpData


# global variables
# datasets_dir = ''
datasets_dir = 'E:\\Datasets\\PublicDataPortal\\HealthCare\\'

# hosp_type = 'Univ'
hosp_type = 'Hosp'
# hosp_type = 'Clinic'
sido = 'Seoul'
# sido = 'GyeongGi'
url = 'http://apis.data.go.kr/B551182/medicInsttDetailInfoService/getFacilityInfo'

hi_name_stem = 'HospInfo' + '_' + sido
hi_dir_name = datasets_dir + hi_name_stem
hi_csv_path = os.path.join(hi_dir_name, hi_name_stem + '_' + hosp_type + '.csv')

dhi_name_stem = 'HospDetail' + '_' + sido
dhi_dir_name = datasets_dir + dhi_name_stem
dhi_file_stem = dhi_name_stem + '_' + hosp_type
dhi_csv_path = os.path.join(dhi_dir_name, dhi_file_stem + '.csv')
dhi_csv_chk_path = os.path.join(dhi_dir_name, dhi_file_stem + '_chk' + '.csv')

# Section 1 Fetching dhi data
# Fetching data and save it into a CSV file
def fetch_dhi_to_df(hi_df, pdp):
    dhi_df = pd.DataFrame()
    with tqdm(total=hi_df.shape[0]) as pbar:
        for i, ykiho in enumerate(hi_df['ykiho']):
            params = {'ykiho': ykiho}
            pdp.set_params(params)
            # the file path will be dir_name/dir_name.h5
            ret_df = pdp.fetch_to_df(concat_dict={'ykiho': [ykiho]})
            dhi_df = pd.concat([dhi_df, ret_df])
            pbar.update(1)
    return dhi_df

# Reading HospInfo (CSV)
hi_df = pd.read_csv(hi_csv_path, encoding='euc-kr')
# print(hi_df.shape, hi_df['ykiho'].nunique())

# Raw data has 3 issues
# * Not all rows have the same fields; some has 'homepage url' and some not
# * Header is repeated in every other row
# * Columns are not ordered properly

# Dealing with Null rows
# if there are any null rows, it is going to fetch the null data again
# The non-null df will be appended to dhi_final_df
fetching_count = 0
dhi_final_df = pd.DataFrame() # the final df container
missing_df = pd.DataFrame()
ykiho_df = hi_df
while fetching_count < 10:
    # Re-Fetch missing data
    pdp = PdpData(url, dhi_csv_path)
    dhi_df = fetch_dhi_to_df(ykiho_df, pdp)

    # Check for Null
    # null check
    # dhi_df_null = dhi_df.query('addr != addr')
    # print(dhi_df_null.head(3))
    # print(dhi_df_null.shape[0])

    # Remove null and concat the non-null data to the final df
    if dhi_df.shape[0] > 0:
        dhi_df_nadropped = dhi_df.dropna(subset=['addr'])
        # Appending non-null data to dhi_final_df
        dhi_final_df = pd.concat([dhi_final_df, dhi_df_nadropped])
    # Compare DHI to HI for any null rows
    notnull_ykihos = list(dhi_final_df['ykiho'])
    missing_df = hi_df[~(hi_df['ykiho'].isin(notnull_ykihos))]
    # if no null found, break out
    if missing_df.shape[0] == 0:
        break
    ykiho_df = missing_df

    # check point
    dhi_final_df.to_csv('dhi_tmp_final.csv', index=False, mode='w', encoding='euc-kr')
    missing_df.to_csv('dhi_missing.csv', index=False, mode='w', encoding='euc-kr')

    fetching_count += 1
    time.sleep(2)

print(f"dhi_final_df:  {dhi_final_df.shape}")

# Section 2 Merging dhi and hi
# Reordering the columns and removing some of them
dhi_col_incl = [
    "yadmNm",
    "ykiho",
    "clCd",
    "clCdNm",
    "orgTyCd",
    "orgTyCdNm",
    "sidoCd",
    "sidoCdNm",
    "sgguCd",
    "sgguCdNm",
#        "emdongNm",
    "postNo",
    "addr",
#        "telno",
    "stdSickbdCnt",
    "hghrSickbdCnt",
    "isnrSbdCnt",
    "aduChldSprmCnt",
#        "nbySprmCnt",
#        "partumCnt",
    "soprmCnt",
    "emymCnt",
    "ptrmCnt",
#        "chldSprmCnt",
#        "psydeptClsHigSbdCnt",
#        "psydeptClsGnlSbdCnt",
#        "anvirTrrmSbdCnt",
#        "hospUrl",
    "estbDd",
]
dhi_df_sub = pd.DataFrame(dhi_final_df, columns=dhi_col_incl)
dhi_df_sub.reset_index(drop=True, inplace=True)
# print(dhi_df_sub.head(2))

# Merging data
hi_col_incl = [
    'XPos',
    'YPos',
    'yadmNm',
    'drTotCnt',
    'mdeptGdrCnt',
    'mdeptSdrCnt',
    'mdeptIntnCnt',
    'mdeptResdntCnt',
    'ykiho',
]
# Joining Coordinate columns
dhi_df_comb = pd.merge(
    dhi_df_sub,
    hi_df.loc[:, hi_col_incl],
    on='ykiho')

print(f"dhi_df_comb:  {dhi_df_comb.shape}")
print(dhi_df_comb.head(3))

# Write to a CSV file
dhi_df_comb.to_csv(dhi_csv_path, index=False, encoding='euc-kr')
