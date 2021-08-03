"""Getting Detailed Hospital Info"""
import pandas as pd
from pdp_utils import PdpData
import os
from tqdm import tqdm


# Section 1 Fetching dhi data
# Fetching data and save it into a CSV file
def fetch_hdi_to_df(hi_df):
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

# global variables
hosp_type = 'Clinic'
# hosp_type = 'Clinic'
# sido = 'Seoul'
sido = 'GyeongGi'
url = 'http://apis.data.go.kr/B551182/medicInsttDetailInfoService/getFacilityInfo'

hi_dir_name = 'HospInfo' + '_' + sido
hi_file_stem = hi_dir_name + '_' + hosp_type
hi_csv_path = os.path.join(hi_dir_name, hi_file_stem + '.csv')

dhi_dir_name = 'HospDetail' + '_' + sido
dhi_file_stem = dhi_dir_name + '_' + hosp_type
dhi_csv_path = os.path.join(dhi_dir_name, dhi_file_stem + '.csv')

# Reading HospInfo (CSV)
hi_df = pd.read_csv(hi_csv_path, encoding='euc-kr')
# print(hi_df.shape, hi_df['ykiho'].nunique())

# Fetching
pdp = PdpData(url, dhi_dir_name, hosp_type)
dhi_df = fetch_hdi_to_df(hi_df)

# Raw data has 3 issues
# * Not all rows have the same fields; some has 'homepage url' and some not
# * Header is repeated in every other row
# * Columns are not ordered properly

# Checking the data integrity
# print(dhi_df.shape)
# print(dhi_df.head(2))

# Dealing with Null rows
# if there are any null rows, it is going to fetch the null data again
# The non-null df will be appended to dhi_final_df
dhi_final_df = pd.DataFrame() # the final df container

# Checking for Null
# back up the original data
dhi_df.to_csv(os.path.join(dhi_dir_name, dhi_file_stem + '_2.csv'), index=False, encoding='euc-kr')
# null check
dhi_df_null = dhi_df.query('addr != addr')
# print(dhi_df_null.head(3))
# print(dhi_df_null.shape[0])

fetching_count = 0
missing_df. = pd.DataFrame()
while fetching_count == 0 or missing_df.shape[0] > 0:
    # Dropping null rows
    dhi_df_nadropped = dhi_df.dropna(subset=['addr'])
    # Appending non-null data to dhi_final_df
    dhi_final_df = pd.concat([dhi_final_df, dhi_df_nadropped])

    # Final Check
    # print(dhi_final_df.nunique())

    # Comparing DHI to HI for any null rows
    all_ykiho = list(dhi_final_df['ykiho'])
    missing_df = hi_df[~(hi_df['ykiho'].isin(all_ykiho))]
    # print(missing_df.shape)

    # Re-Fetching missing data
    pdp = PdpData(url, dhi_dir_name, hosp_type)
    dhi_df = fetch_hdi_to_df(missing_df)
    fetching_count += 1
    sleep(2)

print(dhi_final_df.shape)

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

# Write to a CSV file
dhi_df_comb.to_csv(dhi_csv_path, index=False, encoding='euc-kr')
