import pandas as pd
import numpy as np
import os.path

dir_path = "E:\\Datasets\\PublicDataPortal\\서울시\\상권분석\\상권-추정매출\\서울시우리마을가게상권분석서비스(상권-추정매출)_2020\\"
file_name = "서울시우리마을가게상권분석서비스(상권-추정매출)_2020.csv"
file_path = os.path.join(dir_path, file_name)
target_all = "상권추정매출_전업종.csv"
target_all_path = os.path.join(dir_path, target_all)
target_clinic = "상권추정매출_의원.csv"
target_clinic_path = os.path.join(dir_path, target_clinic)
assert os.path.exists(file_path) is True

df_market_rev_all = pd.read_csv(file_path, encoding='euc-kr')
# drop if store number is 0
df_market_rev_all = df_market_rev_all.query('점포수 != 0')
# transform into revenue per store
df_market_rev_all.loc[:, '분기당_매출_금액':'연령대_60_이상_매출_건수'] = df_market_rev_all.loc[:, '분기당_매출_금액':'연령대_60_이상_매출_건수'].transform(
    lambda x: x / df_market_rev_all['점포수']
)

# columns of interest
info_col = ['기준_년_코드', '상권_구분_코드', '상권_코드', '상권_코드_명', '점포수', '서비스_업종_코드', '서비스_업종_코드_명',]
rev_col = ['분기당_매출_금액', '분기당_매출_건수',
           '연령대_20_매출_비율', '연령대_30_매출_비율', '연령대_40_매출_비율', '연령대_50_매출_비율', '연령대_60_이상_매출_비율',
           '연령대_20_매출_건수', '연령대_30_매출_건수', '연령대_40_매출_건수', '연령대_50_매출_건수', '연령대_60_이상_매출_건수',
           '시간대_00~06_매출_비율', '시간대_11~14_매출_비율', '시간대_14~17_매출_비율', '시간대_17~21_매출_비율', '시간대_21~24_매출_비율']

# container to store the data of interest
df_market_rev_q = df_market_rev_all.loc[:, info_col]
# group by market and store type and calculate the year mean (because each has different number of quarters)
df_trans_q_mean = df_market_rev_all.groupby(['상권_코드', '서비스_업종_코드'], as_index=False)['분기당_매출_금액'].transform(np.mean)
df_market_rev_q['분기당_평균_매출_금액'] = df_trans_q_mean
# just one out of 4 quarters
df_market_rev_q = df_market_rev_q.groupby(['상권_코드', '서비스_업종_코드'], as_index=False).nth(0)
# sort by rev
df_market_rev_q_sorted = df_market_rev_q.sort_values(by='분기당_평균_매출_금액', ascending=False)
# print(df_market_type_year.iloc[df_market_type_year['분기당_평균_매출_금액'].idxmax()])
df_market_rev_q_sorted.to_csv(target_all_path, encoding='euc-kr')

# extract clinic revenue
df_market_rev = df_market_rev_all.loc[:, info_col + rev_col]
grp_clinic_market = df_market_rev.groupby(['서비스_업종_코드'])
df_clinic_rev = grp_clinic_market.get_group('CS200006')
# calculate the quarter mean
df_clinic_tr_q_mean = df_clinic_rev.groupby('상권_코드', as_index=False)[rev_col].transform(np.mean)
rev_sum_col = [x + '_mean' for x in rev_col]
df_clinic_rev[rev_sum_col] = df_clinic_tr_q_mean
# drop each quarter data
df_clinic_rev = df_clinic_rev.drop(columns=rev_col)
df_clinic_rev_q = df_clinic_rev.groupby(['상권_코드']).nth(0)
# sort by rev
df_clinic_rev_q_sorted = df_clinic_rev_q.sort_values(by='분기당_매출_금액_mean', ascending=False)
df_clinic_rev_q_sorted.to_csv(target_clinic_path, encoding='euc-kr')



