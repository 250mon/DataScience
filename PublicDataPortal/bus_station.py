import pandas as pad
import numpy as np
import os


# global variables
# datasets_dir = ''
import pandas as pd

datasets_dir = 'E:\\Datasets\\PublicDataPortal\\서울시\\서울시버스\\'
data_file = '2021년_버스노선별_정류장별_시간대별_승하차_인원_정보(06월).csv'
data_path = os.path.join(datasets_dir, data_file)
coord_file = '서울시버스정류소좌표데이터(2021.01.14.).csv'
coord_path = os.path.join(datasets_dir, coord_file)
target_file = '2021_BusStaion.csv'
target_path = os.path.join(datasets_dir, target_file)

df_bus_station = pd.read_csv(data_path, encoding='euc-kr')
df_coord = pd.read_csv(coord_path, encoding='euc-kr')

df_bus_station_agg = df_bus_station.loc[:, ['표준버스정류장ID']]
df_bus_station_agg.loc[:, '6_18'] = df_bus_station.loc[:, '6시승차총승객수':'18시승차총승객수'].sum(axis=1)
df_bus_station_agg.loc[:, '6_18_dep'] = df_bus_station.loc[:, '6시승차총승객수':'18시승차총승객수':2].sum(axis=1)
df_bus_station_agg.loc[:, '6_18_arr'] = df_bus_station.loc[:, '6시하차총승객수':'18시하차총승객수':2].sum(axis=1)
df_bus_station_agg.loc[:, '7_9_dep'] = df_bus_station.loc[:, '7시승차총승객수':'9시승차총승객수':2].sum(axis=1)
df_bus_station_agg.loc[:, '7_9_arr'] = df_bus_station.loc[:, '7시하차총승객수':'9시하차총승객수':2].sum(axis=1)
df_bus_station_agg.loc[:, '18_21_dep'] = df_bus_station.loc[:, '18시승차총승객수':'21시승차총승객수':2].sum(axis=1)
df_bus_station_agg.loc[:, '18_21_arr'] = df_bus_station.loc[:, '18시하차총승객수':'21시하차총승객수':2].sum(axis=1)
df_bus_station_agg.loc[:, '21_23_dep'] = df_bus_station.loc[:, '21시승차총승객수':'23시승차총승객수':2].sum(axis=1)
df_bus_station_agg.loc[:, '21_23_arr'] = df_bus_station.loc[:, '21시하차총승객수':'23시하차총승객수':2].sum(axis=1)

df_bus_station_final = df_bus_station_agg.groupby('표준버스정류장ID', as_index=False).sum()
df_bus_station_final = pd.merge(df_bus_station_final, df_coord, left_on='표준버스정류장ID', right_on='표준ID')

df_bus_station_final.to_csv(target_path, encoding='euc-kr')