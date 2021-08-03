#!/usr/bin/env python
# coding: utf-8

# # 건강보험심사평가원_병원정보서비스

# In[24]:


import pandas as pd
import os
import csv


# In[25]:


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


# In[26]:


hosp_type = 'Univ'
sido = 'GyeongGi'
# hosp_type = 'Clinic'
url = 'http://apis.data.go.kr/B551182/hospInfoService1/getHospBasisList1'
num_rows = 500
hi_dir_name = 'HospInfo' + '_' + sido
hi_file_stem = hi_dir_name + '_' + hosp_type
hi_hdf_name = hi_file_stem + '.h5'
hi_hdf_path = os.path.join(hi_dir_name, hi_hdf_name)
hi_csv_name = hi_file_stem + '.csv'
hi_csv_path = os.path.join(hi_dir_name, hi_csv_name)


# In[27]:


from pdp_utils import PdpData


# In[28]:


params = {
    'sidoCd': sido_cds[sido],
#     'zipCd': '2070',
    'clCd': cl_cds[hosp_type],
}
pdp = PdpData(url, hi_dir_name, hosp_type, num_rows=num_rows)
pdp.set_params(params)
# the file path will be dir_name/dir_name.h5
pdp.fetch_to_hdf(st_key='data', pbar=True, min_itemsize={'values':200})
# pdp.fetch_to_csv(pbar=True)


# # CSV

# ## Remove multiple headers

# In[11]:


data_df = pd.read_csv(hi_csv_path)
data_df.shape


# # HDF

# ## Processing HDF

# In[31]:


data_df = pd.read_hdf(hi_hdf_path, "data")
data_df.shape


# In[32]:


data_df.nunique()


# In[33]:


data_df.isnull().any()


# ## HDF to CSV

# In[34]:


data_df.to_csv(hi_csv_path, encoding='euc-kr')


# In[35]:


temp_df = pd.read_csv(hi_csv_path)
temp_df.shape

