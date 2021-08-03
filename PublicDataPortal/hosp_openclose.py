#!/usr/bin/env python
# coding: utf-8

# # 건강보험심사평가원_요양기관개폐업정보조회

# In[35]:


from urllib import parse
from urllib.parse import urlencode, quote_plus
import requests as rq
import pandas as pd
import xml.etree.ElementTree as ET


# In[36]:


API_KEY =***REMOVED***

def get_data_from_portal(url, params, bjson=False):    
    svc_key = f'?{quote_plus("ServiceKey")}={API_KEY}&'
    
    parsed_params = {}
    for p_key, p_value in params.items():
        parsed_params[quote_plus(p_key)] = p_value
    if bjson == True:
        parsed_params[quote_plus('_type')] = 'json'
    
    encoded_params = svc_key + urlencode(parsed_params)    
    response = rq.get(url + encoded_params)
    return response


# In[46]:


url = 'http://apis.data.go.kr/B551182/yadmOpCloInfoService/getHospPharmacyOpCloList'
params = {
    'numOfRows': '10', 
    'pageNo': '1',
    'crtrYm': '202106',
    'yadmTp': '1',  # 0: total, 1: hosp, 2: pharm
    'opCloTp': '2',  # 0; both, 1: open, 2: close
}
response = get_data_from_portal(url, params) #, bjson=True)
response


# In[47]:


response.text


# In[48]:


root = ET.fromstring(response.text)
total_count = 0
for elem in root.iter('totalCount'):
    total_count = int(elem.text)
print(f"Total count: {total_count}")


# In[49]:


# for item in root.iter(tag='item'):
#     for nodes in item:
#         print(nodes.tag, nodes.text)

items_dict = []
for item_elem in root.iter('item'):
    item_dict = {}
    for nodes in item_elem:
        item_dict.update({nodes.tag: nodes.text})
    items_dict.append(item_dict)

items_df = pd.DataFrame(items_dict)
items_df.head(10)


# In[105]:


def get_data(num_rows, page_no, **extra_params):
    url = 'http://apis.data.go.kr/B551182/hospInfoService/getHospBasisList'
    params = {
        'pageNo': f'{page_no}', 
        'numOfRows': f'{num_rows}',
    }
    params.update(extra_params)
    
#         'sidoCd': '110000', # Seoul
#         'clCd': '31', # private clinic
#         'dgsbjtCd': '04',
#     }
    response = get_data_from_portal(url, params)
    return response


# In[106]:


def xml_to_df(response):
    root = ET.fromstring(response.text)
    items_dict = []
    for item_elem in root.iter('item'):
        item_dict = {}
        for nodes in item_elem:
            item_dict.update({nodes.tag: nodes.text})
        items_dict.append(item_dict)

    items_df = pd.DataFrame(items_dict)
    return items_df


# In[110]:


def get_total_cnt(**extra_params):
    response = get_data(1, 1, **extra_params)
    root = ET.fromstring(response.text)
    total_count = 0
    for elem in root.iter('totalCount'):
        total_count = int(elem.text)
#     print(f"Total count: {total_count}")
    return total_count


# In[124]:


# main function
# get the data and store it
def get_and_store(total_count, num_rows, file_name, **extra_params):
    with pd.HDFStore(file_name) as store:
        for page in range((total_count-1) // num_rows + 1):
            response = get_data(num_rows, page+1, **extra_params)
            data_df = xml_to_df(response)
            print(f"\t{page}")
            store.append(f"data_df_{page+1}", data_df)
    #         data_df.to_hdf("hosp_info.h5", "data_df", append=True)


# In[125]:


import os
os.makedirs(os.path.join(os.getcwd(), 'Hosp_Info'), exist_ok=True)

num_rows = 200
# cl_cds = [('01', 'Univ'), ('05', 'Spec'), ('11', 'Gen'), ('21', 'Hosp'), ('31', 'Clinic')] # 01 상급종합  05 전문  11 종합  21 병원  31 의원
cl_cds = [('31', 'Clinic')]
# dg_sbjt_cd = ['05', '06', '09', '21']  # 05 정형  06 신경  09 마취  21 재활
for cl_cd, name in cl_cds:
    extra_params = {
            'sidoCd': '110000', # Seoul
            'clCd': f'{cl_cd}', # private clinic
    }
    total_count = get_total_cnt(**extra_params)
    print(f"saving {name} {total_count} ...")
    get_and_store(total_count, num_rows, f"./Hosp_Info/hosp_info_{name}.h5", **extra_params)


# In[129]:


df_clinic = pd.read_hdf("./Hosp_Info/hosp_info_Clinic.h5", "data_df_2")
df_clinic.head()


# In[ ]:




