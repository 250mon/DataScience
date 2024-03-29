#!/usr/bin/env python
# coding: utf-8

# # 예방접종센터 위치정보

# In[1]:


from urllib import parse
from urllib.parse import urlencode, quote_plus
import requests as rq
import pandas as pd
import json
import configparser


# In[2]:


config = configparser.ConfigParser()
config.read('config.ini')
API_KEY = config['DEFAULT']['ApiKey']

def get_data_from_portal(url, params):    
    svc_key = f'?{quote_plus("serviceKey")}={API_KEY}&'
    
    parsed_params = {}
    for p_key, p_value in params.items():
        parsed_params[quote_plus(p_key)] = p_value
    
    encoded_params = svc_key + urlencode(parsed_params)    
    response = rq.get(url + encoded_params)
    return response


# In[3]:


url = 'https://api.odcloud.kr/api/15077586/v1/centers/'
params = {
    'page': '1', 
    'perPage': '10', 
    #'returnType': 'XML',
}
response = get_data_from_portal(url, params)
response


# In[25]:


response.text


# In[14]:


resp_dict = json.loads(response.text)
resp_dict


# In[22]:


total_count = resp_dict['totalCount']
current_count = resp_dict['currentCount']
total_count, current_count


# ## Using dict

# In[23]:


df_data = pd.DataFrame(resp_dict['data'])
df_data


# ## Using pd.json_xxx

# In[24]:


pd.json_normalize(resp_dict, "data")


# In[ ]:




