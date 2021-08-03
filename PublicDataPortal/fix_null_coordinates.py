import pandas as pd
import numpy as np
import os


file_path = os.path.join('HospDetail', 'HospDetail_Clinic.csv')

# In[3]:


data_df = pd.read_csv(file_path, encoding='euc-kr')
data_df.head(2)


# ## Drop duplicates of ykiho

# In[4]:


data_df.ykiho.duplicated().sum()


# In[5]:


data_df.isnull().sum()


# In[67]:


df_no_coord = data_df[data_df.loc[:, 'XPos'].isnull()]
df_no_coord = df_no_coord.drop(['XPos', 'YPos'], axis=1)
df_no_coord.shape


# ## Extract Pure Address Part

# In[7]:


addrs = df_no_coord.iloc[:, df_no_coord.columns.get_loc('addr')]
addrs


# In[42]:


addrs.iat[229]


# In[35]:


import re
def extract_addr(addr_str):
    addr_regex = re.compile(r'[가-힣A-Za-z·\d~\-\.]+(로|길)\s([0-9,-]+)(.*)')
#     addr_regex = re.compile(r'([0-9가-힣\s]+)(\b\d+\b)(.*)')
#     addr_regex = re.compile(r'([0-9가-힣\s]+)(\d+(?=\s))(.*)')
    grp_addr = addr_regex.search(addr_str)
    addr_end = grp_addr.group(2)
    addr_end_ix = addr_str.index(' ' + addr_end)
#     addr_extr = copy.copy(addr_str[:addr_end_ix+len(addr_end)])
    addr_extr = addr_str[:addr_end_ix+len(addr_end)+1]
    return addr_extr


# In[36]:


addrs_extr = addrs.map(extract_addr)
addrs_extr


# In[37]:


addrs_extr.iat[59]


# ## Geocoding; converting address to coordinates using Kakao

# In[38]:


from urllib.parse import urlencode, quote_plus
import requests as rq
import pandas as pd
import json


# In[39]:


def get_coord(params):
    REST_API_KEY = 'e8457c0e55d458752ae1fc4f48277ca6'
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    header = {"Authorization": f"KakaoAK {REST_API_KEY}"}
    
    parsed_params = {}
    for p_key, p_value in params.items():
        parsed_params[quote_plus(p_key)] = p_value
    
    encoded_params = urlencode(parsed_params)    
    response = rq.get(url, params=encoded_params, headers=header)
    return response


# In[43]:


coords = []
no_coords = []
for i, addr in enumerate(addrs_extr):
#     print(f'{i} ==> {addr}')
    response = get_coord({'query': addr})
    resp_dict = json.loads(response.text)
    if len(resp_dict['documents']) > 0:
        coords.append((resp_dict['documents'][0]['x'], resp_dict['documents'][0]['y']))
    else:
        coords.append((np.nan, np.nan))
#     print((resp_dict['documents'][0]['x'], resp_dict['documents'][0]['y']))


# In[48]:


coords.insert(229, (np.nan, np.nan))


# In[49]:


df_coords = pd.DataFrame(coords, columns=['XPos', 'YPos'], dtype="float64")
df_coords


# In[55]:


df_coords.iloc[229, :]


# ## Combine the Coords

# In[79]:


df_coords_final = pd.concat([df_no_coord.reset_index(drop=True), df_coords], axis=1)
df_coords_final.set_index(df_no_coord.index, inplace=True)


# In[80]:


data_df_final = data_df.dropna(subset=['XPos'])
data_df_final.isnull().sum(), data_df_final.shape


# In[81]:


data_df.shape, data_df_final.shape, df_coords_final.shape


# In[82]:


data_df.XPos.dtypes, df_coords_final.XPos.dtypes


# In[83]:


data_df_final = pd.concat([data_df_final, df_coords_final])
data_df_final.isnull().sum(), data_df_final.shape


# In[84]:


data_df_final = data_df_final.dropna(subset=['XPos'])
data_df_final.isnull().sum(), data_df_final.shape


# In[85]:


data_df_final.to_csv(os.path.join('HospDetail', 'HospDetail_NursingHosp_fixed.csv'), encoding='euc-kr')


# In[ ]:




