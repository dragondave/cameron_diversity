#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pathlib import Path
import pandas as pd
import csv


# In[2]:


datapath = Path('../../data/raw/nz_raw_data')


# In[3]:


file = 'NZ 0321_Universities_Workforce_Data_gender_ethnicity 2000-2017.xlsx'


# In[4]:


xl = pd.ExcelFile(datapath/file)


# In[5]:


ethnicity = pd.read_excel(xl, 'Ethnic Group', index_col=[0,1], 
                          skiprows=4, usecols=[1,2,3,4,5,6,8,9])
ethnicity


# In[7]:


coki_format = []
for year in range(2012, 2016):
    for uni,ethn in ethnicity.index.values:
        d = {'grid' : 'none',
                'uni_name' : uni,
                'year' : year,
                'country' : 'New Zealand',
                'region' : 'Oceania',
                'subregion' : 'Australasia',
                'data_name' : 'diversity_nz_doe_{}_august_snapshot_num'.format(ethn.lower()),
                'data_value' : ethnicity.loc[(uni,ethn)][year]
                }
        coki_format.append(d)

for year in [2016,2017]:
    for uni,ethn in ethnicity.index.values:
        d = {'grid' : 'none',
                'uni_name' : uni,
                'year' : year,
                'country' : 'New Zealand',
                'region' : 'Oceania',
                'subregion' : 'Australasia',
                'data_name' : 'diversity_nz_doe_{}_year_end_num'.format(ethn.lower()),
                'data_value' : ethnicity.loc[(uni,ethn)][year]
                }
        coki_format.append(d)        
        
nz_doe_df = pd.DataFrame(coki_format)


# In[9]:


gender_num = pd.read_excel(xl, 'Gender x Full-Part Time', index_col=[0,1,2], 
                          skiprows=4, usecols="B:AT,AV:BA")
gender_num


# In[12]:


coki_format = []
for year in range(2002, 2018):
    for dataitem in gender_num.index.values:
        calc = {'Number of academic staff' : 'num',
                'Full-time Equivalent staff (FTE)' : 'fte'}[dataitem[0]]
        uni = dataitem[1]
        tenure = dataitem[2]
        for group in ['Females', 'Males', 'Total']:
            if year!=2002:
                group_key = group+'.'+str(year-2002)
            else:
                group_key = group
            d = {'grid' : 'none',
                'uni_name' : uni,
                'year' : year,
                'country' : 'New Zealand',
                'region' : 'Oceania',
                'subregion' : 'Australasia',
                'data_name' : 'diversity_nz_doe_academic_{}_{}_{}'.format(group.lower(),
                                                                                  tenure.replace('-', '').lower(),
                                                                                  calc),
                                                                                  
                'data_value' : gender_num.loc[dataitem][group_key]
                }
            coki_format.append(d)

coki_gender_df = pd.DataFrame(coki_format)
nz_doe_df.append(coki_format)


# In[31]:


level_ethn_gender_partfull_num = pd.read_excel(xl, 'Ethnic x Full-Part Time', index_col=[0,1,2], 
                          skiprows=6, usecols="B:AN,AP:BG")
level_ethn_gender_partfull_num


# In[40]:


level_ethn_gender_partfull_num = pd.read_excel(xl, 'Ethnic x Full-Part Time', index_col=[0,1,2], 
                          skiprows=6, usecols="B:AN,AP:BG")
coki_format = []

for dataitem in level_ethn_gender_partfull_num.index.values:
    uni = dataitem[0]
    seniority = dataitem[1]
    ethnicity = dataitem[2]
    for year in range(2012, 2018):    
        for a, fullpart in enumerate(['fulltime', 'parttime', 'totaltime']):
            for group in enumerate(['Females', 'Males', 'Total']):
                colnum = (year-2012)+a
                
                if colnum == 0:
                    group_key = group[1]
                else:
                    group_key = group[1]+'.'+str(colnum)
                d = {'grid' : 'none',
                'uni_name' : uni,
                'year' : year,
                'country' : 'New Zealand',
                'region' : 'Oceania',
                'subregion' : 'Australasia',
                'data_name' : 'diversity_nz_doe_academic_{}_{}_{}_fte'.format(seniority.lower(),
                                                                                  ethnicity.lower(),
                                                                                  group[1].lower()),
                                                                                  
                'data_value' : level_ethn_gender_partfull_num.loc[dataitem][group_key]
                }
                coki_format.append(d)

level_ethn_gender_partfull_num_df = pd.DataFrame(coki_format)
level_ethn_gender_partfull_num_df
nz_doe_df.append(coki_format)


# In[34]:


level_ethn_gender_partfull_num.index.values[0]


# In[19]:


level_ethn_gender_num = pd.read_excel(xl, 'Staff type x Ethnic x Gender', index_col=[0,1,2], 
                          skiprows=5, usecols="B:D,E:P,R:W")
level_ethn_gender_num


# In[26]:


coki_format = []
for year in range(2012, 2018):
    for dataitem in level_ethn_gender_num.index.values:
        uni = dataitem[0]
        seniority = dataitem[1]
        ethnicity = dataitem[2]
        for group in ['Females', 'Males', 'Total']:
            if year!=2012:
                group_key = group+'.'+str(year-2012)
            else:
                group_key = group
            d = {'grid' : 'none',
                'uni_name' : uni,
                'year' : year,
                'country' : 'New Zealand',
                'region' : 'Oceania',
                'subregion' : 'Australasia',
                'data_name' : 'diversity_nz_doe_academic_{}_{}_{}_num'.format(seniority.lower(),
                                                                                  ethnicity.lower(),
                                                                                  group.lower()),
                                                                                  
                'data_value' : level_ethn_gender_num.loc[dataitem][group_key]
                }
            coki_format.append(d)

level_ethn_gender_num_df = pd.DataFrame(coki_format)
nz_doe_df.append(coki_format)


# In[28]:


level_ethn_gender_fte = pd.read_excel(xl, 'Staff type x Ethnic x Gender', index_col=[0,1,2], 
                          skiprows=5, usecols="B:D,X:AI,AK:AP")
coki_format = []
for year in range(2012, 2018):
    for dataitem in level_ethn_gender_fte.index.values:
        uni = dataitem[0]
        seniority = dataitem[1]
        ethnicity = dataitem[2]
        for group in ['Females', 'Males', 'Total']:
            if year!=2012:
                group_key = group+'.'+str(year-2012)
            else:
                group_key = group
            d = {'grid' : 'none',
                'uni_name' : uni,
                'year' : year,
                'country' : 'New Zealand',
                'region' : 'Oceania',
                'subregion' : 'Australasia',
                'data_name' : 'diversity_nz_doe_academic_{}_{}_{}_fte'.format(seniority.lower(),
                                                                                  ethnicity.lower(),
                                                                                  group.lower()),
                                                                                  
                'data_value' : level_ethn_gender_fte.loc[dataitem][group_key]
                }
            coki_format.append(d)

level_ethn_gender_fte_df = pd.DataFrame(coki_format)
nz_doe_df.append(coki_format)


# In[ ]:


nz_doe_df

