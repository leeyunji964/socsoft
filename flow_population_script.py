#!/usr/bin/env python
# coding: utf-8

# In[45]:


import pandas as pd, numpy as np
import sqlalchemy
import psycopg2 as pg2
import geopandas as gpd
import warnings
warnings.filterwarnings('ignore')

from sqlalchemy import create_engine
from urllib.parse import quote_plus as urlquote
from tqdm import trange, notebook
from functools import reduce
import os
from shapely.geometry import Polygon
from fiona.crs import from_string
import gc
import datetime


# In[46]:


try : 
    # db연결
    server = pd.read_csv("./icbpadmin_db.txt", header=None, names=['db','key'])
    product_connection_string = "dbname={dbname} user={user} host={host} password={password} port={port}".format(dbname=server['key'][0],
                                                                                                    user=server['key'][1],
                                                                                                    host=server['key'][2],
                                                                                                    password=server['key'][3],
                                                                                                    port=server['key'][4])
    conn = pg2.connect(product_connection_string)
    cur = conn.cursor()
    
    print('DB 연결 성공')
    
except Exception as e:
    print(e)
    print("DB 연결 실패")


# In[47]:


try : # tdw, tdm crtr_ym 가장 최근 값 불러오기
    tdw_age = """select max(std_ym) from tdw_skt_100_fmnm_age_dynmc_ppltn;"""
    tdm_age = """select max(crtr_ym) from tdm_skt_100_fmnm_age_dynmc_ppltn;"""
    cur.execute(tdw_age, conn); conn.commit()
    cur.execute(tdm_age, conn); conn.commit()
    agew = pd.read_sql(tdw_age, conn); agem = pd.read_sql(tdm_age, conn)
    age_ms = int(agew['max'][0])
    age_mc = int(agem['max'][0])
    
    
    tdw_time = """select max(std_ym) from tdw_skt_100_time_unit_dynmc_ppltn;"""
    tdm_time = """select max(crtr_ym) from tdm_skt_100_time_unit_dynmc_ppltn;"""
    cur.execute(tdw_time, conn); conn.commit()
    cur.execute(tdm_time, conn); conn.commit()
    timew = pd.read_sql(tdw_time, conn); timem = pd.read_sql(tdm_time, conn)
    time_ms = int(timew['max'][0])
    time_mc = int(timem['max'][0])
    
    
    tdw_wkdy = """select max(std_ym) from tdw_skt_100_dweek_accto_dynmc_ppltn;"""
    tdm_wkdy = """select max(crtr_ym) from tdm_skt_100_dweek_accto_dynmc_ppltn;"""
    cur.execute(tdw_wkdy, conn); conn.commit()
    cur.execute(tdm_wkdy, conn); conn.commit()
    wkdyw = pd.read_sql(tdw_wkdy, conn); wkdym = pd.read_sql(tdm_wkdy, conn) 
    week_ms = int(wkdyw['max'][0])
    week_mc = int(wkdym['max'][0])

except Exception as e:
    print(e)
    print('failed')


# In[ ]:


try : # tdw의 std_ym과 tdm의 crtr_ym이 동일하다면 (즉, 이미 코드가 한 번 실행된 적이 있다면)
    if age_ms == age_mc:
        dela_sql = """
        delete from tdm_skt_100_fmnm_age_dynmc_ppltn where crtr_ym = (
        select max(crtr_ym) from tdm_skt_100_fmnm_age_dynmc_ppltn
        where crtr_ym <= replace(left(current_date::varchar,7),'-',''));
        """
        cur.execute(dela_sql, conn); conn.commit()
        print('del duplicated age data')
    
    else : # tdw std_ym > tdm crtr_ym
        pass
    
except Exception as e :
    print(e)
    print('Failed')


# In[ ]:


try : # tdw의 std_ym과 tdm의 crtr_ym이 동일하다면 (즉, 이미 코드가 한 번 실행된 적이 있다면)
    if time_ms == time_mc:
        delt_sql = """
        delete from tdm_skt_100_time_unit_dynmc_ppltn where crtr_ym = (
        select max(crtr_ym) from tdm_skt_100_time_unit_dynmc_ppltn
        where crtr_ym <= replace(left(current_date::varchar,7),'-',''));
        """
        cur.execute(delt_sql, conn); conn.commit()
        print('del duplicated time data')
    
    else : # tdw std_ym > tdm crtr_ym
        pass
    
except Exception as e :
    print(e)
    print('Failed')


# In[ ]:


try : # tdw의 std_ym과 tdm의 crtr_ym이 동일하다면 (즉, 이미 코드가 한 번 실행된 적이 있다면)
    if week_ms == week_mc:
        delw_sql = """
        delete from tdm_skt_100_dweek_accto_dynmc_ppltn where crtr_ym = (
        select max(crtr_ym) from tdm_skt_100_dweek_accto_dynmc_ppltn
        where crtr_ym <= replace(left(current_date::varchar,7),'-',''));
        """
        cur.execute(delw_sql, conn); conn.commit()
        print('del duplicated wkdy data')
    
    else : # tdw std_ym > tdm crtr_ym
        pass
    
except Exception as e :
    print(e)
    print('Failed')


# In[5]:


try :
    # db에 적재되어 있는 데이터 불러오기
    print('데이터 로드 시작')

    a_sql = """select * from tdw_skt_100_fmnm_age_dynmc_ppltn where std_ym = (
    select max(std_ym) from tdw_skt_100_fmnm_age_dynmc_ppltn where left(std_ym,4) <= left(current_date::varchar,4));
    """
    
    t_sql = """select * from tdw_skt_100_time_unit_dynmc_ppltn where std_ym = (
    select max(std_ym) from tdw_skt_100_time_unit_dynmc_ppltn where left(std_ym,4) <= left(current_date::varchar,4));
    """
    
    d_sql = """select * from tdw_skt_100_dweek_accto_dynmc_ppltn where std_ym = (
    select max(std_ym) from tdw_skt_100_dweek_accto_dynmc_ppltn where left(std_ym,4) <= left(current_date::varchar,4));
    """
    
    p_sql = 'select * from tdm_skt_100_dynmc_ppltn_park_grid_5179;' #공원 폴리곤에 걸쳐지는 50*50 격자 shp file
    pk_sql = 'select * from tdm_ctyparkinfo_stddata;' # 공원 정보 데이터
    
    age = pd.read_sql(a_sql, conn); print('성연령대별 데이터 로드 완료')
    time = pd.read_sql(t_sql, conn); print('시간대별 데이터 로드 완료')
    wkdy = pd.read_sql(d_sql, conn); print('요일별 데이터 로드 완료')
    park_info = pd.read_sql(pk_sql, conn)
    pk_info = park_info[['crtr_yr','mng_no','park_nm','park_se','sgg_cd','sgg','emd_cd','emd']]
    
    park_shp = gpd.read_postgis(p_sql, conn, geom_col='geom', crs={'init':'epsg:5179'})
        
    print('유동인구 데이터 로드 성공')
    
except Exception as e:
    print(e)
    print('유동인구 데이터 로드 실패')


# In[6]:


def xy_type(data):
    data['std_ym'] = data['std_ym'].astype(str)
    data['x_coord'] = data['x_coord'].astype(str)
    data['y_coord'] = data['y_coord'].astype(str)


# ## 성연령대별 유동인구

# In[7]:


age_man = age[['std_ym','x_coord','y_coord','man_flow_pop_cnt_10g','man_flow_pop_cnt_20g','man_flow_pop_cnt_30g',
              'man_flow_pop_cnt_40g','man_flow_pop_cnt_50g','man_flow_pop_cnt_60gu']]
age_wman = age[['std_ym','x_coord','y_coord','wman_flow_pop_cnt_10g','wman_flow_pop_cnt_20g','wman_flow_pop_cnt_30g',
              'wman_flow_pop_cnt_40g','wman_flow_pop_cnt_50g','wman_flow_pop_cnt_60gu']]

age_man.insert(3,'sex','남자'); age_man_melt = age_man.melt(id_vars=['std_ym','x_coord','y_coord','sex'])
age_wman.insert(3,'sex','여자'); age_wman_melt = age_wman.melt(id_vars=['std_ym','x_coord','y_coord','sex'])

age_man_melt['variable'] = age_man_melt['variable'].replace('man_flow_pop_cnt_10g','10대')
age_man_melt['variable'] = age_man_melt['variable'].replace('man_flow_pop_cnt_20g','20대')
age_man_melt['variable'] = age_man_melt['variable'].replace('man_flow_pop_cnt_30g','30대')
age_man_melt['variable'] = age_man_melt['variable'].replace('man_flow_pop_cnt_40g','40대')
age_man_melt['variable'] = age_man_melt['variable'].replace('man_flow_pop_cnt_50g','50대')
age_man_melt['variable'] = age_man_melt['variable'].replace('man_flow_pop_cnt_60gu','60대이상')

age_wman_melt['variable'] = age_wman_melt['variable'].replace('wman_flow_pop_cnt_10g','10대')
age_wman_melt['variable'] = age_wman_melt['variable'].replace('wman_flow_pop_cnt_20g','20대')
age_wman_melt['variable'] = age_wman_melt['variable'].replace('wman_flow_pop_cnt_30g','30대')
age_wman_melt['variable'] = age_wman_melt['variable'].replace('wman_flow_pop_cnt_40g','40대')
age_wman_melt['variable'] = age_wman_melt['variable'].replace('wman_flow_pop_cnt_50g','50대')
age_wman_melt['variable'] = age_wman_melt['variable'].replace('wman_flow_pop_cnt_60gu','60대이상')

age_man_melt.rename(columns={'variable':'age','value':'flow_pop'}, inplace=True)
age_wman_melt.rename(columns={'variable':'age','value':'flow_pop'}, inplace=True)

age_melt = pd.concat([age_man_melt, age_wman_melt], axis=0).reset_index(drop=True)
age_mg = gpd.GeoDataFrame(age_melt)

age_mg.head()

print('age data melt end')


# ## 시간대별 유동인구

# In[8]:


time.drop(['sn','block_cd'], axis=1, inplace=True)

time_melt = time.melt(id_vars=['std_ym','x_coord','y_coord'])

time_melt['variable'] = time_melt['variable'].replace('tmst_00','00시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_01','01시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_02','02시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_03','03시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_04','04시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_05','05시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_06','06시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_07','07시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_08','08시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_09','09시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_10','10시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_11','11시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_12','12시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_13','13시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_14','14시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_15','15시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_16','16시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_17','17시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_18','18시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_19','19시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_20','20시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_21','21시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_22','22시간대')
time_melt['variable'] = time_melt['variable'].replace('tmst_23','23시간대')

time_melt.rename(columns={'variable':'time','value':'flow_pop'}, inplace=True)

time_mg = gpd.GeoDataFrame(time_melt)

time_mg.head()

print('time data melt end')


# ## 요일별 유동인구

# In[9]:


wkdy.drop(['sn','block_cd'], axis=1, inplace=True)

wkdy_melt = wkdy.melt(id_vars=['std_ym','x_coord','y_coord'])


wkdy_melt['variable'] = wkdy_melt['variable'].replace('flow_pop_cnt_mon','월요일')
wkdy_melt['variable'] = wkdy_melt['variable'].replace('flow_pop_cnt_tus','화요일')
wkdy_melt['variable'] = wkdy_melt['variable'].replace('flow_pop_cnt_wed','수요일')
wkdy_melt['variable'] = wkdy_melt['variable'].replace('flow_pop_cnt_thu','목요일')
wkdy_melt['variable'] = wkdy_melt['variable'].replace('flow_pop_cnt_fri','금요일')
wkdy_melt['variable'] = wkdy_melt['variable'].replace('flow_pop_cnt_sat','토요일')
wkdy_melt['variable'] = wkdy_melt['variable'].replace('flow_pop_cnt_sun','일요일')

wkdy_melt.rename(columns={'variable':'days','value':'flow_pop'}, inplace=True)

wkdy_mg = gpd.GeoDataFrame(wkdy_melt)

wkdy_mg.head()

print('week data melt end')


# ## 공원별 격자와 join

# In[10]:


gc.collect()


# In[11]:


xy_type(age_mg); xy_type(time_mg); xy_type(wkdy_mg)


# In[12]:


age_mg.rename(columns={'x_coord':'xcoord', 'y_coord':'ycoord'}, inplace=True)
time_mg.rename(columns={'x_coord':'xcoord', 'y_coord':'ycoord'}, inplace=True)
wkdy_mg.rename(columns={'x_coord':'xcoord', 'y_coord':'ycoord'}, inplace=True)

age_mg['xcoord'] = age_mg['xcoord'].astype(float); age_mg['ycoord'] = age_mg['ycoord'].astype(float)
time_mg['xcoord'] = time_mg['xcoord'].astype(float); time_mg['ycoord'] = time_mg['ycoord'].astype(float)
wkdy_mg['xcoord'] = wkdy_mg['xcoord'].astype(float); wkdy_mg['ycoord'] = wkdy_mg['ycoord'].astype(float)


# In[13]:


print('공원 격자와 조인 시작')

park_age = pd.merge(park_shp, age_mg, on=['xcoord','ycoord'], how='left')
park_age.rename(columns={'std_ym':'crtr_ym','sex':'sexdstn','age':'age_lrge','flow_pop':'ppltn_cnt',
                        'sn':'grid_sn'}, inplace=True)

park_age = park_age[['crtr_ym','sexdstn','age_lrge','ppltn_cnt','grid_sn','grid_id','p_id','xcoord','ycoord',
                    'mng_no','park_nm','mdfcn_park_nm','park_se','sgg','emd','geom']]
park_age['crtr_ym'] = age_mg['std_ym']
print('age_park end')

park_time = pd.merge(park_shp, time_mg, on=['xcoord','ycoord'], how='left')
park_time.rename(columns={'sn':'grid_sn','std_ym':'crtr_ym','time':'tmzon','flow_pop':'ppltn_cnt'}, inplace=True)
park_time = park_time[['crtr_ym','tmzon','ppltn_cnt','grid_sn','grid_id','p_id','xcoord','ycoord',
                    'mng_no','park_nm','mdfcn_park_nm','park_se','sgg','emd','geom']]
park_time['crtr_ym'] = time_mg['std_ym']
print('time_park end')


park_wkdy = pd.merge(park_shp, wkdy_mg, on=['xcoord','ycoord'], how='left')
park_wkdy.rename(columns={'sn':'grid_sn', 'std_ym':'crtr_ym','days':'dweek','flow_pop':'ppltn_cnt'}, inplace=True)
park_wkdy = park_wkdy[['crtr_ym','dweek','ppltn_cnt','grid_sn','grid_id','p_id','xcoord','ycoord',
                       'mng_no','park_nm','mdfcn_park_nm','park_se','sgg','emd','geom']]
park_wkdy['crtr_ym'] = wkdy_mg['std_ym']
print('week_park end')


# In[14]:


pa_fin = park_age.drop('geom', axis=1)
pt_fin = park_time.drop('geom', axis=1)
pw_fin = park_wkdy.drop('geom', axis=1)


# In[15]:


# pa_fin.info() #GEodataFrame

paf = pd.merge(pa_fin, pk_info, how='left', on=['mng_no','park_nm','park_se','sgg','emd']).drop('crtr_yr', axis=1)
ptf = pd.merge(pt_fin, pk_info, how='left', on=['mng_no','park_nm','park_se','sgg','emd']).drop('crtr_yr', axis=1)
pwf = pd.merge(pw_fin, pk_info, how='left', on=['mng_no','park_nm','park_se','sgg','emd']).drop('crtr_yr', axis=1)

# display(paf.head(2));display(ptf.head(2));display(pwf.head(2))


# In[17]:


try : 
    
    pa_t = list(paf.itertuples(index=False, name=None))
       
    print('성연령대별 유동인구 DB 적재 시작')
    
    age_query = """
    INSERT INTO tdm_skt_100_fmnm_age_dynmc_ppltn (crtr_ym, sexdstn, age_lrge, ppltn_cnt, grid_sn, grid_id, p_id, xcoord, \
    ycoord, mng_no, park_nm, mdfcn_park_nm, park_se, sgg_cd, sgg, emd_cd, emd) \
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    cur.executemany(age_query, pa_t); conn.commit()

    
    print('성연령대별 유동인구 DB 적재 완료')
    
except Exception as e:
    print(e)
    print('DB 적재 실패')


# In[ ]:


try : 

    pt_t = list(ptf.itertuples(index=False, name=None))
    
    print('시간대별 유동인구 DB 적재 시작')
           
    time_query = """
    INSERT INTO tdm_skt_100_time_unit_dynmc_ppltn (crtr_ym, tmzon, ppltn_cnt, grid_sn, grid_id, p_id, xcoord, \
    ycoord, mng_no, park_nm, mdfcn_park_nm, park_se, sgg_cd, sgg, emd_cd, emd) \
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    cur.executemany(time_query, pt_t); conn.commit()
    
    print('시간대별 유동인구 DB 적재 완료')
    
except Exception as e:
    print(e)
    print('DB 적재 실패')


# In[ ]:


try : 

    pw_t = list(pwf.itertuples(index=False, name=None))
    
    print('요일별 유동인구 DB 적재 시작')

    wkdy_query = """
    INSERT INTO tdm_skt_100_dweek_accto_dynmc_ppltn (crtr_ym, dweek, ppltn_cnt, grid_sn, grid_id, p_id, xcoord, \
    ycoord, mng_no, park_nm, mdfcn_park_nm, park_se, sgg_cd, sgg, emd_cd, emd) \
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.executemany(wkdy_query, pw_t); conn.commit()
    
    print('요일별 유동인구 DB 적재 완료')
    
    conn.close()
    
    print('DB 연결 종료')
    
except Exception as e:
    print(e)
    print('DB 적재 실패')


# In[ ]:


print('유동인구 스크립트 끝')

