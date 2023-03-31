#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd, numpy as np, geopandas as gpd
import os
from tqdm import tqdm
import operator
import warnings
warnings.filterwarnings(action='ignore')
from sqlalchemy import create_engine
from urllib.parse import quote_plus  as urlquote
import psycopg2 as pg2
from functools import reduce
from shapely.geometry import Polygon, Point
import gc
from fiona.crs import from_string
import datetime


# In[2]:


try : 
    # db 연결

    server=pd.read_csv('./icbpadmin_db.txt', header=None, names=['db','key'])
    product_connection_string = "dbname={dbname} user={user} host={host} password={password} port={port}".format(dbname=server['key'][0],
                                                         user=server['key'][1], host=server['key'][2],
                                                         password=server['key'][3], port=server['key'][4])
    conn=pg2.connect(product_connection_string)
    cur=conn.cursor()
    
    print('공원 DB 연결 성공')

except Exception as e:
    print(e)
    print('공원 DB 연결 실패')


# In[3]:


try :
    # db에 적재되어 있는 데이터 불러오기

    sql = """select * from tdm_ctyparkinfo_stddata where crtr_yr = (select max(crtr_yr) from tdm_ctyparkinfo_stddata
    where crtr_yr <= left(current_date::varchar, 4));"""
    
    park= pd.read_sql(sql, conn)
    
    # 기준년도를 key 값으로 최신 데이터 필터링 해오기
    # 만약 올해 데이터가 없으면 max 값으로 불러오기
    
    print('공원 데이터 로드 성공')
    
except Exception as e:
    print(e)
    print('공원 데이터 로드 실패')


# In[4]:


park.head()


# In[4]:


try :
    if int(park['crtr_yr'].max()) < datetime.datetime.now().year:
        #공원정보의 crtr_yr이 현재 연도보다 작으면, 1년 전 데이터 삭제
        del_p = """
        delete from tdm_ic_ctypark_fac_fclts where crtr_yr = (select max(crtr_yr) from tdm_ic_ctypark_fac_fclts
        where crtr_yr = (left(current_date::varchar,4)::int-1)::varchar);"""
        cur.execute(del_p); conn.commit()

        del_f = """
        delete from tdm_ic_ctypark_fac_idx where crtr_yr = (select max(crtr_yr) from tdm_ic_ctypark_fac_idx
        where crtr_yr = (left(current_date::varchar,4)::int-1)::varchar);"""
        cur.execute(del_f); conn.commit()

        del_c= """
        delete from tdm_ic_ctypark_cmf_idx where crtr_yr = (select max(crtr_yr) from tdm_ic_ctypark_cmf_idx
        where crtr_yr = (left(current_date::varchar,4)::int-1)::varchar);"""
        cur.execute(del_c); conn.commit()

        del_t = """
        delete from tdm_ic_ctypark_pbtrnsp_acsblt_idx where crtr_yr = (select max(crtr_yr) from tdm_ic_ctypark_pbtrnsp_acsblt_idx
        where crtr_yr = (left(current_date::varchar,4)::int-1)::varchar);"""
        cur.execute(del_t); conn.commit()

        del_w = """
        delete from tdm_ic_ctypark_wlkacsblt_idx where crtr_yr = (select max(crtr_yr) from tdm_ic_ctypark_wlkacsblt_idx
        where crtr_yr = (left(current_date::varchar,4)::int-1)::varchar);"""
        cur.execute(del_w); conn.commit()

        del_s = """
        delete from tdm_ic_ctypark_safety_idx where crtr_yr = (select max(crtr_yr) from tdm_ic_ctypark_safety_idx
        where crtr_yr = (left(current_date::varchar,4)::int-1)::varchar);"""
        cur.execute(del_s); conn.commit()
        
        print('deleted previous data from db')
    
    elif int(park['crtr_yr'].max()) == datetime.datetime.now().year:
        # 공원정보의  crtr_yr이 현재 연도와 같으면 이미 코드가 한 번 수행되었다는 뜻이므로 동일년도 결과값 삭제
        del_p = """
        delete from tdm_ic_ctypark_fac_fclts where crtr_yr = (select max(crtr_yr) from tdm_ic_ctypark_fac_fclts
        where crtr_yr = left(current_date::varchar, 4));
        """
        cur.execute(del_p); conn.commit()

        del_f = """
        delete from tdm_ic_ctypark_fac_idx where crtr_yr = (select max(crtr_yr) from tdm_ic_ctypark_fac_idx
        where crtr_yr = left(current_date::varchar, 4));
        """
        cur.execute(del_f); conn.commit()

        del_c= """
        delete from tdm_ic_ctypark_cmf_idx where crtr_yr = (select max(crtr_yr) from tdm_ic_ctypark_cmf_idx
        where crtr_yr = left(current_date::varchar, 4));"""
        cur.execute(del_c); conn.commit()

        del_t = """
        delete from tdm_ic_ctypark_pbtrnsp_acsblt_idx where crtr_yr = (select max(crtr_yr) from tdm_ic_ctypark_pbtrnsp_acsblt_idx
        where crtr_yr = left(current_date::varchar, 4));"""
        cur.execute(del_t); conn.commit()

        del_w = """
        delete from tdm_ic_ctypark_wlkacsblt_idx where crtr_yr = (select max(crtr_yr) from tdm_ic_ctypark_wlkacsblt_idx
        where crtr_yr <= left(current_date::varchar, 4));"""
        cur.execute(del_w); conn.commit()

        del_s = """
        delete from tdm_ic_ctypark_safety_idx where crtr_yr = (select max(crtr_yr) from tdm_ic_ctypark_safety_idx
        where crtr_yr <= left(current_date::varchar, 4));"""
        cur.execute(del_s); conn.commit()
        
        print('deleted current_year(duplicated) data from db')
        
except Exception as e:
    print(e)
    print('delete failed!')


# In[6]:


park_exe = park[['crtr_yr','mng_no','park_nm','park_se','lctn_lotno_addr','lat','lot','park_area','park_hold_fclt_sports_fclt']]
park_play = park[['crtr_yr','mng_no','park_nm','park_se','lctn_lotno_addr','lat','lot','park_area','park_hold_fclt_amsmt_fclt']]
park_facility = park[['crtr_yr','mng_no','park_nm','park_se','lctn_lotno_addr','lat','lot','park_area','park_hold_fclt_cnvnnc_fclt']]
park_culture = park[['crtr_yr','mng_no','park_nm','park_se','lctn_lotno_addr','lat','lot','park_area','park_hold_fclt_cltr_fclt']]
park_etc = park[['crtr_yr','mng_no','park_nm','park_se','lctn_lotno_addr','lat','lot','park_area','park_hold_fclt_etc_fclt']]


# In[7]:


def dt_split_max(data):
    temp_i = 0
    temp_len = 0
    
    for i in range(len(data)):
        if data[data.columns[-1]][i] is not None:
            if temp_len == 0 or len(data[data.columns[-1]][i]) > temp_len:
                temp_i = i
                temp_len = len(data[data.columns[-1]][i])
        else:
            data[data.columns[-1]][i] = '-'
    
    dt = data.copy()
    print(temp_i, temp_len)
    #display(dt.head(3))
    
    max_value = data.iloc[temp_i, -1].split(',')
    
    max_len = len(max_value)
    print(max_value, '\n', max_len)
    #display(pd.DataFrame(dt[dt.columns[-1]].str.split(',',max_len).tolist()).head(3))
    
    dd = pd.DataFrame(data[data.columns[-1]].str.split(',',max_len).tolist())
#     display(df.head(3))
    df_fin = pd.concat([data.iloc[:,:5], dd], axis=1)
#     display(df_fin)
    
    global df_melt
    df_melt = df_fin.melt(id_vars=['crtr_yr','mng_no','park_nm','park_se','lctn_lotno_addr'])
    df_melt.drop('variable', axis=1, inplace=True)
    
    df_melt['value'] = df_melt['value'].replace('-', None)
    df_melt['value'] = df_melt['value'].str.strip()
    df_melt.drop('lctn_lotno_addr', axis=1, inplace=True)
    df_melt.sort_values('park_nm', inplace=True)
    df_melt.reset_index(inplace=True, drop=True)
    df_melt = df_melt.replace(np.nan, None)
    df_melt = df_melt.replace('', None)


# In[8]:


dt_split_max(park_exe); pe_melt = df_melt
dt_split_max(park_play); pp_melt = df_melt
dt_split_max(park_facility); pf_melt = df_melt
dt_split_max(park_culture); pc_melt = df_melt
dt_split_max(park_etc); pt_melt = df_melt

del park_exe, park_play, park_facility, park_culture, park_etc, df_melt

gc.collect()


# In[9]:


for i in range(len(pe_melt)):
    if pe_melt['value'][i] is not None:
        pe_melt['value'][i] = pe_melt['value'][i].replace('다리쭉뻗치기','다리뻗치기')
        pe_melt['value'][i] = pe_melt['value'][i].replace('인라인스케이트장(X-게임장)','X-게임장')
        pe_melt['value'][i] = pe_melt['value'][i].replace('X게임장','X-게임장')
        pe_melt['value'][i] = pe_melt['value'][i].replace('인라인장','인라인스케이트장')
        pe_melt['value'][i] = pe_melt['value'][i].replace('하늘걷기 등','하늘걷기')
        pe_melt['value'][i] = pe_melt['value'][i].replace('베드민턴장','배드민턴장')
                
        
        if pe_melt['value'][i].startswith('야외체육시설'):
            pe_melt['value'][i] = '야외체육시설'

        elif pe_melt['value'][i].startswith('운동'):
            pe_melt['value'][i] = '운동기구'

        elif pe_melt['value'][i].startswith('배드민턴'):
            pe_melt['value'][i] = '배드민턴장'

        elif pe_melt['value'][i].startswith('족구'):
            pe_melt['value'][i] = '족구장'

        elif pe_melt['value'][i].startswith('풋살'):
            pe_melt['value'][i] = '풋살경기장'

        elif pe_melt['value'][i].startswith('체력단련'):
            pe_melt['value'][i] = '체력단련시설'

        elif pe_melt['value'][i].endswith(' 등'):
            pe_melt['value'][i] = pe_melt['value'][i][:-2]

        else:
            pass
    else:
        pass


# In[10]:


# origin_type = 기존 데이터 셋에서 속해있던 시설 타입
# new_type = 기준에 의해 재정의한 시설 타입

pe_melt['original_type'] = '운동시설'
pe_melt['new_type'] = '운동시설'
pe_melt['view'] = '시설 편의성'

for i in range(len(pe_melt)):
    if pe_melt['value'][i] is not None:
        if (pe_melt['value'][i] == '자전거 대여') | (pe_melt['value'][i] == '자전거도로'):
            pe_melt['new_type'][i] = '편익시설'

        elif pe_melt['value'][i] == '썰매장':
            pe_melt['new_type'][i] = '유희시설'

        elif pe_melt['value'][i] == '산책로':
            pe_melt['new_type'][i] = '유희시설'
            pe_melt['view'][i] = '쾌적성'

        else:
            pass
        
    else:
        pe_melt['new_type'][i] = None
        pe_melt['view'][i] = None


# In[11]:


# 유희시설

for i in range(len(pp_melt)):
    if pp_melt['value'][i] is not None:
        pp_melt['value'][i] = pp_melt['value'][i].replace('어린이 놀이시설','어린이놀이시설')
        pp_melt['value'][i] = pp_melt['value'][i].replace('짚와이어','짚라인')
        pp_melt['value'][i] = pp_melt['value'][i].replace('자가발전흔들놀이(고래','자가발전흔들놀이')

        if pp_melt['value'][i].startswith('모래'):
            pp_melt['value'][i] = '모래놀이시설'

        elif pp_melt['value'][i].startswith('물'):
            pp_melt['value'][i] = '물놀이시설'

        elif pp_melt['value'][i].startswith('시소'):
            pp_melt['value'][i] = '시소'

        elif pp_melt['value'][i].startswith('회전'):
            pp_melt['value'][i] = '회전놀이기구'

        elif pp_melt['value'][i].startswith('흔들'):
            pp_melt['value'][i] = '흔들놀이기구'    

        elif pp_melt['value'][i].startswith('오르'):
            pp_melt['value'][i] = '오르는기구'

        elif pp_melt['value'][i].endswith('벤치'):
            pp_melt['value'][i] = '벤치'

        elif pp_melt['value'][i].endswith('캠핑장'):
            pp_melt['value'][i] = '캠핑장'

        elif pp_melt['value'][i].endswith(' 등'):
            pp_melt['value'][i] = pp_melt['value'][i][:-2]
        
        elif pp_melt['value'][i] == '':
            pp_melt['value'][i] = None

        else:
            pass    


# In[12]:


pp_melt ['original_type'] = '유희시설'
pp_melt ['new_type'] = '유희시설'
pp_melt ['view'] = ''


for i in range(len(pp_melt)):
    if (pp_melt['value'][i]=='3단철봉') | (pp_melt['value'][i]=='늑목')    | (pp_melt['value'][i]=='배드민턴장') | (pp_melt['value'][i]=='철봉'):
        pp_melt['new_type'][i] = '운동시설'
        
    elif (pp_melt['value'][i]=='벤치') | (pp_melt['value'][i]=='안내판') |     (pp_melt['value'][i]=='자전거 대여') | (pp_melt['value'][i]=='파고라')|     (pp_melt['value'][i]=='해수온수족욕장') | (pp_melt['value'][i]=='편익시설'):
        pp_melt['new_type'][i] = '편익시설'
    
    elif (pp_melt['value'][i] == '교통교육장') | (pp_melt['value'][i] == '생태학습원')|    (pp_melt['value'][i]=='자연체험학습장') | (pp_melt['value'][i] == '숲체험장'):
        pp_melt['new_type'][i] = '교양시설'
    
    elif pp_melt['value'][i] == '금속제울타리':
        pp_melt['new_type'][i] = '기타시설'
    
    else:
        pass


# In[13]:


#  계류형 수경시설, 금속제울타리, 바닥분수, 수경시설인공폭포, 숲속의집, 숲체험장 => 쾌적성
# 생태학습원 => 편의성(교육시설)'

        
for i in range(len(pp_melt)):
    if pp_melt['value'][i] is not None:
        if (pp_melt['value'][i] == '계류형 수경시설') | (pp_melt['value'][i] == '금속제울타리') |        (pp_melt['value'][i] == '바닥분수') | (pp_melt['value'][i] == '수경시설인공폭포'):
            pp_melt['view'][i] = '쾌적성'

        elif (pp_melt['value'][i] == '교통교육장') | (pp_melt['value'][i] == '금속제울타리')|        (pp_melt['value'][i] == '생태학습장'):
            pp_melt['view'][i] = '기타'

        else:
            pp_melt['view'][i] = '시설 편의성'
            
    else:
        pp_melt['new_type'][i] = None
        pp_melt['view'][i] = None  


# In[14]:


idex = pp_melt[pp_melt['value'] == '호랑이)'].index

pp_melt.drop(idex, axis=0, inplace=True)
pp_melt.reset_index(inplace=True, drop=True)
pp_melt


# In[15]:


# 편익시설

for i in range(len(pf_melt)):
    if pf_melt['value'][i] is not None:
        pf_melt['value'][i] = pf_melt['value'][i].replace('움수전','음수전')
        pf_melt['value'][i] = pf_melt['value'][i].replace('까페테리아','카페테리아')
        pf_melt['value'][i] = pf_melt['value'][i].replace('서구국민체육센터','체육센터')

        if pf_melt['value'][i].startswith('음수'):
            pf_melt['value'][i] = '음수시설'

        elif pf_melt['value'][i].endswith(' 등'):
            pf_melt['value'][i] = pf_melt['value'][i][:-2]

        else:
            pass
        
    else:
        pass


# In[16]:


pf_melt['original_type'] = '편익시설'
pf_melt['new_type'] = '편익시설'
pf_melt['view'] = '시설 편의성'


for j in range(len(pf_melt)):
    if pf_melt['value'][j] is None:
        pf_melt['new_type'][j] = None
        pf_melt['view'][j] = None
        
    else:
        if pf_melt['value'][j] == '체육센터':
            pf_melt['new_type'][j] = '운동시설'
        
        elif (pf_melt['value'][j] == '무장애나눔길'):
            pf_melt['new_type'][j] = '기타시설' 
            pf_melt['view'][j] = '시설 편의성'

        elif pf_melt['value'][j] == '관리사무소':
            pf_melt['view'][j] = '시설 편의성'

        elif pf_melt['value'][j] == '도서관':
            pf_melt['new_type'][j] = '교양시설'

        else:
            pass


# In[17]:


# 교양시설

for i in range(len(pc_melt)):
    if pc_melt['value'][i] is not None:
        pc_melt['value'][i] = pc_melt['value'][i].replace('야외공연장','야외무대')
        pc_melt['value'][i] = pc_melt['value'][i].replace('어린이교통교육관','어린이교통안전교육장')

        if (pc_melt['value'][i].endswith('도서관')) & (pc_melt['value'][i].startswith('숲')):
            pc_melt['value'][i] = '숲(속)도서관'

        elif pc_melt['value'][i].startswith('야외'):
            pc_melt['value'][i] = '야외무대'

        elif pc_melt['value'][i].startswith('생태'):
            pc_melt['value'][i] = '생태교육/전시/학습장'

        elif pc_melt['value'][i].endswith('숲체험원'):
            pc_melt['value'][i] = '숲체험원'

        elif pc_melt['value'][i].endswith(' 등'):
            pc_melt['value'][i] = pc_melt['value'][i][:-2]

        else:
            pass  
    else:
        pass


# In[18]:


pc_melt['original_type'] = '교양시설'
pc_melt['new_type'] = '교양시설'
pc_melt['view'] = ''


for i in range(len(pc_melt)):
    if pc_melt['value'][i] is not None:
        if (pc_melt['value'][i] == '다례원') | (pc_melt['value'][i] == '습지원') |        (pc_melt['value'][i] == '야생초화원') | (pc_melt['value'][i] == '어린이동물원') |        (pc_melt['value'][i] == '야외무대') | (pc_melt['value'][i] == '보조공연장') |        (pc_melt['value'][i] == '장미정원')| (pc_melt['value'][i] == '오픈스쿨무대') |        (pc_melt['value'][i] == '대공연장'):
            pc_melt['new_type'][i] = '유희시설'
        
        elif (pc_melt['value'][i] == '관리사무소') |        (pc_melt['value'][i] == '정자') | (pc_melt['value'][i] == '초정') | (pc_melt['value'][i] == '파고라'):
            pc_melt['new_type'][i] = '편익시설'

        elif (pc_melt['value'][i] == '염전보존지역') | (pc_melt['value'][i] == '병영 11동')|        (pc_melt['value'][i] == '경로당(부지)'):
            pc_melt['new_type'][i] = '기타시설'

        else:
            pass
    
    else:
        pass


# In[105]:


# 쾌적성 : 관리사무소, 다례원, 습지원, 야생초화원, 온실, 장미정원

# 편의성 : 대공연장, 도서관, 숲(속)도서관, 보조공연장, 오픈스쿨무대, 파고라, 야외무대, 정자, 초정, 달동네박물관,  아포전시장, 전통정자, 숲체험원', 
#'목재문화체험장', 생태교육/전시/학습장', 어린이교통안전교육장', '원형탐조대', 조류관찰대

# 기타 : 염전보존지역, 갯벌문화관, 경로당(부지), 기념비, 병영 11동, 콜롬비아참전용사기념비, 한미수교기념비, '어린이동물원',
# '영종역사문화관', '월미문화관', '유물전시관', 이민사박물관', 자연학습원','환경미래관'



for i in range(len(pc_melt)):
    if pc_melt['value'][i] is not None:
        if (pc_melt['value'][i] == '관리사무소') | (pc_melt['value'][i] == '다례원') |        (pc_melt['value'][i] == '습지원') | (pc_melt['value'][i] == '야생초화원') |        (pc_melt['value'][i] == '온실') | (pc_melt['value'][i] == '장미정원'):
            pc_melt['view'][i] = '쾌적성'
        
        elif (pc_melt['value'][i] == '대공연장') | (pc_melt['value'][i] == '도서관') |        (pc_melt['value'][i] == '숲(속)도서관') | (pc_melt['value'][i] == '오픈스쿨무대') |        (pc_melt['value'][i] == '파고라') | (pc_melt['value'][i] == '보조공연장') |        (pc_melt['value'][i] == '야외무대') | (pc_melt['value'][i] == '정자') |        (pc_melt['value'][i] == '초정') | (pc_melt['value'][i] == '달동네박물관')|        (pc_melt['value'][i] == '아포전시장') | (pc_melt['value'][i] == '전통정자')|        (pc_melt['value'][i] == '숲체험원') | (pc_melt['value'][i] == '목재문화체험장')|        (pc_melt['value'][i] == '생태교육/전시/학습장') | (pc_melt['value'][i] == '어린이교통안전교육장')|        (pc_melt['value'][i] == '원형탐조대') | (pc_melt['value'][i] == '조류관찰대'):
            pc_melt['view'][i] = '시설 편의성'

        else:
            pc_melt['view'][i] = '기타'
            
    else:
        pc_melt['new_type'][i] = None
        pc_melt['view'][i] = None


# ### 기타시설

# In[106]:


for i in range(len(pt_melt)):
    if pt_melt['value'][i] is not None:
        pt_melt['value'][i] = pt_melt['value'][i].replace('물놀이터 또랑','물놀이터')
        pt_melt['value'][i] = pt_melt['value'][i].replace('안내소','안내사무소')
        pt_melt['value'][i] = pt_melt['value'][i].replace('공원안내사무소','안내사무소')
        pt_melt['value'][i] = pt_melt['value'][i].replace('공원관리사무실','관리사무소')
        pt_melt['value'][i] = pt_melt['value'][i].replace('수변공연스탠드','수변스탠드')
        pt_melt['value'][i] = pt_melt['value'][i].replace('경비실','관리사무소')
        pt_melt['value'][i] = pt_melt['value'][i].replace('자건거보관대','자전거 보관대')


        if pt_melt['value'][i].startswith('관리사무') | pt_melt['value'][i].startswith('공원관리'):
            pt_melt['value'][i] = '관리사무소'

        elif pt_melt['value'][i].startswith('안내사무'):
            pt_melt['value'][i] = '안내사무소'

        elif pt_melt['value'][i].endswith(' 등'):
            pt_melt['value'][i] = pt_melt['value'][i][:-2]

        elif pt_melt['value'][i].endswith('데크'):
            pt_melt['value'][i] = '데크'

        else:
            pass
        
    else:
        pass


# In[107]:


pt_melt['original_type'] = '기타시설'
pt_melt['new_type'] = '기타시설'
pt_melt['view'] = ''


for i in range(len(pt_melt)):
    if pt_melt['value'][i] is not None:
        if pt_melt['value'][i] == '구민운동장':
            pt_melt['new_type'][i] = '운동시설'
    
        elif (pt_melt['value'][i] == '오줌싸개동상') | (pt_melt['value'][i] == '누각') |        (pt_melt['value'][i] == '수목정보센터') | (pt_melt['value'][i] == '전망대') |        (pt_melt['value'][i] == '현충탑') | (pt_melt['value'][i] == '염전체험장') |        (pt_melt['value'][i] == '어린이교통공원'):
            pt_melt['new_type'][i] = '교양시설'

        elif (pt_melt['value'][i] == '경로당') | (pt_melt['value'][i] == '관리사무소') |        (pt_melt['value'][i] == '그네의자') | (pt_melt['value'][i] == '기념광장')|        (pt_melt['value'][i] == '데크') | (pt_melt['value'][i] == '민방위급수시설') |        (pt_melt['value'][i] == '목교') | (pt_melt['value'][i] == '안내판') |        (pt_melt['value'][i] == '육교') | (pt_melt['value'][i] == '파고라') |        (pt_melt['value'][i] == '해수족욕장') | (pt_melt['value'][i] == '평의자') |        (pt_melt['value'][i] == '안내사무소') | (pt_melt['value'][i] == '자전거 보관대') |        (pt_melt['value'][i] == '자전거대여소') | (pt_melt['value'][i] == '앉음벽') |        (pt_melt['value'][i] == '벤치')| (pt_melt['value'][i] == '전망데크'):
            pt_melt['new_type'][i] = '편익시설'

        elif (pt_melt['value'][i] == '바닥분수') | (pt_melt['value'][i] == '경관폭포')|        (pt_melt['value'][i] == '계류') | (pt_melt['value'][i] == '고래분수')|        (pt_melt['value'][i] == '고사분수') | (pt_melt['value'][i] == '물놀이터')|        (pt_melt['value'][i] == '반려동물놀이터') | (pt_melt['value'][i] == '발원분수')|        (pt_melt['value'][i] == '벽천분수')|(pt_melt['value'][i] == '분수') |        (pt_melt['value'][i] == '분수대')| (pt_melt['value'][i] == '장미원') |        (pt_melt['value'][i] == '장미정원')|        (pt_melt['value'][i] == '전통정원') | (pt_melt['value'][i] == '초화원')|        (pt_melt['value'][i] == '캠핑장') | (pt_melt['value'][i] == '폭포')|        (pt_melt['value'][i] == '피크닉장') | (pt_melt['value'][i] == '호수정원')|        (pt_melt['value'][i] == '연못') | (pt_melt['value'][i] == '생태연못')|        (pt_melt['value'][i] == '생태정원') | (pt_melt['value'][i] == '보트하우스')|        (pt_melt['value'][i] == '음악분수') | (pt_melt['value'][i] == '인공폭포')|        (pt_melt['value'][i] == '안개분수') | (pt_melt['value'][i] == '야유회장')|        (pt_melt['value'][i] == '수경시설') | (pt_melt['value'][i] == '실개천')|        (pt_melt['value'][i] == '생태습지원') | (pt_melt['value'][i] == '전망마당'):
            pt_melt['new_type'][i] = '유희시설'      


        else:
            pass
        
    else:
        pt_melt['new_type'][i] = None


# In[108]:


# 관리사무소, 다례원, 습지원, 야생초화원, 온실, 장미정원   => 쾌적성

for i in range(len(pt_melt)):
    if pt_melt['value'][i] is not None:
        if (pt_melt['value'][i].endswith('분수')) | (pt_melt['value'][i] == '분수대')|        (pt_melt['value'][i]=='계류') | (pt_melt['value'][i].endswith('폭포'))|        (pt_melt['value'][i]=='관리사무소') | (pt_melt['value'][i]=='초화원')|        (pt_melt['value'][i].endswith('정원')) | (pt_melt['value'][i].endswith('연못'))|        (pt_melt['value'][i]=='장미원') | (pt_melt['value'][i]=='수경시설')|        (pt_melt['value'][i]=='실개천') | (pt_melt['value'][i] == '생태습지원') | (pt_melt['value'][i] == '전망마당')|        (pt_melt['value'][i] == '습지') | (pt_melt['value'][i] == '오줌싸개동상'):
            pt_melt['view'][i] = '쾌적성'

        elif (pt_melt['value'][i] == '수목정보센터') | (pt_melt['value'][i] == '누각')|        (pt_melt['value'][i] == '전망대')| (pt_melt['new_type'][i] == '운동시설') |        (pt_melt['new_type'][i] == '편익시설') | (pt_melt['value'][i]=='카스토퍼')|        (pt_melt['value'][i] == '횡단보도') | (pt_melt['value'][i]=='CCTV')|        (pt_melt['value'][i] == '캠핑장') | (pt_melt['value'][i]=='피크닉장')|        (pt_melt['value'][i] == '야유회장') | (pt_melt['value'][i]=='CCTV')|        (pt_melt['value'][i] == '보안등') | (pt_melt['value'][i].endswith('놀이터'))|        (pt_melt['value'][i].endswith('의자')) | (pt_melt['value'][i]=='전망데크')|        (pt_melt['value'][i] == '자연체험학습장') | (pt_melt['value'][i]=='숲체험장')|        (pt_melt['value'][i].endswith('공원')):
            pt_melt['view'][i] = '시설 편의성'

        else:
            pt_melt['view'][i] = '기타'
            
    else:
        pt_melt['view'][i] = None


# ## 합본

# In[109]:


pk_full = pd.concat([pe_melt, pp_melt, pf_melt, pc_melt, pt_melt], axis=0)
pk_full.sort_values('park_nm', inplace=True)
pk_full.reset_index(inplace=True, drop=True)


# In[110]:


for i in range(len(pk_full)):
    if pk_full['value'][i] is not None:
        if pk_full['value'][i] == '도서관 등':
            pk_full['value'][i] = '도서관'
        
        elif pk_full['value'][i] == '영종역사문화관 등':
            pk_full['value'][i] = '영종역사문화관'
          
        elif (pk_full['value'][i] == '자전거 대여') | (pk_full['value'][i] == '자전거대여소'):
            pk_full['value'][i] = '자전거 대여소'
        
        elif pk_full['value'][i] == '자전거보관대':
            pk_full['value'][i] = '자전거 보관대'
            
        elif pk_full['value'][i].endswith('족욕장')==True:
            pk_full['value'][i] = '해수족욕장'


# In[111]:


for i in range(len(pk_full)):
    if pk_full['value'][i] is not None:
        if (pk_full['value'][i].endswith('데크') == True)|(pk_full['value'][i] == '보트하우스'):
            pk_full['new_type'][i] = '편익시설'
            pk_full['view'][i] = '시설 편의성'
            
        elif (pk_full['value'][i].endswith('누각') == True) | (pk_full['value'][i].endswith('도서관') == True)|        (pk_full['value'][i].endswith('교육장') == True) | (pk_full['value'][i].endswith('교육관') == True)|        (pk_full['value'][i].endswith('학습장') == True) | (pk_full['value'][i].endswith('학습원') == True)|        (pk_full['value'][i]=='전망대') | (pk_full['value'][i].endswith('전시장')==True)|        (pk_full['value'][i].endswith('체험원')==True)| (pk_full['value'][i].endswith('체험장')==True)|        (pk_full['value'][i].endswith('문화관')==True)| (pk_full['value'][i].endswith('전시관')==True)|        (pk_full['value'][i].endswith('박물관')==True) | (pk_full['value'][i].endswith('미래관')==True)|        (pk_full['value'][i].startswith('염전')==True):
            pk_full['new_type'][i] = '교양시설'
            pk_full['view'][i] = '시설 편의성'
            
            
        elif (pk_full['value'][i].endswith('동물원')==True) | (pk_full['value'][i].endswith('공연장')==True)|        (pk_full['value'][i].startswith('야영')==True) | (pk_full['value'][i] == '야외음악당'):
            pk_full['new_type'][i] = '유희시설'
            pk_full['view'][i] = '시설 편의성'
            
        elif (pk_full['value'][i].endswith('동상')==True) | (pk_full['value'][i]=='병영 11동'):
            pk_full['new_type'][i] = '교양시설'
            pk_full['view'][i] = '기타'
            
        elif (pk_full['value'][i] == '습지') | (pk_full['value'][i].endswith('습지원')==True):
            pk_full['new_type'][i] = '기타시설'
            pk_full['view'][i] = '쾌적성'
            
        elif (pk_full['value'][i].endswith('지킴터')==True) | (pk_full['value'][i] == 'CCTV'):
            pk_full['new_type'][i] = '기타시설'
            pk_full['view'][i] = '기타'
            
        elif pk_full['value'][i] == '인화루':
            pk_full['value'][i] = '누각'
            pk_full['view'][i] = '시설 편의성'
            
    else:
        pk_full['new_type'][i] = None
        pk_full['view'][i] = None


# In[112]:


for i in range(len(pk_full)):
    if pk_full['value'][i] is not None:
        if pk_full['value'][i].startswith('인라인'):
            pk_full['value'][i] = '인라인스케이트장'
            
        elif pk_full['value'][i] == '의자':
            pk_full['new_type'][i] = '편익시설'
            pk_full['view'][i] = '시설 편의성'
            
        elif pk_full['value'][i] == '주차장':
            pk_full['new_type'][i] = '편익시설'
            pk_full['view'][i] = '기타'
            
        else:
            pass
        
    else:
        pass


# In[113]:


pk_full['category'] = ''


for i in range(len(pk_full)):
    if pk_full['new_type'][i] is not None:
        # 편익시설
        if pk_full['new_type'][i] == '편익시설':
            if (pk_full['value'][i]=='매점')|(pk_full['value'][i]=='간이판매대'):
                pk_full['category'][i] = '간이판매대/매점'
                
            elif (pk_full['value'][i]=='관리사무소')|(pk_full['value'][i]=='안내사무소')            |(pk_full['value'][i]=='방문자센터')|(pk_full['value'][i]=='안내판'):
                pk_full['category'][i] = '관리·안내사무소/안내판'
            
            elif (pk_full['value'][i].endswith('의자'))|(pk_full['value'][i]=='정자')|            (pk_full['value'][i] =='벤치')|(pk_full['value'][i]=='앉음벽')| (pk_full['value'][i]=='파고라')|            (pk_full['value'][i] == '쉼터 및 휴게소') | (pk_full['value'][i] == '휴게실'):
                pk_full['category'][i] = '의자/쉼터·휴게소'
                
            elif (pk_full['value'][i]=='카페테리아')| (pk_full['value'][i]=='초정')|            (pk_full['value'][i]=='음수시설')|(pk_full['value'][i]=='약수터'):
                pk_full['category'][i] = '카페테리아/음수시설'

            elif (pk_full['value'][i]=='화장실'):
                pk_full['category'][i] = '화장실'
                
                
            elif (pk_full['value'][i]=='주차장'):
                pk_full['category'][i] = '주차장'    
                # 단, 주차장은 지표 생성 시 다른 데이터(공공주차장 표준데이터) 활용.
                
            elif (pk_full['value'][i].endswith('족욕장')) | (pk_full['value'][i]=='흙먼지털이기'):
                pk_full['category'][i] = '해수족욕장/흙먼지털이기'
                
            elif (pk_full['value'][i]=='경로당') | (pk_full['value'][i]=='노인정'):
                pk_full['category'][i] = '경로당/노인정'
                
            elif (pk_full['value'][i].startswith('자전거')):
                pk_full['category'][i] = '자전거 시설물'
                
            elif (pk_full['value'][i].endswith('데크'))|(pk_full['value'][i]=='목교')|(pk_full['value'][i]=='육교'):
                pk_full['category'][i] = '데크/목·육교'
                
            elif (pk_full['value'][i]=='공중전화') | (pk_full['value'][i]=='기념광장') |            (pk_full['value'][i].startswith('민방위')):
                pk_full['category'][i] = '기타'
                
            else:
                pass
            
        elif pk_full['new_type'][i] == '유희시설':
            if (pk_full['value'][i].endswith('분수')) | (pk_full['value'][i].startswith('계류'))|            (pk_full['value'][i].endswith('폭포'))|(pk_full['value'][i].endswith('연못'))|(pk_full['value'][i]=='수경시설')|            (pk_full['value'][i]=='호수정원'):
                pk_full['category'][i] = '계류/수경시설'
            
            elif (pk_full['value'][i]=='캠핑장') | (pk_full['value'][i]=='야영장')|(pk_full['value'][i]=='피크닉장'):
                pk_full['category'][i] = '캠핑/야영장'
                
            elif (pk_full['value'][i].endswith('공원')) | (pk_full['value'][i]=='온실')|            (pk_full['value'][i].endswith('정원')) | (pk_full['value'][i]=='다례원')|            (pk_full['value'][i]=='전망마당')|(pk_full['value'][i].endswith('초화원'))|            (pk_full['value'][i]=='장미원'):
                pk_full['category'][i] = '정원/공원/온실'
                
            else:
                pk_full['category'][i] = '놀이시설/놀이기구'
                # 끝나고 확인 필요
                
        elif pk_full['new_type'][i] == '교양시설':
            if (pk_full['value'][i].endswith('체험장'))|(pk_full['value'][i].endswith('교육장'))|            (pk_full['value'][i].endswith('학습장'))|(pk_full['value'][i].endswith('학습원'))|            (pk_full['value'][i].endswith('체험원'))|(pk_full['value'][i]=='어린이교통공원'):
                pk_full['category'][i] ='교육/체험/학습장'
            
            elif (pk_full['value'][i].endswith('박물관')) | (pk_full['value'][i].endswith('전시관'))|            (pk_full['value'][i].endswith('전시장'))|(pk_full['value'][i].endswith('문화관'))|            (pk_full['value'][i]=='환경미래관'):
                pk_full['category'][i] = '박물관/전시관/문화관'
            
            elif (pk_full['value'][i].endswith('도서관'))|(pk_full['value'][i].endswith('정보센터')):
                pk_full['category'][i] = '도서관/정보센터'
            
            elif (pk_full['value'][i].endswith('기념비'))|(pk_full['value'][i].endswith('동상'))|(pk_full['value'][i]=='현충팁'):
                pk_full['category'][i] = '기념비/동상'
                
            elif (pk_full['value'][i]=='누각')|(pk_full['value'][i].endswith('탐조대'))|(pk_full['value'][i].endswith('관찰대'))|            (pk_full['value'][i]=='인화루'):
                pk_full['category'][i] ='관찰·전망대/누각'

            
            else:
                pk_full['category'][i] = '기타'
                
        elif pk_full['new_type'][i] == '운동시설':
            if (pk_full['value'][i].startswith('인라인')) | (pk_full['value'][i]=='X-게임장'):
                pk_full['category'][i] = '인라인스케이트장/X-게임장'
                
            elif (pk_full['value'][i].startswith('다목적'))|(pk_full['value'][i]=='소운동장')|            (pk_full['value'][i]=='구민운동장')|(pk_full['value'][i]=='인조잔디구장'):
                pk_full['category'][i] = '운동장/체육관'
                
            elif (pk_full['value'][i]=='게이트볼장')|(pk_full['value'][i]=='골프연습장')|            (pk_full['value'][i]=='궁도장')|(pk_full['value'][i]=='농구장')|(pk_full['value'][i].endswith('야구장'))|            (pk_full['value'][i]=='배드민턴장')|(pk_full['value'][i]=='양궁장')|(pk_full['value'][i]=='족구장')|            (pk_full['value'][i]=='축구장')|(pk_full['value'][i]=='테니스장')|(pk_full['value'][i]=='파크골프장')|            (pk_full['value'][i]=='론볼링장')|(pk_full['value'][i] == '풋살경기장'):
                pk_full['category'][i] = '경기장'
                
            else:
                pk_full['category'][i] = '운동기구/체육시설'
                
        elif pk_full['new_type'][i] == '기타시설':
            if (pk_full['value'][i]=='CCTV')|(pk_full['value'][i]=='보안등')|(pk_full['value'][i].endswith('울타리')):
                pk_full['category'][i] = '보안등/울타리/CCTV'
                
            elif (pk_full['value'][i].startswith('습지')):
                pk_full['category'][i] = '습지/습지원'
                  
            elif (pk_full['value'][i].startswith('염전')):
                pk_full['category'][i] = '염전/염전보존지역'
                
            elif (pk_full['value'][i]=='수변전실')|(pk_full['value'][i]=='수처리실'):
                pk_full['category'][i] = '관리시설'
                
            elif (pk_full['value'][i]=='저류지')|(pk_full['value'][i]=='저수지'):
                pk_full['category'][i] = '저류지/저수지';
                
            else:
                pk_full['category'][i] = '기타'
        
        else:
            pass
        
    else: # value = None
        pk_full['category'][i] = None


# In[114]:


for i in range(len(pk_full)):
    if (pk_full['value'][i] == '원형의자'):
        pk_full['new_type'][i] = '편익시설'
        pk_full['category'][i] = '의자/쉼터·휴게소'
        pk_full['view'][i] = '시설 편의성'
        
    elif pk_full['value'][i] == '보트하우스':
        pk_full['new_type'][i] = '유희시설'
        pk_full['category'][i] = '놀이시설/놀이기구'
        pk_full['view'][i] = '시설 편의성'
            
    else:
        pass
else:
    pass


# del pt_melt, pe_melt, pf_melt, pc_melt, pp_melt
gc.collect()

# display(pk_full.head())
pk_full.rename(columns={'value':'fclts', 'view':'se'}, inplace=True)
pk_full_fin = pk_full.drop(['original_type','new_type','category'], axis=1)

pk_full_fin.reset_index(inplace=True)
pk_full_fin.rename(columns={'index':'no'}, inplace=True)

# yr_now = datetime.datetime.now().date().strftime('%Y')
# pk_full_fin['crtr_yr'] = yr_now

pk_full_fin.head()

pk_full_fin = pk_full_fin[['crtr_yr','no','mng_no','park_nm','park_se','fclts','se']]

pk_full_fin.head(3)


# In[19]:


try :
    pff = list(pk_full_fin.itertuples(index=False, name=None))
   
    result_query = """
    INSERT INTO tdm_ic_ctypark_fac_fclts (crtr_yr, no, mng_no, park_nm, park_se, fclts, se)
    VALUES (%s, %s, %s, %s, %s, %s, %s) """
    
    cur.executemany(result_query, pff)
    conn.commit()
    
    print('inserted new results into db')
    
except Exception as e:
    print(e, '\n')
    print('Failed!')


# In[20]:


# 도시공원서비스 지표 생성

park_nbh = pk_full[pk_full['park_se']=='근린공원'].reset_index(drop=True)
park_ch = pk_full[pk_full['park_se']=='어린이공원'].reset_index(drop=True)
park_sm = pk_full[pk_full['park_se']=='소공원'].reset_index(drop=True)


# ## 시설 편의성

# In[118]:


def fac_df(data):
    park_fac = data[data['se']=='시설 편의성'].groupby(['mng_no','park_nm','park_se','new_type']).count().reset_index().iloc[:, [0,1,2,3,5]]
    park_fac.columns = ['mng_no','park_nm','park_se','fac_type','fclts_repr']
    park_fac2 = park_fac.groupby(['mng_no','park_nm','park_se']).sum().reset_index()

    dt_list = data.iloc[:,:4].drop_duplicates().reset_index(drop=True)
    
    global pk_fac
    pk_fac = pd.merge(dt_list, park_fac2, how='left', on=['mng_no','park_nm','park_se'])
    pk_fac = pk_fac.fillna(0)
    pk_fac['fclts_repr'] = pk_fac['fclts_repr'].astype(int)   
    #display(pk_fac)


# In[119]:


fac_df(park_nbh); nbh_fac = pk_fac
fac_df(park_ch); child_fac = pk_fac
fac_df(park_sm); small_fac = pk_fac

del pk_fac

gc.collect()


# 주차장 데이터 추가

# In[120]:


# 공원조성과_주차장

try :
    # db에 적재되어 있는 데이터 불러오기
    
    #공원조성과 주차장
    sql = """select * from tdw_ic_park_ise_prkplce where crtr_yr = (select max(crtr_yr) from tdw_ic_park_ise_prkplce
    where crtr_yr <= left(current_date::varchar,4));"""
    
    parking = pd.read_sql(sql, conn)
    
    pc_pk = parking[parking['mtchg_park_nm'].notna()].drop('sn', axis=1)
    pc_pk[['parkng_nopg', 'parkng_area']] = pc_pk[['parkng_nopg','parkng_area']].apply(pd.to_numeric)
    pc_pk2 = pc_pk[['crtr_yr','park_nm','parkng_nopg','mtchg_park_nm']]; pck_fin = pc_pk2.dropna()    
    pck_fin['parkng_nopg'] = pck_fin['parkng_nopg'].astype(int)
    
    #공영주차장
    sql2 = """select * from tdm_wnty_prkplce_info_std_data where crtr_yr = (select max(crtr_yr) from tdm_wnty_prkplce_info_std_data
    where crtr_yr <= left(current_date::varchar,4));"""
    
    parking2 = pd.read_sql(sql2, conn)
    parking2 = parking2.iloc[:,[2,3,4,5,6,7,8,30,31]]

    parking2['geometry'] = parking2.apply(lambda row: Point([row['lo'], row['lat']]), axis=1)
    parking2 = gpd.GeoDataFrame(parking2, geometry = 'geometry')
    parking2.crs = {'init':'epsg:4326'}
       
    print('공원조성과, 공영주차장 데이터 로드 성공')
    
except Exception as e :
    print(e)
    print('공원조성과, 공영주차장 데이터 로드 실패')


# In[21]:


#공원조성과 데이터 : 일부 큰 공원 내 주차장 데이터 활용

nbhfac_pk1 = pd.merge(nbh_fac, pck_fin, how='left', on='park_nm') # 129 rows
nbhfac_parking_n = nbhfac_pk1[nbhfac_pk1['mtchg_park_nm'].isnull()==True].reset_index(drop=True).drop(['mtchg_park_nm'], axis=1)
nbhfac_parking_y = nbhfac_pk1[nbhfac_pk1['mtchg_park_nm'].notna()].reset_index(drop=True).drop(['mtchg_park_nm'], axis=1)

chfac_pk1 = pd.merge(child_fac, pck_fin, how='left', on='park_nm') 
chfac_parking_n = chfac_pk1[chfac_pk1['mtchg_park_nm'].isnull()==True].reset_index(drop=True).drop(['mtchg_park_nm'], axis=1)
chfac_parking_y = chfac_pk1[chfac_pk1['mtchg_park_nm'].notna()].reset_index(drop=True).drop(['mtchg_park_nm'], axis=1)


smhfac_pk1 = pd.merge(small_fac, pck_fin, how='left', on='park_nm') 
smfac_parking_n = smhfac_pk1[smhfac_pk1['mtchg_park_nm'].isnull()==True].reset_index(drop=True).drop(['mtchg_park_nm'], axis=1)
smfac_parking_y = smhfac_pk1[smhfac_pk1['mtchg_park_nm'].notna()].reset_index(drop=True).drop(['mtchg_park_nm'], axis=1)


print(len(nbhfac_parking_n), len(nbhfac_parking_y),'\n')
print(len(chfac_parking_n), len(chfac_parking_y),'\n')
print(len(smfac_parking_n), len(smfac_parking_y),'\n')


park_pk = park[['crtr_yr','mng_no','park_nm','park_se','lat','lot']]

park_pk['geometry'] = park_pk.apply(lambda row : Point([row['lot'], row['lat']]), axis=1)
park_pk = gpd.GeoDataFrame(park_pk, geometry = 'geometry')
park_pk.crs = {'init':'epsg:4326'}

park_gl = park_pk[park_pk['park_se']=='근린공원']
park_gl.reset_index(inplace=True, drop=True) # 184개

park_small = park_pk[park_pk['park_se']=='소공원']
park_small.reset_index(inplace=True, drop=True) # 40개

park_child = park_pk[park_pk['park_se']=='어린이공원']
park_child.reset_index(inplace=True, drop=True) # 404개

#버퍼 생성

g_temp = park_gl.to_crs({'init':'epsg:5179'}).buffer(500).to_crs({'init':'epsg:4326'})
s_temp = park_small.to_crs({'init':'epsg:5179'}).buffer(250).to_crs({'init':'epsg:4326'})
c_temp = park_child.to_crs({'init':'epsg:5179'}).buffer(250).to_crs({'init':'epsg:4326'})

gl_buf = gpd.GeoDataFrame({'geometry':g_temp})
sm_buf = gpd.GeoDataFrame({'geometry':s_temp})
ch_buf = gpd.GeoDataFrame({'geometry':c_temp})

pk_gl = gpd.GeoDataFrame(pd.concat([park_gl[['crtr_yr','mng_no','park_nm','park_se']], gl_buf], axis=1),
                       geometry='geometry')

pk_sm = gpd.GeoDataFrame(pd.concat([park_small[['crtr_yr','mng_no','park_nm','park_se']], sm_buf], axis=1),
                       geometry='geometry')

pk_ch = gpd.GeoDataFrame(pd.concat([park_child[['crtr_yr','mng_no','park_nm','park_se']], ch_buf], axis=1),
                       geometry='geometry')


# In[22]:


### 버퍼 내 공영주차장 데이터 join - 근린

gl_parking = gpd.sjoin(pk_gl, parking2, how='inner', op='intersects') # 버퍼 내 공영주차장이 있는 데이터 셋
glp = gl_parking.groupby(['mng_no','park_nm']).sum().reset_index() # 공원별 주차구획수 sum한 데이터 셋
glp = glp.iloc[:,[0,1,3]] # rows = 80 ==> 버퍼 내 공영주차장이 존재하는 공원이 80개라는 의미
del gl_parking


# pk_gl : 근린공원 버퍼 
glp_fin = pd.merge(pk_gl.iloc[:,:3], glp, how='left', on=['mng_no','park_nm'])
glp_fin['prkcmprt_cnt'] = glp_fin['prkcmprt_cnt'].fillna(0);
glp_fin
del glp

nbhfac_parking_n.drop('crtr_yr_y', axis=1, inplace=True)
nbhfac_parking_n.rename(columns = {'crtr_yr_x':'crtr_yr'}, inplace=True)

# 내부 주차장 없는 데이터 + 공영주차장 데이터 ==> 공영주차장 데이터로 값 교체 : 150 rows
nbhfac_parking_n2 = pd.merge(nbhfac_parking_n, glp_fin,
                             how='left', on=['crtr_yr','mng_no','park_nm']).drop(['crtr_yr','parkng_nopg'], axis=1)
nbhfac_parking_n2.rename(columns={'prkcmprt_cnt':'parkng_nopg'}, inplace=True)
nbhfac_parking_n2
del glp_fin

nbhfac_pk_fin = pd.concat([nbhfac_parking_n2, nbhfac_parking_y], axis=0)
del nbhfac_parking_n2
nbhfac_pk_fin.sort_values('park_nm').reset_index(inplace=True, drop=True)
nbhfac_pk_fin = nbhfac_pk_fin.iloc[:,:5]


gl_fac_fin = pd.merge(nbhfac_pk_fin, park[['crtr_yr','mng_no','park_nm','park_se']],
                      how='left', on=['mng_no','park_nm','park_se'])#.drop('manage_nm_x', axis=1)
gl_fac_fin.rename(columns={'parkng_nopg':'parkng_spce_repr'}, inplace=True)
del nbhfac_pk_fin

gc.collect()
gl_fac_fin.head()


# In[23]:


### 버퍼 내 공영주차장 데이터 join - 어린이

ch_parking = gpd.sjoin(pk_ch, parking2, how='inner', op='intersects') # 버퍼 내 공영주차장이 있는 데이터 셋
clp = ch_parking.groupby(['mng_no','park_nm']).sum().reset_index() # 공원별 주차구획수 sum한 데이터 셋
clp = clp.iloc[:,[0,1,3]] # rows = 80 ==> 버퍼 내 공영주차장이 존재하는 공원이 80개라는 의미
del ch_parking


# pk_gl : 근린공원 버퍼 
clp_fin = pd.merge(pk_ch.iloc[:,:3], clp, how='left', on=['mng_no','park_nm'])
clp_fin['prkcmprt_cnt'] = clp_fin['prkcmprt_cnt'].fillna(0);
clp_fin
del clp

chfac_parking_n.drop('crtr_yr_y', axis=1, inplace=True)
chfac_parking_n.rename(columns = {'crtr_yr_x':'crtr_yr'}, inplace=True)

# 내부 주차장 없는 데이터 + 공영주차장 데이터 ==> 공영주차장 데이터로 값 교체 : 150 rows
chfac_parking_n2 = pd.merge(chfac_parking_n, clp_fin,
                             how='left', on=['crtr_yr','mng_no','park_nm']).drop(['crtr_yr','parkng_nopg'], axis=1)
chfac_parking_n2.rename(columns={'prkcmprt_cnt':'parkng_nopg'}, inplace=True)
chfac_parking_n2
del clp_fin

chfac_pk_fin = pd.concat([chfac_parking_n2, chfac_parking_y], axis=0)
del chfac_parking_n2


chfac_pk_fin.sort_values('park_nm').reset_index(inplace=True, drop=True)
chfac_pk_fin = chfac_pk_fin.iloc[:,:5]


ch_fac_fin = pd.merge(chfac_pk_fin, park[['crtr_yr','mng_no','park_nm','park_se']],
                      how='left', on=['mng_no','park_nm','park_se'])#.drop('manage_nm_x', axis=1)
ch_fac_fin.rename(columns={'parkng_nopg':'parkng_spce_repr'}, inplace=True)
del chfac_pk_fin

gc.collect()
ch_fac_fin.head()


# In[24]:


### 버퍼 내 공영주차장 데이터 join - 소공원

sm_parking = gpd.sjoin(pk_sm, parking2, how='inner', op='intersects') # 버퍼 내 공영주차장이 있는 데이터 셋
slp = sm_parking.groupby(['mng_no','park_nm']).sum().reset_index() # 공원별 주차구획수 sum한 데이터 셋
slp = slp.iloc[:,[0,1,3]] # rows = 80 ==> 버퍼 내 공영주차장이 존재하는 공원이 80개라는 의미
del sm_parking


# pk_gl : 근린공원 버퍼 
slp_fin = pd.merge(pk_sm.iloc[:,:3], slp, how='left', on=['mng_no','park_nm'])
slp_fin['prkcmprt_cnt'] = slp_fin['prkcmprt_cnt'].fillna(0);
slp_fin
del slp

smfac_parking_n.drop('crtr_yr_y', axis=1, inplace=True)
smfac_parking_n.rename(columns = {'crtr_yr_x':'crtr_yr'}, inplace=True)

# 내부 주차장 없는 데이터 + 공영주차장 데이터 ==> 공영주차장 데이터로 값 교체 : 150 rows
smfac_parking_n2 = pd.merge(smfac_parking_n, slp_fin,
                             how='left', on=['crtr_yr','mng_no','park_nm']).drop(['crtr_yr','parkng_nopg'], axis=1)
smfac_parking_n2.rename(columns={'prkcmprt_cnt':'parkng_nopg'}, inplace=True)
smfac_parking_n2
del slp_fin

smfac_pk_fin = pd.concat([smfac_parking_n2, smfac_parking_y], axis=0)
del smfac_parking_n2


smfac_pk_fin.sort_values('park_nm').reset_index(inplace=True, drop=True)
smfac_pk_fin = smfac_pk_fin.iloc[:,:5]


sm_fac_fin = pd.merge(smfac_pk_fin, park[['crtr_yr','mng_no','park_nm','park_se']],
                      how='left', on=['mng_no','park_nm','park_se'])#.drop('manage_nm_x', axis=1)
sm_fac_fin.rename(columns={'parkng_nopg':'parkng_spce_repr'}, inplace=True)
del smfac_pk_fin

gc.collect()
sm_fac_fin.head()


# In[25]:


def f_mm_scale(data):
    
    data['fclts_scr']=0; data['parkng_spce_scr'] = 0

    data['fclts_scr'] = round( ((data['fclts_repr'] - data['fclts_repr'].min(axis=0))/(data['fclts_repr'].max(axis=0) - data['fclts_repr'].min(axis=0)) ) *100, 2)
    data['parkng_spce_scr'] = round( ((data['parkng_spce_repr'] - data['parkng_spce_repr'].min(axis=0))/(data['parkng_spce_repr'].max(axis=0) - data['parkng_spce_repr'].min(axis=0)) ) *100, 2)

    data['scr_sum'] = data['fclts_scr'] + data['parkng_spce_scr']

    data['idx_vl'] = round( ((data['scr_sum']-data['scr_sum'].min(axis=0))/
                                        ( data['scr_sum'].max(axis=0)-data['scr_sum'].min(axis=0))*100),2)

    global dts
    dts = data.copy()

    
# 시설 수 및 주차구획수 minmax scailing

f_mm_scale(gl_fac_fin); gl_fac_ff = dts
f_mm_scale(ch_fac_fin); ch_fac_ff = dts
f_mm_scale(sm_fac_fin); sm_fac_ff = dts
del dts

fac_fin = pd.concat([gl_fac_ff, ch_fac_ff, sm_fac_ff], axis=0)
fac_fin.reset_index(inplace=True, drop=True)

del gl_fac_ff, ch_fac_ff, sm_fac_ff
gc.collect()


# In[128]:


fac_ff = pd.merge(fac_fin, park[['crtr_yr','mng_no','park_nm','park_se','sgg_cd','sgg','emd_cd','emd']],
         how='left', on=['crtr_yr','mng_no','park_nm','park_se'])

fac_ff = fac_ff[['crtr_yr','mng_no','park_nm','park_se','sgg_cd','sgg','emd_cd','emd','fclts_repr','parkng_spce_repr',
                'fclts_scr','parkng_spce_scr','scr_sum','idx_vl']]


# fac_ff['crtr_yr'] = yr_now

fac_ff.head()


# In[26]:


try : 
    fac_t = list(fac_ff.itertuples(index=False, name=None))
    
    result_query = """
        INSERT INTO tdm_ic_ctypark_fac_idx (crtr_yr, mng_no, park_nm, park_se, sgg_cd, sgg, emd_cd, emd,
        fclts_repr, parkng_spce_repr, fclts_scr, parkng_spce_scr, scr_sum, idx_vl) VALUES (%s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
    
    cur.executemany(result_query, fac_t); conn.commit()
    print('inserted new results into db')
    
    print('DB 적재 성공')
    
except Exception as e:
    print(e, '\n')
    print('Failed!')


# In[27]:


# 쾌적성

try :
    # db에 적재되어 있는 데이터 불러오기
    ## 공원조성과 완충녹지 테이블
    sql = """
    select * from tdw_ic_ctypark_buffer_green_area where crtr_yr = (select max(crtr_yr) from tdw_ic_ctypark_buffer_green_area
    where crtr_yr <= left(current_date::varchar,4));"""
    
    green = pd.read_sql(sql, conn)
    
    print('완충녹지 데이터 로드 성공')
    
except Exception as e:
    print(e)
    print('완충녹지 데이터 로드 실패')



green.rename(columns={'park_area':'green_park_area'}, inplace=True)

pk_green = pd.merge(park[['crtr_yr','mng_no','park_nm','park_se','park_area','sgg_cd','sgg','emd_cd','emd']],
                    green[['crtr_yr','park_se','green_park_area','greens_area','ctypark_park_nm','sgg','emd']],
                    left_on=['crtr_yr','park_nm','park_se','sgg'],
                    right_on=['crtr_yr','ctypark_park_nm','park_se','sgg'],
                    how='left').drop('emd_y', axis=1)
# del green
gc.collect()

pk_green.rename(columns={'emd_x':'emd','green_park_area':'greens_buffer_zone_park_area',
                         'greens_area':'greens_buffer_zone_area'}, inplace=True)

pk_green.head(2)


# In[144]:


# 계양구 효성동 산18 << 이촌공원 살리기 : 95416	80889
# 인천광역시 연수구 동춘동 938-2 << 동춘공원 살리기 : 10000.1	10000.1

pk_green['greens_buffer_zone_park_area'] = pk_green['greens_buffer_zone_park_area'].fillna(0)
pk_green['greens_buffer_zone_area'] = pk_green['greens_buffer_zone_area'].fillna(0)

pk_green.head(2)


# In[145]:


for i in tqdm(range(len(pk_green))):
    if pk_green['greens_buffer_zone_park_area'][i] == 0.0:
        pk_green['greens_buffer_zone_park_area'][i] = pk_green['park_area'][i]
    else:
        pass
    
    
# 공원 내 녹지비율로 점수화 진행

pk_green['greens_buffer_zone_rt'] = 0.0
pk_green['greens_buffer_zone_rt'] = round(pk_green['greens_buffer_zone_area']/pk_green['greens_buffer_zone_park_area']*100,2)

gc.collect()

pk_green.head(2)


# In[146]:


cmf = park_nbh[park_nbh['se']=='쾌적성'].groupby(['mng_no','park_nm','park_se','new_type']).count().reset_index().iloc[:, [0,1,2,3,5]]
cmf.columns = ['mng_no','park_nm','park_se','fac_type','fclts_repr']

cmf2 = cmf.groupby(['mng_no','park_nm','park_se']).sum().reset_index()

dt_list = park_nbh.iloc[:,:4].drop_duplicates().reset_index(drop=True)

dt_cmf = pd.merge(dt_list, cmf2, how='left', on='park_nm').drop(['mng_no_y', 'park_se_y'], axis=1)
dt_cmf.rename(columns={'mng_no_x':'mng_no', 'park_se_x':'park_se'}, inplace=True)
del cmf

dt_cmf = dt_cmf.fillna(0.0)
dt_cmf['fclts_repr'] = dt_cmf['fclts_repr'].astype(int)

dt_cmff = pd.merge(dt_cmf, pk_green, on=['crtr_yr', 'mng_no', 'park_nm', 'park_se'], how='left')
del dt_cmf

dt_cmff['fclts_scr'] = 0.0

for i in range(len(dt_cmff)):
    if dt_cmff['fclts_repr'][i] == 0:
        dt_cmff['fclts_scr'][i] = 0.0
    else:
        dt_cmff['fclts_scr'] = round( ((dt_cmff['fclts_repr']-dt_cmff['fclts_repr'].min(axis=0))/
                                    ( dt_cmff['fclts_repr'].max(axis=0)-dt_cmff['fclts_repr'].min(axis=0))*100),2)

dt_cmff['scr_sum'] = dt_cmff['fclts_scr'] + dt_cmff['greens_buffer_zone_rt']

dt_cmff['idx_vl'] = round( ((dt_cmff['scr_sum']-dt_cmff['scr_sum'].min(axis=0))/
                                    ( dt_cmff['scr_sum'].max(axis=0)-dt_cmff['scr_sum'].min(axis=0))*100),2)

dt_cmff['idx_vl'] = dt_cmff['idx_vl'].replace({np.nan:None})

for k in range(len(dt_cmff)):
    if dt_cmff['idx_vl'][k] is None:
        dt_cmff['idx_vl'][k] = dt_cmff['idx_vl'][k]

global dat
dat = dt_cmff.copy()
dat.rename(columns={'park_area':'ctypark_park_area','ctypark_park_nm':'greens_buffer_zone_park_nm'}, inplace=True)
dat = dat[['crtr_yr','mng_no','park_nm','park_se','sgg_cd','sgg','emd_cd','emd','fclts_repr','greens_buffer_zone_park_nm',
         'ctypark_park_area','greens_buffer_zone_park_area','greens_buffer_zone_area','greens_buffer_zone_rt',
         'fclts_scr','scr_sum','idx_vl']]


# In[147]:


def cmf_df(data):
    cmf = data[data['se']=='쾌적성'].groupby(['mng_no','park_nm','park_se','new_type']).count().reset_index().iloc[:, [0,1,2,3,5]]
    cmf.columns = ['mng_no','park_nm','park_se','fac_type','fclts_repr']

    cmf2 = cmf.groupby(['mng_no','park_nm','park_se']).sum().reset_index()

    dt_list = data.iloc[:,:4].drop_duplicates().reset_index(drop=True)

    dt_cmf = pd.merge(dt_list, cmf2, how='left', on='park_nm').drop(['mng_no_y', 'park_se_y'], axis=1)
    dt_cmf.rename(columns={'mng_no_x':'mng_no', 'park_se_x':'park_se'}, inplace=True)
    del cmf

    dt_cmf = dt_cmf.fillna(0.0)
    dt_cmf['fclts_repr'] = dt_cmf['fclts_repr'].astype(int)

    dt_cmff = pd.merge(dt_cmf, pk_green, on=['crtr_yr', 'mng_no', 'park_nm', 'park_se'], how='left')
    del dt_cmf

    dt_cmff['fclts_scr'] = 0.0

    for i in range(len(dt_cmff)):
        if dt_cmff['fclts_repr'][i] == 0:
            dt_cmff['fclts_scr'][i] = 0.0
        else:
            dt_cmff['fclts_scr'] = round( ((dt_cmff['fclts_repr']-dt_cmff['fclts_repr'].min(axis=0))/
                                        ( dt_cmff['fclts_repr'].max(axis=0)-dt_cmff['fclts_repr'].min(axis=0))*100),2)

    dt_cmff['scr_sum'] = dt_cmff['fclts_scr'] + dt_cmff['greens_buffer_zone_rt']

    dt_cmff['idx_vl'] = round( ((dt_cmff['scr_sum']-dt_cmff['scr_sum'].min(axis=0))/
                                        ( dt_cmff['scr_sum'].max(axis=0)-dt_cmff['scr_sum'].min(axis=0))*100),2)

    dt_cmff['idx_vl'] = dt_cmff['idx_vl'].replace({np.nan:None})

    for k in range(len(dt_cmff)):
        if dt_cmff['idx_vl'][k] is None:
            dt_cmff['idx_vl'][k] = dt_cmff['scr_sum'][k]

    global dst
    dst = dt_cmff.copy()
    dst.rename(columns={'park_area':'ctypark_park_area','ctypark_park_nm':'greens_buffer_zone_park_nm'}, inplace=True)
    dst = dst[['crtr_yr','mng_no','park_nm','park_se','sgg_cd','sgg','emd_cd','emd','fclts_repr','greens_buffer_zone_park_nm',
             'ctypark_park_area','greens_buffer_zone_park_area','greens_buffer_zone_area','greens_buffer_zone_rt',
             'fclts_scr','scr_sum','idx_vl']]


# In[148]:


cmf_df(park_nbh); gl_cmf = dst; # ok
cmf_df(park_ch); ch_cmf = dst; # ok
cmf_df(park_sm); sm_cmf = dst;
del dst

cmf_fin = pd.concat([gl_cmf,ch_cmf,sm_cmf], axis=0)
cmf_fin.reset_index(inplace=True, drop=True)

print(len(cmf_fin))

del gl_cmf, ch_cmf, sm_cmf
gc.collect()


# cmf_fin['crtr_yr'] = yr_now

cmf_fin


# In[28]:


try : 
    cmf_t = list(cmf_fin.itertuples(index=False, name=None))

    result_query = """
        INSERT INTO tdm_ic_ctypark_cmf_idx (crtr_yr, mng_no, park_nm, park_se, sgg_cd, sgg, emd_cd, emd,
        fclts_repr, greens_buffer_zone_park_nm, ctypark_park_area, greens_buffer_zone_park_area,
        greens_buffer_zone_area, greens_buffer_zone_rt, fclts_scr, scr_sum, idx_vl) \
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
    
    cur.executemany(result_query, cmf_t); conn.commit()
    print('inserted new results into db')

except Exception as e:
    print(e, '\n')
    print('Failed!')


# In[29]:


# 대중교통 접근성

try :
    # db에 적재되어 있는 데이터 불러오기
    s_sql = """select * from tdm_ic_subway_statn_addr where crtr_yr = (select max(crtr_yr) from tdm_ic_subway_statn_addr
    where crtr_yr <= left(current_date::varchar,4));"""
    
 
    b_sql = """select * from tdm_ic_dwtw_bus_sttn_addr where crtr_yr = (select max(crtr_yr) from tdm_ic_dwtw_bus_sttn_addr
    where crtr_yr <= left(current_date::varchar,4));"""

    sbw = pd.read_sql(s_sql, conn)
    bus = pd.read_sql(b_sql, conn)                      

    sbw['geometry'] = sbw.apply(lambda row: Point([row['lot'],row['lat']]), axis=1)
    sbw = gpd.GeoDataFrame(sbw, geometry='geometry')
    sbw.crs = {'init':'epsg:4326'}
    
    bus['geometry'] = bus.apply(lambda row: Point([row['lot'],row['lat']]), axis=1)
    bus = gpd.GeoDataFrame(bus, geometry='geometry')
    bus.crs = {'init':'epsg:4326'}
    
    bus.dropna(inplace=True)
    
    print('지하철 및 버스 데이터 로드 성공')
    
except Exception as e:
    print(e)
    print('지하철 및 버스 데이터 로드 실패')




def trans_df(buffer):
    # 지하철

    gl_sbw = gpd.sjoin(buffer, sbw[['crtr_yr','ln_nm','statn_nm','geometry']], how='left', op='intersects').drop('crtr_yr_right',axis=1)
    gl_sbw.rename(columns={'crtr_yr_left':'crtr_yr'}, inplace=True)
    gls = gl_sbw.groupby(['crtr_yr','mng_no','park_nm','park_se']).count().reset_index() # 공원별 지하철역 개수 sum한 데이터 셋

    gls = gls[['crtr_yr','mng_no','park_nm','park_se','ln_nm']]
    del gl_sbw

    gls_fin = pd.merge(buffer.iloc[:,:4], gls, how='left', on=['crtr_yr','mng_no','park_nm','park_se'])
    gls_fin.rename(columns={'ln_nm':'subway_statn_repr'}, inplace=True)

    #버스
    gl_bus = gpd.sjoin(buffer, bus[['crtr_yr','sttn_id','sttn_no','sttn_nm','geometry']], how='left', op='intersects').drop('crtr_yr_right',axis=1)
    gl_bus.rename(columns={'crtr_yr_left':'crtr_yr'}, inplace=True)
    glb = gl_bus.groupby(['crtr_yr','mng_no','park_nm','park_se']).count().reset_index()

    glb_fin = pd.merge(buffer.iloc[:,:4], glb, how='left', on=['mng_no','park_nm','park_se'])

    glb_fin.rename(columns={'sttn_nm':'bus_sttn_repr'}, inplace=True)
    glb_fin = glb_fin.iloc[:,[0,1,2,3,-1]]
    del glb, gl_bus

    glt = pd.merge(gls_fin, glb_fin, how='left', on=['mng_no','park_nm','park_se']).drop('crtr_yr_x', axis=1)

    glt['scr_sum'] = glt['subway_statn_repr'] + glt['bus_sttn_repr']
    glt['idx_vl'] = round( ((glt['scr_sum'] - glt['scr_sum'].min(axis=0))/
                                     ( glt['scr_sum'].max(axis=0) - glt['scr_sum'].min(axis=0)) ) *100, 2)


    gc.collect()
    
    global glt_fin
    glt_fin = pd.merge(glt, park[['crtr_yr','mng_no','park_nm','park_se','sgg_cd','sgg','emd_cd','emd']],
             on=['crtr_yr','mng_no','park_nm','park_se'], how='left')
    
    glt_fin = glt_fin[['crtr_yr','mng_no','park_nm','park_se','sgg_cd','sgg','emd_cd','emd','subway_statn_repr',
                  'bus_sttn_repr','scr_sum','idx_vl']]



trans_df(pk_gl); gltf = glt_fin
trans_df(pk_ch); cht = glt_fin
trans_df(pk_sm); smt = glt_fin

trans_fin = pd.concat([gltf,cht,smt], axis=0)
trans_fin.reset_index(inplace=True, drop=True)

del gltf,cht,smt

gc.collect()

# trans_fin['crtr_yr'] = yr_now
trans_fin


# In[30]:


try : 
    trans_t = list(trans_fin.itertuples(index=False, name=None))
    
    result_query = """
    INSERT INTO tdm_ic_ctypark_pbtrnsp_acsblt_idx (crtr_yr, mng_no, park_nm, park_se, sgg_cd, sgg, emd_cd, emd,
    subway_statn_repr, bus_sttn_repr, scr_sum, idx_vl) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
    
    cur.executemany(result_query, trans_t)
    conn.commit()
    print('inserted new results into db')
    print('DB 적재 성공')
    
except Exception as e:
    print(e, '\n')
    print('Failed!')


# In[31]:


# 보행접근성

try :
    # db에 적재되어 있는 데이터 불러오기
    
#     l_sql = """
#     select A.grid_id, A.geom, B.crtr_yr, B.ppltn_cnt from tdm_gis_molit_reside_ppltn_crtr A
#     left outer join tdm_molit_reside_ppltn_cnt B on A.grid_id=B.grid_id and B.crtr_yr = (select max(crtr_yr)
#     from tdm_molit_reside_ppltn_cnt where crtr_yr <= left(current_date::varchar,4));
#     """
    
    l_sql = """
    select grid_id, geom, crtr_yr, ppltn_cnt from tdm_gis_ic_tot_ppltn_cnt where crtr_yr = (select max(crtr_yr)
    from tdm_gis_ic_tot_ppltn_cnt where crtr_yr <= left(current_date::varchar,4));
    """
    # 거주인구 (shp 데이터)

    j_sql = """
    select * from (select rpad(c1,10,'0') as emd_cd, c1_nm as emd, sum(dt::numeric) as total from tdw_kosis_stats
    where tbl_id = 'DT_1B04005N' and itm_nm = '총인구수' and prd_de = left(current_date::varchar,4)||'01' and c2='0'
    and length(c1) = 5 and left(c1,2) = '28' group by c1, c1_nm
    union all
    select rpad(c1,10,'0') as emd_cd, c1_nm as emd, sum(dt::numeric) as total from tdw_kosis_stats
    where tbl_id='DT_1B04005N' and itm_nm='총인구수' and prd_de = left(current_date::varchar,4)||'01' and c2='0'
    and length(c1) = 10 and left(c1,2) = '28' group by c1, c1_nm) A order by emd_cd;"""
    # 해당 연도의 1월달 데이터(주민등록인구)

    
    lpop = gpd.read_postgis(l_sql, conn, geom_col='geom', crs={'init':'epsg:5179'})
    jpop = pd.read_sql(j_sql,conn)
       
    print('인구 데이터 로드 성공')
    
except Exception as e:
    print(e)

    print('인구 데이터 로드 실패')


# In[32]:


# 거주인구
lpop['ppltn_cnt'] = lpop['ppltn_cnt'].fillna(0.0)

### 주민등록인구
jpop['total'] = jpop['total'].astype(int)


gllp = gpd.sjoin(pk_gl, lpop, how='left', op='intersects').drop(['index_right','crtr_yr_right'],axis=1)
gllp.rename(columns={'crtr_yr_left':'crtr_yr'}, inplace=True)

gllpp = gllp.groupby(['crtr_yr','mng_no','park_nm','park_se']).sum().reset_index()
del gllp

gllpp_fin = pd.merge(pk_gl.iloc[:,:4], gllpp, how='left', on=['crtr_yr','mng_no','park_nm','park_se'])
gllpp_fin = pd.merge(gllpp_fin, park[['crtr_yr','mng_no','park_nm','park_se','sgg_cd','sgg','emd_cd','emd']],
                     how='left', on=['crtr_yr','mng_no','park_nm','park_se'])


def walk_df(buffer):
    # 버퍼 내 or 버퍼에 걸쳐지는 인구까지 다 포함한, 버퍼 내 거주인구 산출 - 근린

    gllp = gpd.sjoin(buffer, lpop, how='left', op='intersects').drop(['index_right','crtr_yr_right'],axis=1)
    gllp.rename(columns={'crtr_yr_left':'crtr_yr'}, inplace=True)
    
    gllpp = gllp.groupby(['crtr_yr','mng_no','park_nm','park_se']).sum().reset_index()
    del gllp


    gllpp_fin = pd.merge(buffer.iloc[:,:4], gllpp, how='left', on=['crtr_yr','mng_no','park_nm','park_se'])
    gllpp_fin = pd.merge(gllpp_fin, park[['crtr_yr','mng_no','park_nm','park_se','sgg_cd','sgg','emd_cd','emd']],
                         how='left', on=['crtr_yr','mng_no','park_nm','park_se'])

    for i in range(len(gllpp_fin)):
        if gllpp_fin['emd'][i] == '송현1·2동':
            gllpp_fin['emd'][i] = gllpp_fin['emd'][i].replace('송현1·2동', '송현1.2동')

        elif gllpp_fin['emd'][i] == '화수1·화평동':
            gllpp_fin['emd'][i] = gllpp_fin['emd'][i].replace('화수1·화평동','화수1.화평동')

        elif gllpp_fin['emd'][i] == '도화2·3동':
            gllpp_fin['emd'][i] = gllpp_fin['emd'][i].replace('도화2·3동','도화2.3동')
            
        elif gllpp_fin['emd'][i] == '용현1·4동':
            gllpp_fin['emd'][i] = gllpp_fin['emd'][i].replace('용현1·4동','용현1.4동')
            
        elif gllpp_fin['emd'][i] == '숭의1·3동':
            gllpp_fin['emd'][i] = gllpp_fin['emd'][i].replace('숭의1·3동','숭의1.3동')
            
    global walk
    walk = pd.merge(gllpp_fin, jpop, how='left', on=['emd_cd','emd'])
    walk.rename(columns={'ppltn_cnt':'reside_ppltn_cnt','total':'dong_rsgst_ppltn_cnt'}, inplace=True)
    walk['ppltn_cnt_prcs'] = walk['reside_ppltn_cnt']/walk['dong_rsgst_ppltn_cnt']

    # minmax
    walk['idx_vl'] = round( ((walk['ppltn_cnt_prcs'] - walk['ppltn_cnt_prcs'].min(axis=0))/
                                   ( walk['ppltn_cnt_prcs'].max(axis=0) - walk['ppltn_cnt_prcs'].min(axis=0)) ) *100, 2)
    
    walk = walk[['crtr_yr','mng_no','park_nm','park_se','sgg_cd','sgg','emd_cd','emd','reside_ppltn_cnt',
                'dong_rsgst_ppltn_cnt','ppltn_cnt_prcs','idx_vl']]


walk_df(pk_gl); gl_walk = walk;
walk_df(pk_ch); ch_walk = walk;
walk_df(pk_sm); sm_walk = walk;
del walk

# 합본
walk_fin = pd.concat([gl_walk,ch_walk, sm_walk], axis=0)
walk_fin.reset_index(inplace=True, drop=True)
del gl_walk, ch_walk, sm_walk

gc.collect()


# walk_fin['crtr_yr'] = yr_now
walk_fin


# In[33]:


try : 
    walk_t = list(walk_fin.itertuples(index=False, name=None))
    
    result_query = """
        INSERT INTO tdm_ic_ctypark_wlkacsblt_idx (crtr_yr, mng_no, park_nm, park_se, sgg_cd, sgg, emd_cd, emd,
        reside_ppltn_cnt, dong_rsgst_ppltn_cnt, ppltn_cnt_prcs, idx_vl) \
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
    
    cur.executemany(result_query, walk_t)
    conn.commit()
    
    print('inserted new results into db')
    
    print('DB 적재 성공')
    
except Exception as e:
    print(e, '\n')
    print('Failed!')


# In[34]:


# 안전성

try :
    # db에 적재되어 있는 데이터 불러오기
    
    c_sql = """select * from tdm_ic_cctv_lcinfo where crtr_yr = (select max(crtr_yr) from tdm_ic_cctv_lcinfo
    where crtr_yr <= left(current_date::varchar,4));"""
    
    b_sql = """select * from tdm_safe_emgnc_bell_lcinfo where crtr_yr = (select max(crtr_yr) from tdm_safe_emgnc_bell_lcinfo
    where crtr_yr <= left(current_date::varchar,4));"""
    
    p_sql = """select * from tdm_112_sttemnt_dsctn where crtr_yr = (select max(crtr_yr) from tdm_112_sttemnt_dsctn
    where crtr_yr <= left(current_date::varchar,4)) and lat is not null;"""
    
    cctv = gpd.read_postgis(c_sql, conn, geom_col='geom', crs={'init':'epsg:4326'})
    bell = gpd.read_postgis(b_sql, conn, geom_col='geom', crs={'init':'epsg:4326'})
    
    police = pd.read_sql(p_sql, conn)
    police['geometry'] = police.apply(lambda row: Point([row['lot'], row['lat']]), axis=1)
    police = gpd.GeoDataFrame(police, geometry = 'geometry')
    police.crs = {'init':'epsg:4326'}
    
    print('안전성 관련 데이터 로드 성공')
    
except Exception as e:
    print(e)
    print('안전성 관련 데이터 로드 실패')


# In[35]:


# 경찰 데이터

police['trmn_clsf'] = police['trmn_clsf'].replace('비출동 종결','비출동종결')
police['trmn_clsf'] = police['trmn_clsf'].replace('"','')

anal_112 = police[(police['incdnt_asort']!='상담문의')&(police['incdnt_asort']!='내용확인불가')&(police['incdnt_asort']!='보호조치')&                  (police['incdnt_asort']!='분실습득')&(police['incdnt_asort']!='피싱사기')&(police['incdnt_asort']!='무전취식승차')&                  (police['incdnt_asort']!='기타_타기관')&(police['incdnt_asort']!='서비스요청')&(police['incdnt_asort']!='주거침입')&                  (police['incdnt_asort']!='FTX')&(police['incdnt_asort']!='경비업체요청')&(police['incdnt_asort']!='교통불편')&                  (police['incdnt_asort']!='가정폭력')&(police['incdnt_asort']!='아동폭력(가정내)')&(police['incdnt_asort']!='위험방지')&                  (police['incdnt_asort']!='소음')&(police['incdnt_asort']!='가출 등')&(police['incdnt_asort']!='실종(실종아동 등)')&                  (police['incdnt_asort']!='위험방지')&(police['incdnt_asort']!='아동학대(가정내)')&                  (police['incdnt_asort']!='아동학대(기타)')&(police['incdnt_asort']!='사기')&                   (police['incdnt_asort']!='도박')&(police['trmn_clsf']!='허위오인')]

anal_112.head()


# In[36]:


gl_cctv = gpd.sjoin(pk_gl, cctv, how='inner', op='intersects')#.drop(['index_right','crtr_yr_right','lot','lat'], axis=1)
gl_cctv.rename(columns = {'crtr_yr_left':'crtr_yr'}, inplace=True)

glc = gl_cctv.groupby(['crtr_yr','mng_no','park_nm','park_se']).count().reset_index()
glc = glc[['crtr_yr','mng_no','park_nm','park_se','cctv_nm']]
del gl_cctv

glc_fin = pd.merge(pk_gl.iloc[:,:4], glc, how='left', on=['crtr_yr','mng_no','park_nm','park_se'])
glc_fin = glc_fin.fillna(0.0)
glc_fin.rename(columns={'cctv_nm':'cctv_repr'}, inplace=True)
del glc


# bell
gl_bell = gpd.sjoin(pk_gl, bell[['crtr_yr','no','emgnc_bell_mng','wgs84_lat','wgs84_lo', 'geom']], how='inner', op='intersects')
gl_bell.rename(columns={'crtr_yr_left':'crtr_yr'}, inplace=True)
glb = gl_bell.groupby(['crtr_yr','mng_no','park_nm','park_se']).count().reset_index()

glb = glb[['crtr_yr','mng_no','park_nm','park_se','emgnc_bell_mng']]
del gl_bell

glb_fin = pd.merge(pk_gl.iloc[:,:4], glb, how='left', on=['crtr_yr','mng_no','park_nm','park_se'])
glb_fin = glb_fin.fillna(0.0)
glb_fin.rename(columns={'emgnc_bell_mng':'safe_emgnc_bell_repr'}, inplace=True)
del glb


# In[37]:


# police
gl_112 = gpd.sjoin(pk_gl, anal_112[['crtr_yr','no','rcpt_ymd','rcpt_hr','asort_clsf','incdnt_asort','geometry']],
                  how='left', op='intersects')
gl_112.rename(columns={'crtr_yr_left':'crtr_yr'}, inplace=True)
glpo = gl_112.groupby(['crtr_yr','mng_no','park_nm','park_se']).count().reset_index()
glpo = glpo[['crtr_yr','mng_no','park_nm','park_se','incdnt_asort']]
del gl_112


# In[38]:


def safe_cbp(buffer):
    # cctv
    gl_cctv = gpd.sjoin(buffer, cctv, how='inner', op='intersects')#.drop(['index_right','crtr_yr_right','lot','lat'], axis=1)
    gl_cctv.rename(columns = {'crtr_yr_left':'crtr_yr'}, inplace=True)

    glc = gl_cctv.groupby(['crtr_yr','mng_no','park_nm','park_se']).count().reset_index()
    glc = glc[['crtr_yr','mng_no','park_nm','park_se','cctv_nm']]
    del gl_cctv

    glc_fin = pd.merge(buffer.iloc[:,:4], glc, how='left', on=['crtr_yr','mng_no','park_nm','park_se'])
    glc_fin = glc_fin.fillna(0.0)
    glc_fin.rename(columns={'cctv_nm':'cctv_repr'}, inplace=True)
    del glc


    # bell
    gl_bell = gpd.sjoin(buffer, bell[['crtr_yr','no','emgnc_bell_mng','wgs84_lat','wgs84_lo', 'geom']], how='inner', op='intersects')
    gl_bell.rename(columns={'crtr_yr_left':'crtr_yr'}, inplace=True)
    glb = gl_bell.groupby(['crtr_yr','mng_no','park_nm','park_se']).count().reset_index()

    glb = glb[['crtr_yr','mng_no','park_nm','park_se','emgnc_bell_mng']]
    del gl_bell

    glb_fin = pd.merge(buffer.iloc[:,:4], glb, how='left', on=['crtr_yr','mng_no','park_nm','park_se'])
    glb_fin = glb_fin.fillna(0.0)
    glb_fin.rename(columns={'emgnc_bell_mng':'safe_emgnc_bell_repr'}, inplace=True)
    del glb

    
    # police
    gl_112 = gpd.sjoin(buffer, anal_112[['crtr_yr','no','rcpt_ymd','rcpt_hr','asort_clsf','incdnt_asort','geometry']],
                      how='left', op='intersects')
    gl_112.rename(columns={'crtr_yr_left':'crtr_yr'}, inplace=True)
    glpo = gl_112.groupby(['crtr_yr','mng_no','park_nm','park_se']).count().reset_index()
    glpo = glpo[['crtr_yr','mng_no','park_nm','park_se','incdnt_asort']]
    del gl_112

    glpo_fin = pd.merge(buffer.iloc[:,:4], glpo, how='left', on=['crtr_yr','mng_no','park_nm','park_se'])
    glpo_fin.rename(columns={'incdnt_asort':'dclr_nocs_112'}, inplace=True)
    glpo_fin['dclr_nocs_112'] = glpo_fin['dclr_nocs_112'].fillna(0.0)
    del glpo

    dfs = [glc_fin, glb_fin, glpo_fin]
    
    glcb = reduce(lambda left, right: pd.merge(left, right, on=['crtr_yr','mng_no','park_nm','park_se']), dfs)
    
    glcb['scr_sum'] = glcb['cctv_repr'] + glcb['safe_emgnc_bell_repr']
    glcb['sum_ajmt'] = round( ((glcb['scr_sum'] - glcb['scr_sum'].min(axis=0))/
                                              ( glcb['scr_sum'].max(axis=0) - glcb['scr_sum'].min(axis=0))*100),2)
    
    glcb['dclr_nocs_ajmt'] = round( abs( ((glcb['dclr_nocs_112'] - glcb['dclr_nocs_112'].min(axis=0))/
                                              ( glcb['dclr_nocs_112'].max(axis=0) - glcb['dclr_nocs_112'].min(axis=0)))-1)*100,2)
    
    glcb['ajmt_sum'] = glcb['sum_ajmt'] + glcb['dclr_nocs_ajmt']
    glcb['idx_vl'] = round( ((glcb['ajmt_sum'] - glcb['ajmt_sum'].min(axis=0))/
                                              ( glcb['ajmt_sum'].max(axis=0) - glcb['ajmt_sum'].min(axis=0)))*100,2)
    
    global glcb_fin
    glcb_fin = pd.merge(glcb, park[['crtr_yr','mng_no','park_nm','park_se','lctn_lotno_addr','sgg_cd','sgg','emd_cd','emd']],
         how='left', on=['crtr_yr','mng_no','park_nm','park_se'])
    glcb_fin.rename(columns={'lctn_lotno_addr':'addr'}, inplace=True)
    
    glcb_fin = glcb_fin[['crtr_yr','mng_no','park_nm','park_se','sgg_cd','sgg','emd_cd','emd','addr','cctv_repr',
                        'safe_emgnc_bell_repr','scr_sum','sum_ajmt','dclr_nocs_112','dclr_nocs_ajmt','ajmt_sum','idx_vl']]
    
    gc.collect()


# In[39]:


safe_cbp(pk_gl); gl_cb = glcb_fin

safe_cbp(pk_ch); ch_cb = glcb_fin
safe_cbp(pk_sm); sm_cb = glcb_fin
del glcb_fin

gc.collect()

safe_fin = pd.concat([gl_cb,ch_cb,sm_cb], axis=0)
# safe_fin['crtr_yr'] = yr_now

safe_fin


# In[40]:


try : 
    # db 연결

    server=pd.read_csv('./icbpadmin_db.txt', header=None, names=['db','key'])
    product_connection_string = "dbname={dbname} user={user} host={host} password={password} port={port}".format(dbname=server['key'][0],
                                                         user=server['key'][1], host=server['key'][2],
                                                         password=server['key'][3], port=server['key'][4])
    conn=pg2.connect(product_connection_string)
    cur=conn.cursor()
    
    print('DB 연결 성공')

except Exception as e:
    print(e)
    print('DB 연결 실패')


# In[41]:


try :
    sff = list(safe_fin.itertuples(index=False, name=None))
    
    ins_sql = """
    insert into tdm_ic_ctypark_safety_idx (crtr_yr, mng_no, park_nm, park_se, sgg_cd, sgg, emd_cd, emd, addr,
    cctv_repr, safe_emgnc_bell_repr, scr_sum, sum_ajmt, dclr_nocs_112, dclr_nocs_ajmt, ajmt_sum, idx_vl) VALUES (%s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
    
    cur.executemany(ins_sql, sff); conn.commit()
    print('inserted new results into db');
    
    conn.close()
    print('DB 연결 끊기')
    
except Exception as e:
    print(e)
    print('Failed!')


# In[42]:


print(len(fac_ff), len(cmf_fin), len(trans_fin), len(walk_fin), len(safe_fin))

print(len(fac_ff.columns), len(cmf_fin.columns), len(trans_fin.columns), len(walk_fin.columns), len(safe_fin.columns))


# In[168]:


print('도시공원서비스 지표 스크립트 끝')
