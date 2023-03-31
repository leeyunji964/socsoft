#!/usr/bin/env python
# coding: utf-8

# In[78]:


import pandas as pd, numpy as np
import os
import re
import nltk
from PyKomoran import *
from konlpy.tag import *
from tqdm import tqdm
from wordcloud import WordCloud
from collections import Counter
from konlpy.tag import Okt
# from PIL import Image
import operator
import warnings
warnings.filterwarnings(action='ignore')
from sqlalchemy import create_engine
from urllib.parse import quote_plus  as urlquote
import psycopg2 as pg2
import datetime


# In[79]:


try : 
    # db 연결

    server=pd.read_csv('./icbpadmin_db.txt', header=None, names=['db','key'])
    product_connection_string = "dbname={dbname} user={user} host={host} password={password} port={port}".format(dbname=server['key'][0],
                                                                                                                user=server['key'][1],
                                                                                                                host=server['key'][2],
                                                                                                                password=server['key'][3],
                                                                                                                port=server['key'][4])
    conn=pg2.connect(product_connection_string)
    cur=conn.cursor()
    
    print('민원 DB 연결 성공')

except Exception as e:
    print(e)
    print('민원 DB 연결 실패')


# In[80]:


try : # tdw, tdm의 연월 가장 최근 값 불러오기
#     test_ym = """select replace(left(max(minwon_reg_dt),7),'-','') as ym from minwon_test_table"""
#     cur.execute(test_ym); conn.commit()
#     mw_ym = pd.read_sql(test_ym,conn)
    
    sw_ym = """select replace(left(max(minwon_reg_dt),7),'-','') as ym from saeol_minwon;"""
    kw_ym = """select max(crtr_ym) from tdm_ic_park_cvlcpt_kwrd;"""
      
    cur.execute(sw_ym); conn.commit()
    cur.execute(kw_ym); conn.commit()

    mw_ym = pd.read_sql(sw_ym,conn);
    kw_ym = pd.read_sql(kw_ym, conn)
    
    mym = int(mw_ym['ym'][0]); kwm = int(kw_ym['max'][0])
    
    print('tdw std_ym : ', mw_ym, '\n', 'tdm crtr_ym : ', kw_ym)

except Exception as e:
    print(e)
    print('Failed')


# In[83]:


try :
    # mym과 kwm이 같다면 : 즉, 코드가 이미 한 번 이상 실행되었다면
    if mym <= kwm:
        print('mym과 kwm 동일하므로 kwm 최근 값 삭제')
        
        d_sql = """
        delete from tdm_ic_park_cvlcpt_kwrd where crtr_ym = (select max(crtr_ym) from tdm_ic_park_cvlcpt_kwrd
        where crtr_ym <= replace(left(current_date::varchar,7),'-',''));
        """
        
        cur.execute(d_sql, conn); conn.commit()
        print('del duplicated keyword data')
        
    elif mym > kwm:
        print('아직 코드가 수행되지 않았으므로 kwm 최근 값 삭제하지 않음')
        pass
    
except Exception as e:
    print(e)
    print('failed')


# In[84]:


try :
    # db에 적재되어 있는 데이터 불러오기
    
#     sql = """
#     select * from minwon_test_table where case when right(replace(left(current_date::varchar,7),'-',''),2)='01' then
#     replace(left(minwon_reg_dt,7),'-','') = (((left(current_date::varchar,4))::int-1)::varchar)||'12'
#     else replace(left(minwon_reg_dt,7),'-','') = ((replace(left(current_date::varchar,7),'-',''))::int-1)::varchar END
#     """
    
    sql = """
    select * from saeol_minwon where case when right(replace(left(current_date::varchar,7),'-',''),2)='01' then
    replace(left(minwon_reg_dt,7),'-','') = (((left(current_date::varchar,4))::int-1)::varchar)||'12'
    else replace(left(minwon_reg_dt,7),'-','') = ((replace(left(current_date::varchar,7),'-',''))::int-1)::varchar END
    """
    
    minwon=pd.read_sql(sql, conn)
    minwon.insert(2,'sgg','')
    
    for i in range(len(minwon)):
        minwon['sgg'][i] = minwon['provd_instt_nm'][i][6:]
    
    print('민원 데이터 로드 성공')
    print('민원 데이터 길이 : ', len(minwon))
    
    if len(minwon) == 0:
        print('데이터가 존재하지 않습니다.')
        print('db 연결을 해제합니다.')
        
        cur.close()
        conn.close()
    
except Exception as e:
    print(e)
    print('민원 데이터 로드 실패')


# In[76]:


# 한글, 띄어쓰기 제외 모두 제거
minwon['cleansing_mw'] = ''

if len(minwon) >= 1:
    for i in tqdm(range(len(minwon))):
        stemmed = re.sub(r"[^\uAC00-\uD7A3\s]", "", str(minwon['minwon_cn'][i]))#한글, 띄어쓰기 제외하고 삭제
        stemmed = stemmed.replace('\n','').replace('\t','').replace('\r','')
        minwon['cleansing_mw'][i] = stemmed
        
    mw_park = minwon[minwon['cleansing_mw'].str.contains('공원')].reset_index(drop=True)
    mw_park.insert(0,'mw_ym','')
    #display(mw_park.head())

    for y in range(len(mw_park)):
        mw_park['mw_ym'][y] = mw_park['minwon_reg_dt'][y][:7].replace('-','')

    ym_list = list(mw_park.mw_ym.unique())
    sgg_list = list(mw_park.sgg.unique())
    
else:
    print('민원 데이터 row 수가 0 입니다.')
    print('db 연결을 해제합니다.')
    
    cur.close()
    conn.close()


# In[77]:


temp = pd.DataFrame()

try :
    if len(ym_list) >= 1:
        for ym in tqdm(ym_list):
            dt = mw_park[mw_park['mw_ym'] == ym].reset_index(drop=True)
            #print(dt.mw_ym.unique())
            
            for p in sgg_list:
                #print(p)
                if len(dt[dt['sgg']==p]) >= 1:
                    dt2 = dt[dt['sgg']==p].reset_index(drop=True)
                    #print(dt2.mw_ym.unique())
                    okt = Okt()
                    nouns_li = []

                    # 명사만 추출
                    for j in dt2['cleansing_mw']:
                        nouns = okt.nouns(j)
                        nouns_li.extend(nouns)

                    # 불용어 불러오기
                    
                    with open('./mw_stop_words.txt','r', encoding='utf-8-sig') as file:
                        strings = file.readlines()
                    stop_words = [s.replace('\n','') for s in strings]

                    # 불용어 제거
                    result = [word for word in nouns_li if not word in stop_words and len(word) > 1]

                    Count = Counter(result)
                    items = sorted(Count.items(), key=operator.itemgetter(1), reverse=True)

                    mw_words = pd.DataFrame(items)
                    mw_words.columns = ['word','frequency']


                    global dfs
                    dfs = mw_words
                    dfs.insert(0,'mw_ym','')

                    #print('dt2.mw_ym.unique : ', dt2.mw_ym.unique())
                    dfs['mw_ym'] = dt2['mw_ym'][0]
                    #print("dt2['mw_ym'][0] : ", dt2['mw_ym'][0])

                    dfs['sgg'] = p

                    temp = pd.concat([temp, dfs], axis=0)

                    #display(temp)
                    temp.reset_index(drop=True, inplace=True)
                else:
                    pass
        print(ym + ' 민원 def 만들기 성공')
        
        temp.sort_values(['mw_ym','sgg'], inplace=True)
        temp.reset_index(inplace=True, drop=True)
        temp = temp[['mw_ym','word','sgg','frequency']]
        temp.columns = ['crtr_ym','wrd','gungu','repr']

        print(temp)
        
        temp_t = list(temp.itertuples(index=False, name=None))
        
        # db 적재
        try : 
            result_query = """
                insert into tdm_ic_park_cvlcpt_kwrd (crtr_ym, wrd, gungu, repr) values (%s, %s, %s, %s) """

            cur.executemany(result_query, temp_t)
            conn.commit()

            print('DB 적재 성공')
            print('민원 스크립트 끝')

            # db 연결 끊기
            cur.close()
            conn.close()
            print('DB 연결 해제 성공')


        except Exception as e:
            print(e, '\n')
            print('DB 적재 실패')
        
    else:
        print('민원 데이터 길이가 0 입니다.')
        print('DB에 적재할 데이터가 없습니다.')
        pass
    
        cur.close()
        conn.close()
        print('DB 연결 해제')

except Exception as e:
    print('민원 def 만들기 실패')
    print(e)
