import pandas as pd
import numpy as np
import datetime as dt
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import os
import requests
# import mysql.connector
import pyodbc
import time
import re

os.system("cls")
os.getcwd()
os.chdir("/Users/meng/Desktop/KD/獎金計算")



####"####################### CRM Setting ###########################
###################################################################
userID = "11021300@twkd.com"
pwd = "Kd11021300"
security_token_TWOS = "AGLnwLr1"

#### p10 ####
url_token_TWOS = "https://login-p10.xiaoshouyi.com/auc/oauth2/token"
payload = {
  "grant_type" : "password",
  "client_id" : "bc4896f9e3e4fad682f9fe60d5fbaa2e",
  "client_secret" : "3407faf39a38b6821f18cdbe019a56e3",
  "username" : userID,
  "password" : pwd + security_token_TWOS
}

response = requests.post(url_token_TWOS, data=payload)
content = response.json()
ac_token_TWOS = content["access_token"]

## Header
header_TWOS = {"Authorization" : "Bearer " + ac_token_TWOS,
             "Content-Type" : "application/x-www-form-urlencoded"}
header_insert_TWOS = {"Authorization" : "Bearer " + ac_token_TWOS,
                    "Content-Type" : "application/json"}



#### datetime fn ####
def fn_datetime(ts):
  if(pd.isna(ts) or ts == ''):
      return pd.NaT
  else:
      ts = float(ts)/1000
      return dt.datetime.fromtimestamp(ts)

#### Query Function ####
url_select_TWOS = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"

def query_CRM(select_query, url_select = url_select_TWOS, header = header_TWOS):
    old_qloc = ''
    scrm_data = pd.DataFrame()
    while True:
        data = {
            "xoql": select_query,
            "batchCount": 2000,
            "queryLocator": old_qloc
        }
        response = requests.post(url_select, headers=header, data=data)
        crm = response.json()
        data = pd.DataFrame(crm["data"]["records"])
        scrm_data = pd.concat([scrm_data, data], ignore_index=True, sort=False)
        
        if not crm['queryLocator']:
            break
        old_qloc = crm['queryLocator']
    return pd.DataFrame(scrm_data)

########################### 人員資料 ###########################
###############################################################
now = pd.Timestamp.now(tz="UTC") 
start_date_K = pd.Timestamp(year=now.year, month=now.month, day=1, tz="UTC").timestamp() * 1000

select_query = f'''
SELECT customItem21__c 員工編號
, customItem22__c  人員姓名
, customItem10__c 失效日期
, customItem9__c 生效日期
, customItem25__c 獎金用職級
FROM customEntity31__c'''
TW_staff = query_CRM(select_query)
data_staff = TW_staff[:]


data_staff['失效日期'] = data_staff['失效日期'].apply(fn_datetime)
data_staff['失效日期'] = pd.to_datetime(data_staff['失效日期'])
data_staff['生效日期'] = data_staff['生效日期'].apply(fn_datetime)
data_staff['生效日期'] = pd.to_datetime(data_staff['生效日期'])
data_staff['獎金用職級'] = data_staff.apply(lambda x: re.sub(r'[\[\]\"\']', '', str(x['獎金用職級'])), axis=1)


first_day = dt.datetime.today().replace(day=1)
data_staff = data_staff.loc[
    (data_staff['失效日期'].isna() | (data_staff['失效日期'] >= first_day)) &
    ((data_staff['獎金用職級'] == '門市業務') | (data_staff['獎金用職級'] == '展示館兼職'))]
data_staff = data_staff.drop_duplicates(subset=['員工編號'])




##################################
########## 展示館預約資料 ##########
now = pd.Timestamp.now(tz="UTC") 
start_date_K = pd.Timestamp(year=now.year, month=now.month, day=1, tz="UTC").timestamp() * 1000


select_query = f'''
SELECT name
,customItem1__c 預約參訪日期
,customItem18__c.employeeCode 員編
,customItem18__c.name 接待人員
,customItem51__c 是否來訪
,customItem64__c 接待分鐘數
,customItem77__c 是否講解K大
,customItem81__c 講解分鐘數
,customItem29__c 展示館區域
FROM customEntity43__c
WHERE customItem1__c >= {start_date_K}'''
TWOS_exh = query_CRM(select_query)
data_exh = TWOS_exh[:]

data_exh['預約參訪日期'] = data_exh['預約參訪日期'].apply(fn_datetime)
data_exh['預約參訪日期'] = pd.to_datetime(data_exh['預約參訪日期'])
data_exh['接待分鐘數'] = pd.to_numeric(data_exh['接待分鐘數'])
data_exh['講解分鐘數'] = pd.to_numeric(data_exh['講解分鐘數'])
data_exh['是否講解K大'] = data_exh['是否講解K大'].str.get(0)
data_exh['是否來訪'] = data_exh['是否來訪'].str.get(0)


data_exh['接待人員'] = data_exh['接待人員'].replace("", float("nan"))
data_exh = data_exh.loc[data_exh['接待人員'].notna()]
data_exh = data_exh[data_exh['接待人員'].isin(data_staff['人員姓名'])]
data_exh = data_exh.merge(data_staff[["人員姓名", "獎金用職級"]], left_on="接待人員", right_on="人員姓名", how="left")
data_exh  = data_exh .drop(columns=["人員姓名"])

data_exh['交辦認列場次'] = data_exh.apply(
    lambda row: 0 if (row['獎金用職級'] == '展示館兼職') else
               0 if pd.notna(row['接待分鐘數']) and row['接待分鐘數'] <= 10 and row['獎金用職級'] == '門市業務' else
               0.3 if pd.notna(row['接待分鐘數']) and row['接待分鐘數'] <= 30 and row['獎金用職級'] == '門市業務' else
               0.8 if pd.notna(row['接待分鐘數']) and row['接待分鐘數'] <= 60 and row['獎金用職級'] == '門市業務' else
               1 if pd.notna(row['接待分鐘數']) and row['獎金用職級'] == '門市業務' else np.nan, axis=1)


data_exh['K大目標場次'] = data_exh['接待分鐘數'].apply(
    lambda x: 1 if x>0 else 0)

data_exh['K大認列場次'] = data_exh.apply(
    lambda row: 0.3 if pd.notna(row['講解分鐘數']) and row['講解分鐘數'] <= 10 and row['獎金用職級'] == '門市業務' else
               0.8 if pd.notna(row['講解分鐘數']) and row['講解分鐘數'] <= 20 and row['獎金用職級'] == '門市業務' else
               1 if pd.notna(row['講解分鐘數']) and row['講解分鐘數'] <= 30 and row['獎金用職級'] == '門市業務' else
               1.2 if pd.notna(row['講解分鐘數']) and row['獎金用職級'] == '門市業務' else 
               0 if pd.notna(row['講解分鐘數']) and row['講解分鐘數'] <= 10 and row['獎金用職級'] == '展示館兼職' else 
               0.5 if pd.notna(row['講解分鐘數']) and row['講解分鐘數'] <= 15 and row['獎金用職級'] == '展示館兼職' else
               1 if pd.notna(row['講解分鐘數']) and row['講解分鐘數'] <= 25 and row['獎金用職級'] == '展示館兼職' else
               2 if pd.notna(row['講解分鐘數'])  and row['獎金用職級'] == '展示館兼職' else np.nan, axis=1)

# data_exh.to_excel('data_exh.xlsx', index=False)


data_exh_final = pd.DataFrame()
data_exh_final = data_exh.groupby('接待人員').agg(
    交辦認列場次 =('交辦認列場次', 'sum'),
    K大目標場次 =('K大目標場次', 'sum'),
    K大認列場次 =('K大認列場次', 'sum')
).reset_index()

data_exh_final.merge(data_staff[["人員姓名", "獎金用職級"]], left_on="接待人員", right_on="人員姓名", how="left")

# data_exh_final.to_excel('data_exh_final.xlsx', index=False)


with pd.ExcelWriter("exh_bonus.xlsx") as writer:
    data_exh.to_excel(writer, sheet_name="pivot", index=False)
    data_exh_final.to_excel(writer, sheet_name="final", index=False)
