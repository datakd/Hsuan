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


# os.system("cls")
# os.getcwd()
# os.chdir("/Users/Hsuan/Desktop/KD/獎金計算")


########################### CRM Setting ###########################
###################################################################
userID = "11021300@twkd.com"
pwd = "Kd110213001998"
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



########################### SAP ###########################
###########################################################





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

first_day = dt.datetime.today().replace(day=1)

sys_staff = data_staff.loc[
    (data_staff['失效日期'].isna() | (data_staff['失效日期'] >= first_day)) &
    ((data_staff['獎金用職級'] == '系統櫃外勤業務'))]
sys_staff = sys_staff.drop_duplicates(subset=['員工編號'])





########## 追蹤紀錄 ##########
############################
now = pd.Timestamp.now(tz="UTC") 
start_date_K = pd.Timestamp(year=now.year, month=now.month, day=1, tz="UTC").timestamp() * 1000


select_query = f'''
SELECT name
, customItem126__c 創建人
, createdAt 創建日期
, customItem4__c 工作類別
FROM customEntity15__c
WHERE createdAt >= {start_date_K}'''
track_data = query_CRM(select_query)
track = track_data[:]

track['創建日期'] = track['創建日期'].apply(fn_datetime)
track['創建日期'] = pd.to_datetime(track['創建日期'])