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

data_staff['失效日期'] = data_staff['失效日期'].apply(fn_datetime)
data_staff['失效日期'] = pd.to_datetime(data_staff['失效日期'])
data_staff['獎金用職級'] = data_staff.apply(lambda x: re.sub(r'[\[\]\"\'\(\)\,]', '', str(x['獎金用職級'])), axis=1)

sys_staff = data_staff.loc[
    (data_staff['失效日期'].isna() | (data_staff['失效日期'] >= first_day)) &
    ((data_staff['獎金用職級'] == '系統櫃外勤業務'))]
sys_staff = sys_staff.drop_duplicates(subset=['員工編號'])





########## 追蹤紀錄 ##########
#############################
now = pd.Timestamp.now(tz="UTC") 
start_date_K = pd.Timestamp(year=now.year, month=now.month, day=1, tz="UTC").timestamp() * 1000


select_query = f'''
SELECT name id
, customItem126__c.name 創建人
, createdAt 創建日期
, customItem4__c 工作類別
FROM customEntity15__c
WHERE createdAt >= {start_date_K}'''
track = query_CRM(select_query)
track_data = track[:]

track_data['創建日期'] = track_data['創建日期'].apply(fn_datetime)
track_data['創建日期'] = pd.to_datetime(track_data['創建日期'])
track_data['工作類別'] = track_data.apply(lambda x: re.sub(r'[\[\]\"\'\(\)\,]', '', str(x['工作類別'])), axis=1)
track_data = track_data.loc[track_data['工作類別'].str.contains("G1-1|G1-2|G1-3")]

track_data = track_data.loc[track_data['創建人'].isin(sys_staff['人員姓名'])]




data = {'採購單分機': ['洪仁傑/GTR03759960', '洪仁傑/GTR03760133', '高孟賢/GTR00000001','高孟賢/GTR03760145/0.2', '高孟賢/GTR00000005/0.2','黃小明/GTR00000010'],
        '銷售金額':[1000,1000,1000,1000,1000,1000]}
fake_df = pd.DataFrame(data)

fake_df[['業務人員姓名', '工作代號', '拆分']] = fake_df['採購單分機'].str.split('/', expand=True)
fake_df = fake_df.loc[fake_df['業務人員姓名'].isin(sys_staff['人員姓名'])]

test_data = pd.read_excel("C:/Users/11021300/Desktop/系統櫃測試數據.xlsx")

sap_data = fake_df.merge(test_data, left_on='工作代號', right_on='id', how='left')
sap_data['拆分'] = pd.to_numeric(sap_data['拆分'], errors='coerce')


def calculate_performance(row):
    salesperson_performance = 0
    creator_performance = 0
    
    if row['業務人員姓名'] == row['創建人'] and row['工作代號'] == row['id'] and pd.isna(row['拆分']):
        salesperson_performance = row['銷售金額'] * 1
    elif row['業務人員姓名'] != row['創建人'] and row['工作代號'] == row['id'] and pd.isna(row['拆分']):
        salesperson_performance = row['銷售金額'] * 0.6
        creator_performance = row['銷售金額'] * 0.4
    elif pd.isna(row['id']):
        salesperson_performance = row['銷售金額'] * 1
    elif row['業務人員姓名'] != row['創建人'] and row['工作代號'] == row['id'] and row['創建人'] != '林怡伶':
        salesperson_performance = row['銷售金額'] * row['拆分']
        creator_performance = row['銷售金額'] * (1 - row['拆分'])
    elif row['創建人'] == '林怡伶':
        salesperson_performance = row['銷售金額'] * row['拆分']
    
    return pd.Series([salesperson_performance, creator_performance])


sap_data[['業務人員業績', '追蹤紀錄創建人業績']] = sap_data.apply(calculate_performance, axis=1)

sap_data1 = sap_data[['業務人員姓名', '業務人員業績']].rename(columns={'業務人員姓名': '姓名', '業務人員業績': '個人業績'})
sap_data2 = sap_data[['創建人', '追蹤紀錄創建人業績']].rename(columns={'創建人': '姓名', '追蹤紀錄創建人業績': '個人業績'})

sap_data_final = pd.concat([sap_data1, sap_data2], ignore_index=True)
sap_data_final = sap_data_final.groupby('姓名')['個人業績'].sum().reset_index()
sap_data_final = sap_data_final.loc[~sap_data_final['姓名'].isna() &\
                                    (sap_data_final['姓名'] != '林怡伶')]


warning_data = sap_data.loc[sap_data['創建人'].isna()]



