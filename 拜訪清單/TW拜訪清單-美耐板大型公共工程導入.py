import pandas as pd
import networkx as nx
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
os.chdir("C:/Users/11021300/Documents/拜訪清單/2503")

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


########## 客戶 ##########
select_query = f'''
SELECT accountCode__c 公司代號
, accountName 公司全名
, SAP_CompanyID__c sap公司代號
, dimDepart.departName 資料區域名稱
, customItem202__c 公司地址
, customItem322__c 目標客戶類型
, customItem198__c.name 公司型態
, SAP_CompanyID__c sap公司代號
, customItem278__c 倒閉無效
, customItem291__c 勿擾選項
FROM account'''
account = query_CRM(select_query)
data_account = account[:]

data_account['公司全名'] = data_account.apply(lambda x: re.sub(r'[\[\]\"\'\(\)]', '', str(x['公司全名'])), axis=1)
data_account['目標客戶類型'] = data_account.apply(lambda x: re.sub(r'[\[\]\"\'\(\)]', '', str(x['目標客戶類型'])), axis=1)
data_account['倒閉無效'] = data_account.apply(lambda x: re.sub(r'[\[\]\"\'\(\)]', '', str(x['倒閉無效'])), axis=1)
data_account['勿擾選項'] = data_account.apply(lambda x: re.sub(r'[\[\]\"\'\(\)\,]', '', str(x['勿擾選項'])), axis=1)
data_account = data_account.drop_duplicates('公司代號')

company_name = ['倒閉', '歇業', '停業', '轉行', '退休', '過世', '燈箱', '群組', '支援', '留守', '教育訓練', '無效拜訪', '資料不全', '搬遷', '廢止', 
                '解散', '管制', '非營業中']
data_account_filtered = data_account.loc[(~data_account['公司全名'].str.contains('|'.join(company_name), na=False)) &\
                                (data_account['倒閉無效'] != '是') &\
                                ~data_account['資料區域名稱'].str.contains('INV|888|LB|CPT|PD|Z1|Z2|Z3|Z4') &\
                                data_account['資料區域名稱'].str.contains('TW') &\
                                data_account['公司型態'].str.contains('KD') &\
                                ~data_account['勿擾選項'].str.contains('勿拜訪') &\
                                ~data_account['公司地址'].str.contains('金門｜澎湖｜馬祖')]


########## 關係網路 ##########
select_query = f'''
SELECT customItem11__c 公司代號1
, customItem10__c 公司代號2
, customItem8__c 層級說明
FROM customEntity53__c'''
network = query_CRM(select_query)
data_network = network[:]

data_network['層級說明'] = data_network.apply(lambda x: re.sub(r'[\[\]\"\']', '', str(x['層級說明'])), axis=1)
data_network = data_network.loc[data_network['層級說明'] == "主要聯繫客戶"]
data_network_duplicate = data_network.loc[data_network['公司代號1'] == (data_network['公司代號2'])]
data_network_duplicate['related_company'] = data_network_duplicate['公司代號1']
data_network = data_network.loc[data_network['公司代號1'] != (data_network['公司代號2'])]

parent_map = dict(zip(data_network["公司代號2"], data_network["公司代號1"]))
def find_root(company, max_iterations=5):
    count = 0
    while company in parent_map:
        company = parent_map[company]
        count += 1
        if count >= max_iterations:
            return None
    return company
data_network["related_company"] = data_network["公司代號2"].apply(lambda x: find_root(x, 5))

data_network = pd.concat([data_network, data_network_duplicate], ignore_index=True)
data_network = data_network.drop_duplicates('related_company')






# ########## 關聯公司明細 ##########
# select_query = f'''
# SELECT customItem15__c 公司代號1
# , customItem10__c 公司代號2
# FROM customEntity64__c'''
# connect = query_CRM(select_query)
# data_connect = connect[:]


# ##建立查找字典
# parent_map = dict(zip(data_connect["公司代號2"], data_connect["公司代號1"]))
# ##定義函數，回溯最上層公司
# def find_root(company):
#     while company in parent_map:
#         company = parent_map[company]
#     return company
# # 應用函數來查找每個公司代號2的最上層公司
# data_connect["top_company"] = data_connect["公司代號2"].apply(find_root)



########## 客戶關係聯絡人 ##########
select_query = f'''
SELECT customItem8__c 公司代號
, customItem24__c 關係狀態
, customItem50__c 空號
, customItem51__c 停機
, customItem42__c 聯絡人資料無效
, customItem89__c 號碼錯誤非本人
FROM customEntity22__c'''
rel_contact = query_CRM(select_query)
data_rel_contact = rel_contact[:]

data_rel_contact['關係狀態'] = data_rel_contact.apply(lambda x: re.sub(r'[\[\]\"\'\(\)]', '', str(x['關係狀態'])), axis=1)
data_rel_contact['號碼錯誤非本人'] = data_rel_contact.apply(lambda x: re.sub(r'[\[\]\"\'\(\)]', '', str(x['號碼錯誤非本人'])), axis=1)
data_rel_contact['聯絡人資料無效'] = data_rel_contact.apply(lambda x: re.sub(r'[\[\]\"\'\(\)]', '', str(x['聯絡人資料無效'])), axis=1)
data_rel_contact = data_rel_contact.loc[
    (data_rel_contact['關係狀態'].str.contains('在職', na=False)) & 
    (data_rel_contact['空號'].isin(['0', ''])) & 
    (data_rel_contact['停機'].isin(['0', ''])) & 
    (data_rel_contact['聯絡人資料無效'].isin(['否', ''])) &
    (data_rel_contact['號碼錯誤非本人'].isin(['否', '', 'nan']))]
data_rel_contact = data_rel_contact.drop_duplicates('公司代號')


########## 近三個月拜訪追蹤紀錄 ##########
now = pd.Timestamp.now(tz="UTC")
three_months_ago = now - pd.DateOffset(months=3)
start_date_track = three_months_ago.timestamp() * 1000

select_query = f'''
SELECT accountCode__c 公司代號
, customItem128__c 觸客類型
, createdAt 創建日期
FROM customEntity15__c
WHERE createdAt >= {start_date_track}'''
track = query_CRM(select_query)
data_track = track[:]

data_track['創建日期'] = data_track['創建日期'].apply(fn_datetime)
data_track['創建日期'] = pd.to_datetime(data_track['創建日期'])
data_track['觸客類型'] = data_track.apply(lambda x: re.sub(r'[\[\]\"\'\(\)]', '', str(x['觸客類型'])), axis=1)
data_track = data_track.loc[data_track['觸客類型'].str.contains('A1')]
data_track = data_track.drop_duplicates("公司代號")



########## 近一年拜訪追蹤紀錄 ##########
now = pd.Timestamp.now(tz="UTC")
six_months_ago = now - pd.DateOffset(months=6)
start_date_track_months = six_months_ago.timestamp() * 1000

select_query = f'''
SELECT accountCode__c 公司代號
, customItem128__c 觸客類型
, createdAt 創建日期
FROM customEntity15__c
WHERE createdAt >= {start_date_track_months}
AND createdAt <= {start_date_track}'''
track_months = query_CRM(select_query)
data_track_months = track_months[:]

data_track_months['創建日期'] = data_track_months['創建日期'].apply(fn_datetime)
data_track_months['創建日期'] = pd.to_datetime(data_track_months['創建日期'])
data_track_months['觸客類型'] = data_track_months.apply(lambda x: re.sub(r'[\[\]\"\'\(\)]', '', str(x['觸客類型'])), axis=1)
data_track_months = data_track_months.loc[data_track_months['觸客類型'].str.contains('A1')]
data_track_months = data_track_months.sort_values(by="創建日期", ascending=False)
data_track_months = data_track_months.drop_duplicates("公司代號")


#############################
##step1:合併客戶公司代號到關係網路公司代號-2,並用公司代號-1取代公司代號-2##
final_data1 = data_network.merge(data_account[["公司代號"]], 
                                 left_on="公司代號2", right_on="公司代號", how='left')
final_data1 = final_data1[['related_company']]
final_data1 = final_data1.drop_duplicates('related_company')

##step2:合併final_data1與data_account取得戶資料##
final_data2 = final_data1.merge(data_account_filtered[["公司代號", "公司全名","sap公司代號", "資料區域名稱", "公司地址", "公司型態", "目標客戶類型"]], 
                              left_on="related_company", right_on="公司代號", how='left')
final_data2 = final_data2.loc[~final_data2['公司代號'].isna()]
final_data2 = final_data2.drop(columns=['公司代號'])

filtered_data = data_account_filtered.loc[~data_account_filtered['公司代號'].isin(data_network['related_company'])]
filtered_data = filtered_data.drop(columns=['倒閉無效', '勿擾選項'])
filtered_data = filtered_data.rename(columns={'公司代號': 'related_company'})

final_data2 = pd.concat([final_data2, filtered_data])


##step3:final_data2公司代號排除無效聯絡人
final_data3 = final_data2.loc[final_data2['related_company'].isin(data_rel_contact['公司代號'])]

##step4:final_data3排除近三個月有拜訪的客戶並對追蹤紀錄創建日期排序(遠到近)##
final_data4 = final_data3.loc[~final_data3['related_company'].isin(data_track['公司代號'])]
final_data4 = final_data4.merge(data_track_months[["公司代號", "創建日期"]], left_on="related_company", right_on="公司代號", how='left')
final_data4 = final_data4.drop(columns=['公司代號'])
final_data4 = final_data4.sort_values(by="創建日期", ascending=True, na_position='first')
final_data4 = final_data4.drop_duplicates("related_company")

final_data4['主旨'] = "(指標)美耐板大型公共工程導入"


#####信用管制#####
control_data = pd.read_excel("C:/Users/11021300/Documents/拜訪清單/SAP信用管制_2503.xlsx")
control_data = control_data.loc[control_data['客戶編號'].str.contains("TW") &\
                                control_data['信用管制說明'].str.contains("管制")]

final_data4 = final_data4.loc[~final_data4['sap公司代號'].isin(control_data['客戶編號'])]

final_data4.to_excel("美耐板大型公共工程導入.xlsx", index=False)