'''
提交申請
'''
from sqlalchemy import create_engine
import pandas as pd
import pyodbc
import json
import requests
import datetime
import os
import time
from glob import glob
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import random   
import datetime as dt




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




date_1 = datetime.now() - timedelta(days=1)
date_1_scrm = int(date_1.timestamp() * 1000)

'''
select from TW
'''
url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
headers = {
    "Authorization": f"Bearer {ac_token_TWOS}",
    "Content-Type":"application/x-www-form-urlencoded"
    # Replace with your actual access token
}
queryLocator = ''
Tasks_df = pd.DataFrame()
 
while True:
    data = {
        "xoql": f'''
                  select id, name, customItem10__c 
                            from customEntity14__c 
                            where createdBy = '3339579481364832' 
                            and createdAt >= {date_1_scrm} 
                            and (approvalStatus = '待提交')
                  ''',
        "batchCount": 2000,
        "queryLocator": queryLocator
    }
    response = requests.post(url_2, headers=headers, data=data)
    crm = response.json()
    data = pd.DataFrame(crm["data"]["records"])
    Tasks_df = pd.concat([Tasks_df, data], ignore_index=True, sort=False)
    
    if not crm['queryLocator']:
        break
    queryLocator = crm['queryLocator']
    
status_url = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/creekflow/task/actions/preProcessor"  #獲取下一步訊息
task_url = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/creekflow/task"                         #審批
headers = {
        "Authorization": f"Bearer {ac_token_TWOS}",
        "Content-Type": "application/json"
    }

def preProcessor(process_ID):
    status_body = {
        "data": {
            "action": "submit",
            "entityApiKey": "customEntity14__c",
            "dataId": process_ID }}

    response = requests.post(status_url, headers=headers, json=status_body)
    crm_json = response.json()['data']
    return crm_json

tasks_df_last_row = Tasks_df.iloc[-1]
approval_status = preProcessor(tasks_df_last_row['id'])


def submit_task(row):
    data_id = row['id']
    task_id = row['customItem10__c']
    url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/creekflow/task"
    headers = {
        "Authorization": f"Bearer {ac_token_TWOS}",
        "Content-Type": "application/json"
    }

    # Updated nextAssignees/ccs list

    data = {
        "data": {
            "action": "submit",
            "entityApiKey": "customEntity14__c",
            "dataId": data_id,
            "procdefId": approval_status['procdefId'],
            "nextTaskDefKey": approval_status['nextTaskDefKey'],
            "nextAssignees": [task_id],
            "ccs": [task_id]  
        }
    }

    response = requests.post(url_2, headers=headers, json=data)
    result = response.json()
    print(f"Response for dataId {data_id}: {result}")

# Assuming data_ids is your DataFrame
data_ids_df = Tasks_df[['id', 'customItem10__c']]

# Create a thread for each row in the DataFrame

threads = []
for index, row in data_ids_df.iterrows():
    thread = threading.Thread(target=submit_task, args=(row,))
    threads.append(thread)
    thread.start()
    time.sleep(0.08)

# Wait for all threads to finish
for thread in threads:
    thread.join()