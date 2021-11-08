import requests
import json
import time
import sys
import datetime
import pyodbc  

import logging

import azure.functions as func
from myfunc import api_call1, api_call2 , api_endpoint,headers, db_connect, db_insert, logic
 
# SQL CONNECTION

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=cvpcotestsql01.database.windows.net,1433;DATABASE=CVPCOtestDB01;UID=sqladmin;PWD=sq1DW@fe3GR$ht5JY^')




# api_call1 

query = '{allPractices{practiceId:id sourceId}}'

res = requests.post(api_endpoint, headers=headers, data={'query': query}).text
response = json.loads(res)
data = response['data']['allPractices']
print(data)


def api_call2(practice_id, source_id, offset):
    loop = True
    while loop:
        startdate = '\"2021-08-01 04:00:00\"' 
        enddate = '\"2021-09-01 04:00:00\"'

        query = "{allTransactions(sourceId:" + str(source_id) + " filter:{practiceId:" +\
                        str(practice_id) + " typeIn:[REVENUE_ADJUSTMENT,INVOICE]" + " postedAtGTE: " + startdate +"postedAtLTE: " + enddate +\
                        "} first:1000 skip:" + str(offset) + "){sourceId practiceId " + \
                        "postedAt transactionId clientId amount type } }"
        print(query)
        response = requests.post(api_endpoint, headers=headers, data={'query': query}).text
        response = json.loads(response)
        print(response)
        print("*"* 150)
        data = response['data']['allTransactions']
      
        db_insert(data)
        offset += 1000
        
        if response['extensions']['remaining'] <= 0:
            time.sleep(1)

        if response['extensions']['cost'] == 0:
            loop = False



def logic(data):
    offset = 0
    for res in data:
        api_call2(res['practiceId'], res['sourceId'], offset)


def db_insert(datas):
    conn = db_connect()
    cur = conn.cursor()
    
    sql = """INSERT INTO testdummy("practiceId", "sourceId", postedAt, "transactionId", "clientId","amount","type")
     VALUES (?,?,?,?,?,?,?) ; """  
    for data in datas:
 
        cur.execute(sql,
                    (data["practiceId"], data["sourceId"], data["postedAt"], data["transactionId"], data["clientId"],
                     data["amount"], data["type"]))
        
        conn.commit()
    cur.close()
    return 'Done'


if __name__ == "__main__":
    first_res = api_call1()
    logic(first_res)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}.  This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
