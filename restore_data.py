# -*- coding: utf-8 -*-

import os
import json
import requests
import base64
from datetime import datetime, timedelta


def get_data_from_elasticsearch(start_date, end_date, from_, size):
    base_url = "https://it-log-nadar.z3.r03.local:9200/restored-logstash-it-security-*/_search"
    
    print(base_url)
    # Authentication credentials
    username = 'elastic'
    password = 'w0RKbzBpz!lLXUTev4awBaOEUENmERhL'

    # Base64 encode the username and password
    credentials = base64.b64encode('{}:{}'.format(username, password).encode('utf-8')).decode('utf-8')
    headers = {
        'Authorization': 'Basic {}'.format(credentials),
        'Content-Type': 'application/json'
               }
    query = {

        "query": {
          "bool": {
            "must": [
              {
               "range": {
                "@timestamp": {
                    "gte": "2023-02-28",
                    "lt": "2023-03-22"
                }
               }
            },
           {
            "match": {
                "message": "marechac"
            }
             
          }
            ]
          }
        },
        "from": from_,
        "size": size
    }
  
    try:
        response = requests.get(base_url, headers=headers, json=query, verify=False)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print("Error during the API request:", e)
        return None

def main(start_date, end_date, batch_size=5000):
    from_ = 0
    total_documents = batch_size
    filename2 = ""
    while batch_size >= total_documents: 
        data = get_data_from_elasticsearch(start_date, end_date, from_, batch_size)
        total_documents = len(data['hits']['hits'])
        for hit in data['hits']['hits']:   
            timestamp = hit['_source']['@timestamp']
            timedata = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
            timestamp_data= timedata.strftime('%Y-%m-%d')
            filename = '/extract/data_{}.json'.format(timestamp_data)         
            if filename != filename2 :
                # if  f :
                #     f.close()
                f = open(filename, "w")
                        
            json.dumps(hit['_source'], f)      
            filename2 = filename

            from_ += batch_size


if __name__ == "__main__":
    start_date = datetime(2023, 2, 27)
    end_date = datetime(2023, 3, 22)
    main(start_date, end_date)
