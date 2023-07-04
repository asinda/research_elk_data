# -*- coding: utf-8 -*-

import os
import json
import requests
import base64
from datetime import datetime, timedelta


def get_data_from_elasticsearch(start_date, end_date, from_, size):
    base_url = "http://localhost:9200/logstash-p-test-*/_search"
    
    # Print base URL for debugging
    print(base_url)
    
    # Authentication credentials
    username = 'elastic'
    password = 'changeme'

    # Base64 encode the username and password
    credentials = base64.b64encode('{}:{}'.format(username, password).encode('utf-8')).decode('utf-8')
    headers = {
        'Authorization': 'Basic {}'.format(credentials),
        'Content-Type': 'application/json'
    }
    
    # Build the Elasticsearch query
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "timestamp": {
                                "gte": start_date.strftime('%Y-%m-%d'),
                                "lt": end_date.strftime('%Y-%m-%d')
                            }
                        }
                    },
                    {
                        "match": {
                            "content": "example"
                        }
                    }
                ]
            }
        },
        "from": from_,
        "size": size
    }
  
    try:
        # Send the HTTP request to Elasticsearch
        response = requests.get(base_url, headers=headers, json=query, verify=False)
        response.raise_for_status()  # Raise an exception if the response status is not successful
        return response.json()
    except requests.exceptions.RequestException as e:
        # Handle request exceptions
        print("Error during the API request:", e)
        return None

def main(start_date, end_date, batch_size=40):
    from_ = 0
    total_documents = batch_size
    filename2 = ""
    existing_documents = set()
    
    while from_ < total_documents: 
        data = get_data_from_elasticsearch(start_date, end_date, from_, batch_size)
        
        if data is None:
            # Handle the case when the API request fails
            print("Failed to retrieve data from Elasticsearch.")
            break
        
        total_documents = data['hits']['total']['value']
        print(total_documents)
        
        for hit in data['hits']['hits']:   
            timestamp = hit['_source']['timestamp']
            timedata = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
            timestamp_data = timedata.strftime('%Y-%m-%d')
            filename = 'data_{}.json'.format(timestamp_data)         

            if filename != filename2:
                if 'f' in locals():
                    f.close()
                f = open(filename, "a")

            document_id = hit['_id']
            if document_id not in existing_documents:
                try:
                    json.dump(hit['_source'], f) 
                    f.write("\n")
                    existing_documents.add(document_id)
                    print(existing_documents)
                except Exception as e:
                    # Handle the case when writing to the file fails
                    print("Failed to write data to file:", e)
            
            filename2 = filename
        
        from_ += batch_size
        print(from_)
    
    if 'f' in locals():
        f.close()


if __name__ == "__main__":
    start_date = datetime(2023, 6, 27)
    end_date = datetime(2023, 7, 22)
    main(start_date, end_date)
