# -*- coding: utf-8 -*-

import os
import json
import requests
import base64
from datetime import datetime, timedelta

def get_data_from_elasticsearch(start_date, end_date, from_=0, size=4):
    base_url = "http://localhost:9200/logstash-p-test-*/_search"
    
    # Authentication credentials
    username = 'elastic'
    password = 'changeme'

    # Base64 encode the username and password
    credentials = base64.b64encode('{}:{}'.format(username, password).encode('utf-8')).decode('utf-8')
    headers = {
        'Authorization': 'Basic {}'.format(credentials),
        'Content-Type': 'application/json'
               }
    query = {
        "query": {
            "range": {
                "timestamp": {
                    "gte": start_date.strftime('%Y-%m-%d'),
                    "lt": (start_date + timedelta(days=1)).strftime('%Y-%m-%d')
                }
            }
        },
        "from": from_,
        "size": size
    }

    try:
        response = requests.get(base_url, headers=headers, json=query)
        print(response.content)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print("Error during the API request:", e)
        return None

def save_data_to_json(data, folder_path, file_name):
    file_path = os.path.join(folder_path, file_name)
    with open(file_path, 'w') as json_file:
        json.dump(data, json_file)

def main(start_date, end_date, batch_size=4):
    current_date = start_date
    while current_date <= end_date:
        folder_name = current_date.strftime('%Y-%m-%d')
        folder_path = os.path.join(os.getcwd(), folder_name)
        try:
            os.makedirs(folder_path)
        except OSError:
            # le dossier existe
            pass
        from_ = 0
        total_documents = batch_size
        if total_documents >= batch_size:
            data = get_data_from_elasticsearch(current_date, current_date, from_, batch_size)
            for hit in data['hits']['hits']:
                total_documents = data.get('hits', {}).get('total', 0)
                print(total_documents)
                size = from_+batch_size
                if total_documents > 0:
                    file_name = "{}_{}-{}.json".format(current_date.strftime('%Y-%m-%d'), from_, size-1)
                    save_data_to_json(data, folder_path, file_name)
                    from_ += batch_size
                    size += size
        current_date += timedelta(days=1)

if __name__ == "__main__":
    start_date = datetime(2023, 6, 27)
    end_date = datetime(2023, 7, 7)
    main(start_date, end_date)
