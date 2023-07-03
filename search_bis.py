# -*- coding: utf-8 -*-

import os
import json
import requests
import base64
from datetime import datetime, timedelta

# Elasticsearch connection parameters
# Elasticsearch server details
elasticsearch_host = 'http://localhost:9200'
index_name = 'logstash-p-test-*'

# Authentication credentials
username = 'elastic'
password = 'changeme'

# Base64 encode the username and password
credentials = base64.b64encode('{}:{}'.format(username, password).encode('utf-8')).decode('utf-8')

# Define range date for the request
start_date = datetime(2023, 6, 27)
end_date = datetime(2023, 7, 7)

# Paramètres de configuration
scroll_size = 2
scroll_time = '1s'
max_documents_per_file = 2 

# Fonction pour créer un dossier
def create_folder(folder_name):
    try:
        os.makedirs(folder_name)
    except OSError:
        if not os.path.isdir(folder_name):
            raise

# Fonction pour exporter les documents dans un fichier JSON
def export_to_json(documents, folder_name, file_counter):
    output_file = os.path.join(folder_name, 'data_{}.json'.format(file_counter))
    with open(output_file, 'w') as file:
        json.dump(documents, file)

    print("Data exported to '{}'".format(output_file))


# Fonction pour effectuer une requête de recherche Elasticsearch avec défilement
def perform_scroll_search(scroll_url, headers, search_query):
    try:
        response = requests.post(scroll_url, headers=headers, json=search_query)
        response_data = response.json()
        return response_data
    except requests.exceptions.RequestException as e:
        print("Error in Elasticsearch scroll search:", e)
        return None

# Format des dates
date_format = '%Y-%m-%d'
start_date_str = start_date.strftime(date_format)
end_date_str = end_date.strftime(date_format)

# Boucle pour parcourir les tranches de dates
current_date = start_date
while current_date <= end_date:
    next_date = current_date + timedelta(days=1)

    # Créer le dossier pour la tranche de dates actuelle
    folder_name = current_date.strftime(date_format)
    create_folder(folder_name)

    # Initialiser le défilement Elasticsearch pour la tranche de dates actuelle
    scroll_url = "{}/{}/_search?scroll={}".format(elasticsearch_host, index_name, scroll_time)
    headers = {
        'Authorization': 'Basic {}'.format(credentials),
        'Content-Type': 'application/json'
        }

    search_query = {
        "size": scroll_size,
        "query": {
                "bool": {
                    "must": [
                    {
                        "match": {
                        "timestamp": current_date.strftime('%Y-%m-%d')
                        }
                    }
                    ]
                }
            
                },
                    "size": max_documents_per_file,
                    "sort": [
                    {
                        "timestamp": {
                        "order": "desc"
                        }
                    }
                    ]
    }

    response_data = perform_scroll_search(scroll_url, headers, search_query)
    if response_data is None:
        break

    scroll_id = response_data['_scroll_id']
    total_documents = response_data['hits']['total']['value']

    # Extraire et stocker les données dans des fichiers JSON
    file_counter = 1
    documents_counter = 0
    print('hello')
   
