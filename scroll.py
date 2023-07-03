# -*- coding: utf-8 -*-

import os
import json
import requests
import base64

index_name = 'logstash-p-test-*'

# Authentication credentials
username = 'elastic'
password = 'changeme'

# Base64 encode the username and password
credentials = base64.b64encode('{}:{}'.format(username, password).encode('utf-8')).decode('utf-8')

# Fonction pour extraire les données Elasticsearch avec scroll
def extract_data(date_start, date_end):
    url = 'http://localhost:9200/logstash-p-test-*/_search?scroll=1m'
    headers = {
        'Authorization': 'Basic {}'.format(credentials),
        'Content-Type': 'application/json'
        }
    data ={
         
        "query": {
            "bool": {
                "must": [
                {
                    "match": {
                        "timestamp": "2023-07-02"
                    }
                }
                ]
                }
            },
            "sort": [
            {
                "timestamp": {
                "order": "desc"
                }
            }
            ]
    }
    response = requests.get(url, headers=headers, json=data)
    response_data = response.json()
    scroll_id = response_data['_scroll_id']
    results = response_data['hits']['hits']
    documents = []

    # Parcourir les résultats avec scroll jusqu'à ce qu'il n'y ait plus de documents
    while len(results) > 0:
        
        scroll_url = 'http://localhost:9200/_search/scroll'
        scroll_params = {
            "scroll": "1m",
            "scroll_id": scroll_id
        }
        scroll_response = requests.get(scroll_url, headers=headers, params=scroll_params)
        scroll_data = scroll_response.json()
        scroll_id = scroll_data['_scroll_id']
        documents += scroll_data['hits']['hits']
        
    return documents

# Fonction pour stocker les données dans des fichiers JSON par tranche de date
def store_data_by_date(documents):
    date_documents = {}

    # Regrouper les documents par date
    for document in documents:
        date = document['_source']['date_field']
        if date not in date_documents:
            date_documents[date] = []
        date_documents[date].append(document)

    # Créer un dossier pour chaque date et écrire les documents dans des fichiers JSON
    for date, docs in date_documents.iteritems():
        folder_path = os.path.join('data', date)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        num_files = len(docs) / 2 + 1
        for i in range(num_files):
            file_path = os.path.join(folder_path, 'data_' + str(i) + '.json')
            start_idx = i * 2
            end_idx = (i + 1) * 2
            chunk_docs = docs[start_idx:end_idx]
            with open(file_path, 'w') as outfile:
                json.dump(chunk_docs, outfile)

# Fonction principale pour gérer le processus d'extraction et de stockage des données
def main():
    date_start = '2023-06-27'
    date_end = '2023-07-03'
    
    try:
        documents = extract_data(date_start, date_end)
        store_data_by_date(documents)
        print("Extraction et stockage des données terminés avec succès !")
    except Exception as e:
        print("Une erreur s'est produite : {}".format(str(e)))

if __name__ == '__main__':
    main()
