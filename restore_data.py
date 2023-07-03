# -*- coding: utf-8 -*-
import os
import json
import requests
import base64
from datetime import datetime, timedelta 


def extract_data(start_date, end_date):
    base_url = "http://localhost:9200"  # URL de votre cluster Elasticsearch
    
    index_name = 'logstash-p-test-*'

    # Authentication credentials
    username = 'elastic'
    password = 'changeme'

    # Base64 encode the username and password
    credentials = base64.b64encode('{}:{}'.format(username, password).encode('utf-8')).decode('utf-8')

    # Configuration des paramètres de pagination
    page_size = 3
    from_index = 0
    total_hits = 1
    # Function to store documents in a JSON file
    def store_documents(documents, folder, file_name):
        try:
            file_path = os.path.join(folder, file_name)
            with open(file_path, 'w') as file:
                json.dump(documents, file)
            print("Data exported to '{}'".format(file_path))
        except IOError as e:
            print("Error storing data in file: {}".format(e))
            raise
    # Boucle pour itérer sur les résultats paginés
    while from_index < total_hits:
        try:
            # Construction de l'URL de la requête de recherche
            url = "{}/{}/_search".format(base_url, index_name)
            headers = {
                'Authorization': 'Basic {}'.format(credentials),
                'Content-Type': 'application/json'
             }

            # Construction de la requête de recherche
            query = {
                "query": {
                    "range": {
                        "timestamp": {
                            "gte": start_date.strftime('%Y-%m-%d'),
                            "lt": (start_date + timedelta(days=1)).strftime('%Y-%m-%d')
                        }
                    }
                },
                "from": from_index,
                "size": page_size
            }
  
            total_documents = 0
            current_documents = []

            # Envoi de la requête de recherche à Elasticsearch
            response = requests.get(url,headers=headers, json=query)
            response.raise_for_status()

            # Extraction des résultats
            data = response.json()["hits"]["hits"]

            # Vérification du nombre total de résultats
            if from_index == 0:
                total_hits = response.json()["hits"]["total"]["value"]

            # Traitement des résultats
            for item in data:
                # Extraire les données dont vous avez besoin
                # (exemple : `item["_source"]["field"]`)

                # Stockage des données par date dans un dossier
              
                current_date = start_date
                while current_date <= end_date:
                    date = current_date.strftime('%Y-%m-%d')
                    folder_path = os.path.join("/home/alice/Documents/Alice/004-Elk/research_elk_data/data/", date)
                    if not os.path.exists(folder_path):
                        os.makedirs(folder_path)
                    # Écriture des données dans un fichier JSON
                    current_documents.append(item['_source'])
                    total_documents += 1
                    
                    if len(current_documents) >= page_size:
                        file_name = "data_{}_{}.json".format(date, total_documents)
                        store_documents(current_documents, folder_path, file_name)
                        current_documents = []
                # Store remaining documents in a final file
                if current_documents:
                  file_name = "data_{}_{}.json".format(current_date.strftime('%Y-%m-%d'), total_documents)
                  store_documents(current_documents, folder_path, file_name)

                current_date += timedelta(days=1)
                # Mise à jour de l'index de pagination
                from_index += page_size
        except requests.exceptions.RequestException as e:
            print("Erreur lors de la requête : {}".format(str(e)))
            break

        except KeyError as e:
            print("Clé introuvable dans la réponse JSON : {}".format(str(e)))
            break

        except IOError as e:
            print("Erreur d'entrée/sortie lors de l'écriture du fichier : {}".format(str(e)))
            break


# Define range date for the request
start_date = datetime(2023, 6, 27)
end_date = datetime(2023, 7, 7)
extract_data(start_date, end_date)
