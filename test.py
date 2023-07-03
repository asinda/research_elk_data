import requests
import json
import time

# Paramètres Elasticsearch
es_host = 'http://localhost:9200'
index_name = 'votre_index'
from_value = 0  # à partir duquel enregistrement extraire
size_value = 1000  # nombre d'enregistrements à extraire à chaque fois

# Effectuer la requête initiale pour obtenir le nombre total de documents
response = requests.get(f'{es_host}/{index_name}/_count')
total_docs = response.json()['count']

# Boucle pour extraire tous les documents
while from_value < total_docs:
    # Effectuer une requête de recherche avec les paramètres from et size
    payload = {
        "query": {
            "match_all": {}
        },
        "from": from_value,
        "size": size_value
    }
    response = requests.post(f'{es_host}/{index_name}/_search', json=payload)
    data = response.json()

    # Récupérer les documents et les enregistrer dans des fichiers JSON
    for doc in data['hits']['hits']:
        timestamp = doc['_source']['timestamp']  # supposons que le champ de timestamp s'appelle 'timestamp'
        filename = f'{timestamp}.json'
        with open(filename, 'w') as f:
            json.dump(doc['_source'], f)

    # Mettre à jour les valeurs 'from' et 'size' pour la prochaine itération
    from_value += size_value

    # Attendre quelques secondes entre les requêtes pour éviter de surcharger Elasticsearch
    time.sleep(2)

print('Extraction terminée.')
