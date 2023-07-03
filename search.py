import os
import json
import requests
from datetime import datetime, timedelta

def extract_data(start_date, end_date, index_name):
    current_date = start_date
    while current_date <= end_date:
        current_date_str = current_date.strftime("%Y-%m-%d")
        folder_path = os.path.join("data", current_date_str)
        os.makedirs(folder_path, exist_ok=True)

        from_index = 0
        size = 1000
        total_docs = get_total_docs(index_name, current_date)
        
        while from_index < total_docs:
            try:
                response = query_elasticsearch(index_name, current_date, from_index, size)
                data = response.json()
                file_path = os.path.join(folder_path, f"data_{from_index}.json")
                
                with open(file_path, "w") as f:
                    json.dump(data, f)
                
                from_index += size
                
            except requests.exceptions.RequestException as e:
                print(f"An error occurred while querying Elasticsearch: {e}")
                break
            
            except Exception as e:
                print(f"An error occurred: {e}")
                break
        
        current_date += timedelta(days=1)

def get_total_docs(index_name, current_date):
    query = {
        "query": {
            "bool": {
                "filter": [
                    {"range": {"date_field": {"gte": current_date, "lt": current_date + timedelta(days=1)}}}
                ]
            }
        }
    }
    
    response = requests.get(f"http://localhost:9200/{index_name}/_count", json=query)
    data = response.json()
    
    return data["count"]

def query_elasticsearch(index_name, current_date, from_index, size):
    query = {
        "query": {
            "bool": {
                "filter": [
                    {"range": {"date_field": {"gte": current_date, "lt": current_date + timedelta(days=1)}}}
                ]
            }
        },
        "from": from_index,
        "size": size
    }
    
    response = requests.get(f"http://localhost:9200/{index_name}/_search", json=query)
    return response

# Exemple d'utilisation
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 1, 10)
index_name = "your_index_name"

extract_data(start_date, end_date, index_name)
