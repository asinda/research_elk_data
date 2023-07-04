# research_elk_data
Elasticsearch Data Retrieval and Storage

This Python script allows you to retrieve data from an Elasticsearch index and store the documents in separate JSON files based on their timestamps.
Prerequisites

Before running the script, ensure you have the following prerequisites:

    Python 3.x installed on your machine
    Required Python packages: requests

Install the required package using pip:

pip install requests

Configuration

In the script, you may need to update the Elasticsearch URL and authentication credentials to match your Elasticsearch instance. Edit the following lines in the get_data_from_elasticsearch function:

python

base_url = "http://localhost:9200/logstash-p-test-*/_search"
username = 'elastic'
password = 'changeme'

Adjust the base_url to the appropriate Elasticsearch URL for your specific index and cluster.
Usage

To run the script, use the following command:

bash

python script_name.py

Replace script_name.py with the actual filename of the Python script.
Parameters

The main function main() takes three optional parameters:

    start_date: A datetime object representing the start date of the data retrieval period.
    end_date: A datetime object representing the end date of the data retrieval period.
    batch_size: The number of documents to retrieve from Elasticsearch in each API request.

By default, start_date and end_date are set to datetime(2023, 6, 27) and datetime(2023, 7, 22), respectively. The batch_size is set to 40 documents.
Output

The script will retrieve data from the specified Elasticsearch index using the given time range and store each document in a separate JSON file. The files will be named based on their timestamps, following the format: data_<timestamp>.json.

If a document with the same timestamp has already been retrieved and stored in a file, it will be skipped to avoid duplication.

Note: If there are any issues during data retrieval or file writing, the script will print error messages to the console.

Please make sure you have proper access rights to read from the Elasticsearch index and write files to the local directory where the script is running.