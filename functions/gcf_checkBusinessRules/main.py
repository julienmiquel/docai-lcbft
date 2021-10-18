import logging
import traceback
import tempfile
import os
import re
import io

from google.cloud import storage

from PyPDF2 import PdfFileWriter, PdfFileReader
from urllib.parse import urlparse
import pandas_gbq
import pandas as pd

import base64
import re
import os
import json
from datetime import datetime
from google.cloud import bigquery
from google.cloud import documentai_v1beta3 as documentai
from google.cloud import storage
from google.cloud import pubsub_v1

from dateutil import parser


def get_env():
    print(os.environ)
    if 'GCP_PROJECT' in os.environ:
        return os.environ['GCP_PROJECT']
    import google.auth
    _, project_id = google.auth.default()
    print(project_id)
    return project_id


project_id = get_env()
print(project_id)

# Reading environment variables
gcs_output_uri_prefix = os.environ.get('GCS_OUTPUT_URI_PREFIX')
project_id = get_env()
location = os.environ.get('PARSER_LOCATION')
processor_id = os.environ.get('PROCESSOR_ID')
geocode_request_topicname = os.environ.get('GEOCODE_REQUEST_TOPICNAME')
kg_request_topicname = os.environ.get('KG_REQUEST_TOPICNAME')
timeout = 300
# int(os.environ.get('TIMEOUT'))

# An array of Future objects
# Every call to publish() returns an instance of Future
geocode_futures = []
kg_futures = []
# Setting variables

gcs_output_uri = os.environ.get('gcs_output_uri')
gcs_archive_bucket_name = os.environ.get('gcs_archive_bucket_name')

destination_uri = f"{gcs_output_uri}/{gcs_output_uri_prefix}/"
name = f"projects/660199673046/locations/eu/processors/7b9066f18d0c7366"
dataset_name = os.environ.get(
    'BQ_DATASET_NAME', 'ERROR: Specified environment variable is not set.')

table_name = 'doc_ai_extracted_entities'
# Create a dict to create the schema
# and to avoid BigQuery load job fails due to inknown fields
bq_schema = {
    "input_file_name": "STRING",
    "insert_date": "DATE",

    # CI
    "family_name": "STRING",
    "date_of_birth": "DATE",
    "document_id": "STRING",
    "given_names": "STRING",

    "address": "STRING",
    "expiration_date": "DATE",    
    "issue_date": "DATE",

    "error": "STRING",

    # Invoices
    "carrier": "STRING",
    "currency": "STRING",
    "currency_exchange_rate": "STRING",
    "customer_tax_id": "STRING",
    "delivery_date": "DATE",
    "due_date": "DATE",
    "freight_amount": "STRING",
    "invoice_date": "DATE",
    "invoice_id": "STRING",
    "net_amount": "STRING",
    "payment_terms": "STRING",
    "purchase_order": "STRING",
    "receiver_address": "STRING",
    "receiver_email": "STRING",
    "receiver_name": "STRING",
    "receiver_phone": "STRING",
    "receiver_tax_id": "STRING",
    "remit_to_address": "STRING",
    "remit_to_name": "STRING",
    "ship_from_address": "STRING",
    "ship_from_name": "STRING",
    "ship_to_address": "STRING",
    "ship_to_name": "STRING",
    "supplier_address": "STRING",
    "supplier_email": "STRING",
    "supplier_iban": "STRING",
    "supplier_name": "STRING",
    "supplier_phone": "STRING",
    "supplier_registration": "STRING",
    "supplier_tax_id": "STRING",
    "supplier_website": "STRING",
    "total_amount": "STRING",
    "total_tax_amount": "STRING",
    "vat_tax_amount": "STRING",
    "vat_tax_rate": "STRING",
    "line_item": "STRING",
    "receipt_date": "DATE",
    "purchase_time": "STRING",
    "supplier_city": "STRING"
}
bq_load_schema = []
for key, value in bq_schema.items():
    bq_load_schema.append(bigquery.SchemaField(key, value))

bq_schema_rules = {
    "input_file_name": "STRING",
    "name": "STRING"
}
bq_load_schema_rules = []
for key, value in bq_schema.items():
    bq_load_schema_rules.append(bigquery.SchemaField(key, value))


opts = {"api_endpoint": f"{location}-documentai.googleapis.com"}
docai_client = documentai.DocumentProcessorServiceClient(client_options=opts)

storage_client = storage.Client()
bq_client = bigquery.Client( project_id )
pub_client = pubsub_v1.PublisherClient()


def write_to_bq(dataset_name, table_name, entities_extracted_dict):

    if len(entities_extracted_dict) == 0:
        print("Nothing to write")
        return 

    dataset_ref = bq_client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)

    test_dict = entities_extracted_dict.copy()
    for key, value in test_dict.items():
        new_key =str(key).replace(' ', '_').replace('/','_').lower()
        entities_extracted_dict[new_key] = entities_extracted_dict.pop(key)

    test_dict = entities_extracted_dict.copy()
    for key, value in test_dict.items():
        if key not in bq_schema:
            print("Deleting key:"+key)
            del entities_extracted_dict[key]

    row_to_insert = []
    row_to_insert.append(entities_extracted_dict)

    json_data = json.dumps(row_to_insert, sort_keys=False)
    # Convert to a JSON Object
    json_object = json.loads(json_data)

    job_config = bigquery.LoadJobConfig(schema=bq_load_schema)
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

    print(f"json_object to dump into BQ: {dataset_name}.{table_name}" )
    print(json_object)
    job = bq_client.load_table_from_json(
        json_object, table_ref, job_config=job_config)
    error = job.result()  # Waits for table load to complete.
    print(error)

    

def checkBusinessRule(dataset_name):
    #/// Check business rules
    # Perform a query.
    QUERY = (
        f'SELECT name, query FROM `{dataset_name}.business_rules` '
        'WHERE trigger = "every_insert" ')

    query_job = bq_client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    print(rows)
    row_to_insert = []

    for row in rows:
        row_to_insert = runBusinessRule(row_to_insert, row.query)
    
    if len(row_to_insert) > 0:
        write_to_bq(dataset_name,"business_rules_result", row_to_insert)
    else:
        print("No business rules matched")

    return 
    runBusinessRule(row_to_insert, f'SELECT input_file_name, "address_is_null" as name FROM \
        `{dataset_name}.doc_ai_extracted_entities` where address is null')

    runBusinessRule(row_to_insert, f'SELECT input_file_name, "address_not_found" as name FROM \
        `{dataset_name}.doc_ai_extracted_entities` where address is not null and input_file_name not in (select input_file_name from `{dataset_name}.geocodes_details`) ')


def runBusinessRule(row_to_insert, query):
    print(f'business rule: {query}')
        
    query_job = bq_client.query(query)  # API request
    rows_business_rule = query_job.result()  # Waits for query to finish

    for row_rule_matched in rows_business_rule:
        print(f'Row returned: {row_rule_matched.name} , {row_rule_matched.input_file_name}')
        row_to_insert[row_rule_matched.name] = row_rule_matched.input_file_name

    return row_to_insert

def main_run(event, context):
    # Check business rules
    checkBusinessRule(dataset_name)

