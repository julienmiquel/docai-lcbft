import json
import logging
import time

from google.api_core.exceptions import BadRequest, Forbidden
from google.cloud import bigquery
from google.cloud.bigquery.retry import DEFAULT_RETRY

from function_variables import FunctionVariables
import convert 

var = FunctionVariables()


# Create a dict to create the schema
# and to avoid BigQuery load job fails due to unknown fields
# bq_schema = {
#     "input_file_name": "STRING",
#     "insert_date": "DATETIME",
#     "doc_type": "STRING",
#     "signed_url": "STRING",
#     "key": "STRING",
#     "image": "BYTES",

#     # CI
#     "family_name": "STRING",
#     "date_of_birth": "DATE",
#     "document_id": "STRING",
#     "given_names": "STRING",

#     "address": "STRING",
#     "expiration_date": "DATE",    
#     "issue_date": "DATE",

#     "error": "STRING",
#     "timer": "INTEGER",
#     "result": "STRING",
#     "hitl": "STRING",

#     # identity Fraud detection
#     "fraud_signals_suspicious_words": "STRING",
#     "fraud_signals_is_identity_document": "STRING",
#     "fraud_signals_image_manipulation": "STRING",

#     # entities


#     # Invoices
#     "carrier": "STRING",
#     "currency": "STRING",
#     "currency_exchange_rate": "STRING",
#     "customer_tax_id": "STRING",
#     "delivery_date": "DATE",
#     "due_date": "DATE",
#     "freight_amount": "FLOAT",
#     "invoice_date": "DATE",
#     "invoice_id": "STRING",
#     "net_amount": "FLOAT",
#     "payment_terms": "STRING",
#     "purchase_order": "STRING",
#     "receiver_address": "STRING",
#     "receiver_email": "STRING",
#     "receiver_name": "STRING",
#     "receiver_phone": "STRING",
#     "receiver_tax_id": "STRING",
#     "remit_to_address": "STRING",
#     "remit_to_name": "STRING",
#     "ship_from_address": "STRING",
#     "ship_from_name": "STRING",
#     "ship_to_address": "STRING",
#     "ship_to_name": "STRING",
#     "supplier_address": "STRING",
#     "supplier_email": "STRING",
#     "supplier_iban": "STRING",
#     "supplier_name": "STRING",
#     "supplier_phone": "STRING",
#     "supplier_registration": "STRING",
#     "supplier_tax_id": "STRING",
#     "supplier_website": "STRING",
#     "total_amount": "FLOAT",
#     "total_tax_amount": "FLOAT",
#     "vat_tax_amount": "FLOAT",
#     "vat_tax_rate": "STRING",
#     "line_item": "STRING",
#     "receipt_date": "DATE",
#     "purchase_time": "STRING",
#     "supplier_city": "STRING"
# }
# bq_load_schema = []
# for key, value in bq_schema.items():
#     bq_load_schema.append(bigquery.SchemaField(key, value))

def read_bigquery_schema_from_json_recursive(json_schema):
    """
    CAUTION: Recursive function
    This method can generate BQ schemas for nested records
    """
    
    result = []
    for field in json_schema:
        if field.get('type').lower() == 'record' and field.get('fields'):
            schema = bigquery.SchemaField(
                name=field.get('name'),
                field_type=field.get('type', 'STRING'),
                mode=field.get('mode', 'NULLABLE'),
                description=field.get('description'), 
                fields=read_bigquery_schema_from_json_recursive(field.get('fields'))
            )
        else:
            schema = bigquery.SchemaField(
                name=field.get('name'),
                field_type=field.get('type', 'STRING'),
                mode=field.get('mode', 'NULLABLE'),
                description=field.get('description')
            )
        result.append(schema)
    return result 

bq_load_schema = None
with open('bq_docai_extracted_entities.json') as json_file:
    bq_schema = json.load(json_file)
    bq_load_schema = read_bigquery_schema_from_json_recursive(bq_schema)
    print("bq_load_schema init")
    print(bq_load_schema)


bq_schema_rules = {
    "input_file_name": "STRING",
    "name": "STRING"
}
bq_load_schema_rules = []
for key, value in bq_schema_rules.items():
    bq_load_schema_rules.append(bigquery.SchemaField(key, value))

bq_client = bigquery.Client( var.project_id )


def write_to_bq(entities_extracted_dict, dataset_name=var.dataset_name, table_name=var.table_name):

    if len(entities_extracted_dict) == 0:
        print("Nothing to write")
        return 

    dataset_ref = bq_client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)

    test_dict = entities_extracted_dict.copy()
    for key, value in test_dict.items():
        new_key =convert.cleanEntityType(key) 
        entities_extracted_dict[new_key] = entities_extracted_dict.pop(key)

    test_dict = entities_extracted_dict.copy()
    for key, value in test_dict.items():
        if key not in [item['name'] for item in bq_schema]:
            print("Deleting key:"+key)
            del entities_extracted_dict[key]
        else:
            print(f"key/value {key} = {entities_extracted_dict[key]}")

    row_to_insert = []
    row_to_insert.append(entities_extracted_dict)

    try:
        json_data = json.dumps(row_to_insert, sort_keys=False)
        # Convert to a JSON Object
        json_object = json.loads(json_data)
    except Exception as err:
        print(err)
        print(f"json.dumps ERROR: {err}")
        print("Row below cannot be converted")
        print(row_to_insert)
        return
        
    job_config = bigquery.LoadJobConfig(schema=bq_load_schema)
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

    print(f"json_object to dump into BQ: {dataset_name}.{table_name}" )
    print(json_object)
    job = bq_client.load_table_from_json(
        json_object, table_ref, job_config=job_config)

    retryCount = 0
    while(retryCount < 3):
        try:
            retryCount = retryCount + 1

            error = job.result(retry=DEFAULT_RETRY)  # Waits for table load to complete.
            print("BQ completed")
            print(error)
            return 
        
        except Forbidden as e:
            for e in job.errors:
                print(f'BQ ERROR {retryCount} Forbidden: {e}') 
                time.sleep(retryCount)
                
                if retryCount >= 3:
                    print('BQ FATAL Forbidden: {}'.format(e)) 
                    raise e

        except BadRequest as e:
            for e in job.errors:
                print('BQ ERROR BadRequest: {}'.format(e)) 
            return




def bqInsert(rows_to_insert, table_id):
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Make an API request.
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))





def checkBusinessRule(dataset_name= var.dataset_name):
    #/// Check business rules
    # Perform a query.
    QUERY = (
        f'SELECT name, query FROM `{dataset_name}.business_rules` '
        'WHERE trigger = "every_insert" ')

    rows = query(QUERY)

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

def query(QUERY):
    query_job = bq_client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish
    print(rows)
    
    return rows


def runBusinessRule(row_to_insert, query):
    print(f'business rule: {query}')
        
    query_job = bq_client.query(query)  # API request
    rows_business_rule = query_job.result()  # Waits for query to finish

    for row_rule_matched in rows_business_rule:
        print(f'Row returned: {row_rule_matched.name} = {row_rule_matched.input_file_name}')
        row_to_insert[str(row_rule_matched.name)] = str(row_rule_matched.input_file_name)

    return row_to_insert

