import json
import time

from google.api_core.exceptions import BadRequest, Forbidden
from google.cloud import bigquery
from google.cloud.bigquery.retry import DEFAULT_RETRY

from function_variables import FunctionVariables
import convert 

var = FunctionVariables()


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
with open('bq_docai_id.json') as json_file:
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

            error = job.result(retry=DEFAULT_RETRY, timeout=retryCount*1000)   # Waits for table load to complete.
            print("BQ completed")
            print(error)
            return 
        
        except Forbidden as e:
            for e in job.errors:
                print(f'BQ ERROR {retryCount} Forbidden: {e}') 
                time.sleep(retryCount*100)
                
                if retryCount >= 3:
                    print('BQ FATAL Forbidden: {}'.format(e)) 
                    raise e

        except BadRequest as e:
            for e in job.errors:
                print('BQ ERROR BadRequest: {}'.format(e)) 
            return

