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

def get_env():
    print(os.environ)
    if 'GCP_PROJECT' in os.environ:
        return os.environ['GCP_PROJECT']
    import google.auth
    _, project_id = google.auth.default()
    print(project_id)
    return project_id

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

    # CI
    "family_name": "STRING",
    "date_of_birth": "STRING",
    "document_id": "STRING",
    "given_names": "STRING",

    "address": "STRING",
    "expiration_date": "STRING",    
    "issue_date": "STRING",

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

opts = {"api_endpoint": f"{location}-documentai.googleapis.com"}
docai_client = documentai.DocumentProcessorServiceClient(client_options=opts)

storage_client = storage.Client()
bq_client = bigquery.Client()
pub_client = pubsub_v1.PublisherClient()


def write_to_bq(dataset_name, table_name, entities_extracted_dict):

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


def main_run(event, context):
    gcs_input_uri = 'gs://' + event['bucket'] + '/' + event['name']
    print('Printing the contentType: ' + event['contentType'])
    print(name)

    if(event['contentType'] == 'image/gif' or event['contentType'] == 'application/pdf' or event['contentType'] == 'image/tiff' or event['contentType'] == 'image/jpeg'):
        
        input_config = documentai.types.document_processor_service.BatchProcessRequest.BatchInputConfig(
            gcs_source=gcs_input_uri, mime_type=event['contentType'])
        # Where to write results
        output_config = documentai.types.document_processor_service.BatchProcessRequest.BatchOutputConfig(
            gcs_destination=destination_uri)

        request = documentai.types.document_processor_service.BatchProcessRequest(
            name=name,
            input_configs=[input_config],
            output_config=output_config,
        )

        operation = docai_client.batch_process_documents(request)

        print("Wait for the operation to finish")
        
        operation.result(timeout=timeout)
        print("Operation finished")
        

        match = re.match(r"gs://([^/]+)/(.+)", destination_uri)
        output_bucket = match.group(1)
        prefix = match.group(2)

        # Get a pointer to the GCS bucket where the output will be placed
        bucket = storage_client.get_bucket(output_bucket)

        blob_list = list(bucket.list_blobs(prefix=prefix))
        print("Output files:")

        for i, blob in enumerate(blob_list):
            # Download the contents of this blob as a bytes object.
            if ".json" not in blob.name:
                print("blob name " + blob.name)
                print(f"skipping non-supported file type {blob.name}")
            else:
                # Setting the output file name based on the input file name
                print("Fetching from " + blob.name)
                #start = blob.name.rfind("/") + 1
                #end = blob.name.rfind(".") + 1
                input_filename = gcs_input_uri #blob.name[start:end:] + "gif"
                print("input_filename " + input_filename)
                
                # Getting ready to read the output of the parsed document - setting up "document"
                blob_as_bytes = blob.download_as_bytes()
                document = json.loads( blob_as_bytes)
                print(document)
                #document = documentai.types.Document.from_json(blob_as_bytes)

                # Reading all entities into a dictionary to write into a BQ table
                entities_extracted_dict = {}
                entities_extracted_dict['input_file_name'] = input_filename
                #entities_extracted = ""
                for entity in document['entities']:
                #for entity in document.entities:    
                    #entity_type = str(entity.type_)
                    print(entity)

                    entity_type = getKey(entity,'type')
                    entity_type = entity_type.replace(' ', '_').replace('/','_').lower()
                    entity_text = getKey(entity,'mentionText')
                    entity_confidence = getKey(entity,'confidence')
                    
                    # Not available yet
                    entity_normalized_value = getKey(entity, 'normalized_value')
                    
                    #entity_text = str(entity.mentionText)
                    # Normalize date format in case the entity being read is a date
                    if "date" in entity_type:
                        print(entity_text)
                        #d = datetime.strptime(entity_text, '%Y-%m-%d').date()
                        entities_extracted_dict[entity_type] = entity_text
                        #print (d)
                    else:
                        entity_text = str(entity_text)
                        print("Normalized text : " +
                              entity_normalized_value)
                        print("Mention text : " + entity_text)
                        entities_extracted_dict[entity_type] = entity_text
                        #print(entity_type + ":" + entity_text)


                    # Creating and publishing a message via Pub Sub to validate address
                    if (isContainAddress(entity_type) ) :
                        print(input_filename)
                        message = {
                            "entity_type": entity_type,
                            "entity_text": entity_text,
                            "input_file_name": input_filename,
                        }
                        message_data = json.dumps(message).encode("utf-8")

                        sendGeoCodeRequest(message_data)
                        sendKGRequest(message_data)                        

                    if (isContainName(entity_type) ) :
                        print(input_filename)
                        message = {
                            "entity_type": entity_type,
                            "entity_text": entity_text,
                            "input_file_name": input_filename,
                        }
                        message_data = json.dumps(message).encode("utf-8")

                        sendKGRequest(message_data)                        

            print(entities_extracted_dict)
            print("Writing to BQ")
            # Write the entities to BQ
            write_to_bq(dataset_name, table_name, entities_extracted_dict)

        # print(blobs)
        # Deleting the intermediate files created by the Doc AI Parser
        blobs = bucket.list_blobs(prefix=gcs_output_uri_prefix)
        
        delete_blob = True
        if delete_blob == True:
            for blob in blobs:
                blob.delete()
        # Copy input file to archive bucket
        source_bucket = storage_client.bucket(event['bucket'])
        source_blob = source_bucket.blob(event['name'])
        destination_bucket = storage_client.bucket(gcs_archive_bucket_name)
        blob_copy = source_bucket.copy_blob(
            source_blob, destination_bucket, event['name'])
        # delete from the input folder
        source_blob.delete()
    else:
        print('Cannot parse the file type')

def sendKGRequest(message_data):
    print("sendKGRequest")

    kg_topic_path = pub_client.topic_path(
        project_id, kg_request_topicname)
    print("kg_topic_path")
    print(kg_topic_path)

    kg_future = pub_client.publish(
        kg_topic_path, data=message_data)
    kg_futures.append(kg_future)

def sendGeoCodeRequest(message_data):
    print("sendGeoCodeRequest")
    print(message_data)
    geocode_topic_path = pub_client.topic_path(
        project_id, geocode_request_topicname)
    
    print("geocode_topic_path")
    print(geocode_topic_path)

    geocode_future = pub_client.publish(
        geocode_topic_path, data=message_data)
    geocode_futures.append(geocode_future)

def isContainAddress(entity_type):
    address_substring_tab = ["address", "adresse"]

    address_substring_found = False
    for address_substring in address_substring_tab:
        if address_substring in entity_type.lower():
            print("find address:" + entity_type)
            return True
    
    print("address not found in :" + entity_type)
    return False

def isContainName(entity_type):
    if "family" in entity_type.lower():
        return True
    if "given" in entity_type.lower():
        return True    
    return False    

def getKey(entity, key):
    if key in entity:
        return entity[key]
        
    return ''

def main_run_ex(event, context):
    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('Bucket: {}'.format(event['bucket']))
    print('File: {}'.format(event['name']))
    print('Metageneration: {}'.format(event['metageneration']))
    print('Created: {}'.format(event['timeCreated']))
    print('Updated: {}'.format(event['updated']))

    logging.debug('Event ID: {}'.format(context.event_id))
    logging.debug('Event type: {}'.format(context.event_type))
    logging.debug('Bucket: {}'.format(event['bucket']))
    logging.debug('File: {}'.format(event['name']))
    logging.debug('Metageneration: {}'.format(event['metageneration']))
    logging.debug('Created: {}'.format(event['timeCreated']))
    logging.debug('Updated: {}'.format(event['updated']))

    bqTableName = os.environ.get(
        'BQ_TABLENAME', 'ERROR: Specified environment variable is not set.')
    if bqTableName in "ERROR":
        logging.fatal('inputPdfPath variable is not set exit program')
        return

    input = "gs://" + event['bucket'] + "/" + event['name']

    if input.lower().endswith(".pdf"):
        mime_type = 'application/pdf'
    elif input.lower().endswith(".tiff"):
        mime_type = "image/tiff"
    elif input.lower().endswith(".png"):
        mime_type = "image/png"
    elif input.lower().endswith(".jpg") or input.lower().endswith(".jpeg"):
        mime_type = "image/jpeg"
    else:
        print("Exit: Extention not recognized:" + input)
        return

    if input.lower().find("fr_driver_license") >= 0:
        doc_type = 'fr_driver_license'

    elif input.lower().find("fr_national_id") >= 0:
        doc_type = "fr_national_id"

    elif input.lower().find("us_passport") >= 0:
        doc_type = "us_passport"

    elif input.lower().find("us_driver_license") >= 0:
        doc_type = "us_driver_license"

    else:
        doc_type = "fr_national_id"

    project_id = get_env()
    print(project_id)

    row = kyc(input, doc_type, mime_type)
    document = row
    df = getDF(document, input, doc_type)

    print(df)
    print("df json")
    print(df.to_json())

    print("Start Insert BQ : " + bqTableName)

    pandas_gbq.to_gbq(df, bqTableName, if_exists='append')
    print("Insert BQ done in : " + bqTableName)

    return "OK"


def getDF(document, name, doc_type):
    lst = [[]]
    lst.pop()

    entities = document.get('entities')
    if entities:
        for entity in document['entities']:
            print(entity)

            type = entity['type']
            val = entity['mentionText']
            confidence = entity['confidence']
            lst.append([type, val, type, confidence, name, doc_type])
    else:
        type = "ERROR"
        val = "Nothing parsed"
        confidence = 1.0
        lst.append([type, val, type, confidence, name, doc_type])

    df = pd.DataFrame(lst, columns=['key', 'value', 'type', 'confidence', 'file', 'doc_type']
                      )
    return df


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


def getToken():
    import google.auth
    import google.auth.transport.requests
    from google.oauth2 import service_account

    credentials = google.auth.default()[0]
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    return "Bearer " + credentials.token


def kyc(gcsFilePath: str, document_type, fileMimeType: str):
    import requests

    api = "https://eu-alpha-documentai.googleapis.com/v1alpha1/projects/google.com:ml-baguette-demos/locations/eu/documents:process"
    headers = {
        "Authorization": getToken(),
        "Content-Type": "application/json",
        "charset": "utf-8",
        "X-Goog-User-Project": get_env()

    }

    payload = """{
            input_config: {
              mime_type: '"""+fileMimeType+"""',
              gcs_source: {
                uri: '"""+gcsFilePath+"""'
              }
            },
            document_type: '"""+document_type+"""' 
        }"""

    print("Send post request:" + payload)
    r = requests.post(api, data=payload, headers=headers)
    json = r.json()
    print("Result")
    print(json)
    return json





def split_pdf(inputpdf, start_page, end_page, uri, gcs_output_uri: str, gcs_output_uri_prefix: str):
    storage_client = storage.Client()

    with io.StringIO() as stream:

        print("numPages: {}".format(inputpdf.numPages))

        output = PdfFileWriter()
        for i in range(start_page, end_page+1):
            output.addPage(inputpdf.getPage(i))
            print("add page {}".format(i))

        file = uri.path[:-4] + \
            "-page-{}-to-{}.pdf".format(start_page, end_page)
        print(file)

        buf = io.BytesIO()
        output.write(buf)
        data = buf.getvalue()
        outputBlob = gcs_output_uri_prefix + file
        print("Start write:"+outputBlob)
        bucket = storage_client.get_bucket(urlparse(gcs_output_uri).hostname)

        bucket.blob(outputBlob).upload_from_string(
            data, content_type='application/pdf')

        stream.truncate(0)

    print("split finish")


def pages_split(text: str, document: dict, uri, gcs_output_uri: str, gcs_output_uri_prefix: str):
    """
    Document AI identifies possible page splits
    in document. This function converts page splits
    to text snippets and prints it.    
    """
    for i, entity in enumerate(document.entities):
        confidence = entity.confidence
        text_entity = ''
        for segment in entity.text_anchor.text_segments:
            start = segment.start_index
            end = segment.end_index
            text_entity += text[start:end]

        pages = [p.page for p in entity.page_anchor.page_refs]
        print(f"*** Entity number: {i}, Split Confidence: {confidence} ***")
        print(
            f"*** Pages numbers: {[p for p in pages]} ***\nText snippet: {text_entity[:100]}")
        print("type: " + entity.type_)
        start_page = pages[0]
        end_page = pages[len(pages)-1]
        print(start_page)
        print(end_page)

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(uri.hostname)
        blob = bucket.get_blob(uri.path[1:])

        inputpdf = PdfFileReader(
            io.BytesIO(blob.download_as_bytes()), strict=False)

        split_pdf(inputpdf, start_page, end_page, uri, gcs_output_uri,
                  gcs_output_uri_prefix + "/" + entity.type_)


# Synchronous processing
def process(
    project_id: str, location: str,  gcs_input_uri: str, gcs_output_uri: str, gcs_output_uri_prefix, OUTPUT_JSON_URI,  timeout: int = 300,
):
    uri = urlparse(gcs_input_uri)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(uri.hostname)

    blob = bucket.get_blob(uri.path[1:])
    image_content = blob.download_as_bytes()

    if gcs_input_uri.lower().endswith(".pdf"):
        convertToImage(gcs_input_uri, outputBucket=OUTPUT_JSON_URI)
    else:
        # Read the detected page split from the processor
        print("\nThe processor detected the following page split entities:")
        pages_split("text", "document", uri,
                    gcs_output_uri, gcs_output_uri_prefix)


def convertToImage(f, outputBucket):
    from pdf2image import convert_from_path

    # Set poppler path
    poppler_path = "/var/task/lib/poppler-utils-0.26/usr/bin"

    images = convert_from_path(f, dpi=150, poppler_path=poppler_path)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(outputBucket)

    for i in range(len(images)):
        file = "/tmp/images/" + \
            f[4:].replace("/", "").replace(".pdf",
                                           "").replace(":", "") + '-p' + str(i) + '.jpg'

        # Save pages as images in the pdf
        images[i].save(file, 'JPEG')
        bucket.upload_from_filename(file)
