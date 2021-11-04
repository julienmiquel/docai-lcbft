import logging
import traceback
import tempfile
import os
import re
import io
import time

import dateutil


from ghostscript import Ghostscript

from PyPDF2 import PdfFileWriter, PdfFileReader
from urllib.parse import urlparse
from google.api_core import retry
import pandas_gbq
import pandas as pd

import base64
import re
import os
import json
from datetime import datetime

from google.cloud import bigquery
from google.cloud import documentai_v1 as documentai
from google.cloud import storage
from google.cloud import pubsub_v1
#from google.cloud import logging

from dateutil import parser
from typing import List, Union



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
docai_fr_driver_license = "projects/660199673046/locations/eu/processors/cee6a410ee499b69"
docai_fr_national_id = "projects/660199673046/locations/eu/processors/57993de9b197ee1f"
docai_us_passport = "projects/660199673046/locations/eu/processors/3bd9c32d439b29cf"
docai_us_driver_license = "projects/660199673046/locations/eu/processors/57993de9b197e27c"
docai_invoice = "projects/660199673046/locations/eu/processors/abf12796440cc270"

def getDocType(input : str):
    
    if input.lower().find("fr_driver_license") >= 0:
        doc_type = 'fr_driver_license'
        processor_path = docai_fr_driver_license

    elif input.lower().find("fr_national_id") >= 0:
        doc_type = "fr_national_id"
        processor_path = docai_fr_national_id


    elif input.lower().find("us_passport") >= 0:
        doc_type = "us_passport"
        processor_path = docai_us_passport

    elif input.lower().find("fr_passport") >= 0:
        doc_type = "fr_passport_not_yet_supported"
        processor_path = docai_us_passport        


    elif input.lower().find("us_driver_license") >= 0:
        doc_type = "us_driver_license"
        processor_path = docai_us_driver_license


    elif input.lower().find("invoice") >= 0:
        doc_type = "invoice"
        processor_path = docai_invoice

    else:
        doc_type = "fr_national_id"
        processor_path = docai_fr_driver_license
    print(f"doc_type:{doc_type} - processor_path: {processor_path}")
    return doc_type, processor_path


dataset_name = os.environ.get(
    'BQ_DATASET_NAME', 'ERROR: Specified environment variable is not set.')

table_name = 'doc_ai_extracted_entities'
# Create a dict to create the schema
# and to avoid BigQuery load job fails due to inknown fields
bq_schema = {
    "input_file_name": "STRING",
    "insert_date": "DATETIME",
    "doc_type": "STRING",
    "signed_url": "STRING",
    "key": "STRING",
    "image": "BYTES",

    # CI
    "family_name": "STRING",
    "date_of_birth": "DATE",
    "document_id": "STRING",
    "given_names": "STRING",

    "address": "STRING",
    "expiration_date": "DATE",    
    "issue_date": "DATE",

    "error": "STRING",
    "timer": "INTEGER",
    "result": "STRING",

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



def generate_signed_url(service_account_file, bucket_name, object_name,
                        subresource=None, expiration=604800, http_method='GET',
                        query_parameters=None, headers=None):

    
    import binascii
    import collections
    import datetime
    import hashlib
    import sys

    # pip install google-auth
    from google.oauth2 import service_account
    # pip install six
    import six
    from six.moves.urllib.parse import quote
    if expiration > 604800:
        print('Expiration Time can\'t be longer than 604800 seconds (7 days).')
        sys.exit(1)

    escaped_object_name = quote(six.ensure_binary(object_name), safe=b'/~')
    canonical_uri = '/{}'.format(escaped_object_name)

    datetime_now = datetime.datetime.utcnow()
    request_timestamp = datetime_now.strftime('%Y%m%dT%H%M%SZ')
    datestamp = datetime_now.strftime('%Y%m%d')

    google_credentials = service_account.Credentials.from_service_account_file(
        service_account_file)
    client_email = google_credentials.service_account_email
    credential_scope = '{}/auto/storage/goog4_request'.format(datestamp)
    credential = '{}/{}'.format(client_email, credential_scope)

    if headers is None:
        headers = dict()
    host = '{}.storage.googleapis.com'.format(bucket_name)
    headers['host'] = host

    canonical_headers = ''
    ordered_headers = collections.OrderedDict(sorted(headers.items()))
    for k, v in ordered_headers.items():
        lower_k = str(k).lower()
        strip_v = str(v).lower()
        canonical_headers += '{}:{}\n'.format(lower_k, strip_v)

    signed_headers = ''
    for k, _ in ordered_headers.items():
        lower_k = str(k).lower()
        signed_headers += '{};'.format(lower_k)
    signed_headers = signed_headers[:-1]  # remove trailing ';'

    if query_parameters is None:
        query_parameters = dict()
    query_parameters['X-Goog-Algorithm'] = 'GOOG4-RSA-SHA256'
    query_parameters['X-Goog-Credential'] = credential
    query_parameters['X-Goog-Date'] = request_timestamp
    query_parameters['X-Goog-Expires'] = expiration
    query_parameters['X-Goog-SignedHeaders'] = signed_headers
    if subresource:
        query_parameters[subresource] = ''

    canonical_query_string = ''
    ordered_query_parameters = collections.OrderedDict(
        sorted(query_parameters.items()))
    for k, v in ordered_query_parameters.items():
        encoded_k = quote(str(k), safe='')
        encoded_v = quote(str(v), safe='')
        canonical_query_string += '{}={}&'.format(encoded_k, encoded_v)
    canonical_query_string = canonical_query_string[:-1]  # remove trailing '&'

    canonical_request = '\n'.join([http_method,
                                   canonical_uri,
                                   canonical_query_string,
                                   canonical_headers,
                                   signed_headers,
                                   'UNSIGNED-PAYLOAD'])

    canonical_request_hash = hashlib.sha256(
        canonical_request.encode()).hexdigest()

    string_to_sign = '\n'.join(['GOOG4-RSA-SHA256',
                                request_timestamp,
                                credential_scope,
                                canonical_request_hash])

    # signer.sign() signs using RSA-SHA256 with PKCS1v15 padding
    signature = binascii.hexlify(
        google_credentials.signer.sign(string_to_sign)
    ).decode()

    scheme_and_host = '{}://{}'.format('https', host)
    signed_url = '{}{}?{}&x-goog-signature={}'.format(
        scheme_and_host, canonical_uri, canonical_query_string, signature)

    return signed_url

import tempfile
ROOT_FOLDER = os.path.abspath(tempfile.gettempdir())
TMP_FOLDER = os.path.join(ROOT_FOLDER, "tmp")
PDF_FOLDER = os.path.join(TMP_FOLDER, "pdf")
JPG_FOLDER = os.path.join(TMP_FOLDER, "jpg")
ERROR_FOLDER = os.path.join(TMP_FOLDER, "error")

def create_tmp_folders():
    """
    Function that creates tmp local folders where to store intermediate files
    """
    for folder in (PDF_FOLDER, JPG_FOLDER, ERROR_FOLDER):
        if not os.path.exists(folder):
            os.makedirs(folder)
            print(f"make dir: {folder}")

def send_file(path, filepath):
    print("Uploading %s to %s", filepath, path)
    blob = __get_blob(path)
    blob.upload_from_filename(filepath)
    return blob

def __get_bucket(path):
    bucket_name, _ = __parse_url(path)
    return storage_client.get_bucket(bucket_name)

def get_data(path):
    print("Reading data from %s", path)

    blob = __get_blob(path)
    return blob.download_as_string()

def __get_blob(path):
    bucket_name, file_name = __parse_url(path)
    bucket = storage_client.get_bucket(bucket_name)
    return bucket.blob(file_name, chunk_size=10485760)  # 10MB


def __parse_url(path):
    match = re.match(r"^gs://(?P<bucket>[^\/]+)/(?P<file_name>.*)$", path)

    if match:
        return match.groups()

    print(f"Invalid GCS path: {path}")

def get_file(path, filepath):
    print("Reading data from %s to %s", path, filepath)

    blob = __get_blob(path)
    return blob.download_to_filename(filepath)

def main_run(event, context):
    gcs_input_uri = 'gs://' + event['bucket'] + '/' + event['name']
    print('Printing the contentType: ' + event['contentType'] + ' input:' + gcs_input_uri)

    uri = urlparse(gcs_input_uri)
    bucket = storage_client.get_bucket(uri.hostname)
    blob = bucket.get_blob(uri.path[1:])
    image_content = blob.download_as_bytes()

    if( event['contentType'] == 'application/pdf' ):
        print("create tmp folder")
        create_tmp_folders()
        print("convert pdf to image before processing")
        #gcs_uri_in = os.path.join("gs://", event["bucket"], event["name"])
        
        print(f"Preprocessing split from uri: {gcs_input_uri}")
        tmp_pdf_filepaths, success = split_one_pdf_pages(gcs_input_uri)

        tmp_jpg_filepaths = convert_all_pdf_to_jpg(tmp_pdf_filepaths)
        if tmp_jpg_filepaths != []:
            for file in tmp_jpg_filepaths:
                output=os.path.join(gcs_input_uri, os.path.basename(file))
                print(f"Send file: {file} to {output}")
            
                blob = send_file(output,        file)
                print(f"Successfully sent jpg pages of file{blob.public_url}  ")

        else:
            print("error convert_all_pdf_to_jpg return no files") 
        
        print(f"pdf converted to jpg to {blob.public_url} will now exit")
        # Copy input file to archive bucket
        backupAndDeleteInput(event)

        return

    t0 = time.time()

    if(event['contentType'] == 'image/gif' or event['contentType'] == 'application/pdf' 
    or event['contentType'] == 'image/tiff' or event['contentType'] == 'image/jpeg'):
        
        doc_type, name = getDocType(gcs_input_uri)
 
        document = {"content": image_content,"mime_type":event['contentType']}
        request = {"name": name, "raw_document": document }
                
        print("Wait for the operation to finish")

        result = docai_client.process_document(request=request ) #, retry= retry.Retry(deadline=60)  ,metadata=  ("jmb", "test"))
        print("Operation finished")
        
        document = result.document
        input_filename = gcs_input_uri

        key = os.path.dirname(uri.path)[1:]
        key = key.replace(doc_type,"").replace("/","")
        print(f"key: {key} - input_filename : {input_filename} - doc type : {doc_type} - EP url : {name}")

        # Reading all entities into a dictionary to write into a BQ table
        entities_extracted_dict = {}
        entities_extracted_dict['input_file_name'] = input_filename
        entities_extracted_dict['doc_type'] = doc_type
        entities_extracted_dict['key'] =  key
        #entities_extracted_dict['image'] = base64.b64encode(image_content).decode('ascii')
        entities_extracted_dict['insert_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        #entities_extracted_dict['result'] = json.dumps(document.entities).encode("utf-8")
        #print(str(result.document).replace('\n','\t').replace('\cr','\t'))

        entities = document.entities
        if not entities or len(entities) == 0:
            entity_text = "entities returned by docai is empty" 
            entity_type ="error"
            print(f"{entity_type} - {entity_text}")
            entities_extracted_dict[entity_type] = entity_text
        else:
            print(f"entities size: {len(entities)}")

            for entity in entities:
                print(str(entity).replace('\n','\t'))

                entity_type = cleanEntityType(entity.type_)
                entity_text = entity.mention_text
                entity_confidence = entity.confidence
                print(f"{entity_type}:{entity_text} - {entity_confidence}")

                # Normalize date format in case the entity being read is a date
                if "date" in entity_type:
                    #print(entity_text.replace('\n','\t'))
                    #d = datetime.strptime(entity_text, '%Y-%m-%d').date()
                    entity_text = entity_text.replace(' ', '')

                    try:
                        entity_text = parser.parse(entity_text).date().strftime('%Y-%m-%d')
                        entities_extracted_dict[entity_type] = entity_text
                        
                    except dateutil.parser._parser.ParserError  as err:
                        entity_type ="error"
                        entity_text = f"Parser error for entity type:value:{entity_type}:value:{entity_text}" 
                        entities_extracted_dict[entity_type] = entity_text
                    
                elif "amount" in entity_type:
                    #TODO: FIX this code
                    try:                        
                        entity_text = float(entity_text)
                    except ValueError  as err:
                        print("error: " + err)
                        try:                        
                            entity_text = entity_text.replace(',','.')
                        except ValueError  as err:
                            print("error: " + err)
                            entity_text = str(entity_text)
                    
                    entities_extracted_dict[entity_type] = entity_text

                else:
                    entity_text = str(entity_text)
                    
                    #print("Mention text : " + entity_text)
                    entities_extracted_dict[entity_type] = entity_text

                # Creating and publishing a message via Pub Sub to validate address
                if (isContainAddress(entity_type) ) :
                    if doc_type.find("fr") >=0:
                        entity_address = entity_text + " France"
                    else:
                        entity_address = entity_text
                    message = {
                        "entity_type": entity_type,
                        "entity_text": entity_address,
                        "input_file_name": input_filename,
                    }
                    message_data = json.dumps(message).encode("utf-8")

                    sendGeoCodeRequest(message_data)
                    #sendKGRequest(message_data)                        

                if (isContainName(entity_type) ) :
                    message = {
                        "entity_type": entity_type,
                        "entity_text": entity_text,
                        "input_file_name": input_filename,
                    }
                    message_data = json.dumps(message).encode("utf-8")

                    sendKGRequest(message_data)                        

        print("Read the text recognition output from the processor")
        for page in document.pages:
            for form_field in page.form_fields:
                print(f"form_field:{form_field}")
                entity_type = get_text(form_field.field_name, document)
                entity_type = cleanEntityType(entity_type)

                field_value = get_text(form_field.field_value, document)       
                print(f"{entity_type}:{field_value} ")

                entities_extracted_dict[entity_type] = field_value

        entities_extracted_dict['timer'] = int(time.time()-t0)

        signed_url = generate_signed_url("google.com_ml-baguette-demos-f1b859baa944.json", gcs_archive_bucket_name, event['name'])
        print(signed_url)
        entities_extracted_dict['signed_url'] = signed_url
        
        print(entities_extracted_dict)
        print(f"Writing to BQ: {input_filename}")
        # Write the entities to BQ
        write_to_bq(dataset_name, table_name, entities_extracted_dict)

        delete_blob = False
        if delete_blob == True:
            blob.delete()

        # Check business rules
        checkBusinessRule(dataset_name)

            # print(blobs)
            # Deleting the intermediate files created by the Doc AI Parser
            #blobs = bucket.list_blobs(prefix=gcs_output_uri_prefix)
            
        
        # Copy input file to archive bucket
        backupAndDeleteInput(event)

    else:
        print('Cannot parse the file type')

def backupAndDeleteInput(event):
    source_bucket = storage_client.bucket(event['bucket'])
    source_blob = source_bucket.blob(event['name'])
    destination_bucket = storage_client.bucket(gcs_archive_bucket_name)

    print(f"backup input file to: {destination_bucket.path}{event['name']}")
    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, event['name'])
    # delete from the input folder
    print(f"delete input file to: {source_blob.path} {event['name']}")
    source_blob.delete()

def cleanEntityType(entity_type):
    if entity_type is None:
        return ""
    return entity_type.replace(' ', '_').replace('/','_').lower()

# Extract shards from the text field
def get_text(doc_element: dict, document: dict):
    """
    Document AI identifies form fields by their offsets
    in document text. This function converts offsets
    to text snippets.
    """
    response = ""
    # If a text segment spans several lines, it will
    # be stored in different text segments.
    for segment in doc_element.text_anchor.text_segments:
        start_index = (
            int(segment.start_index)
            if segment in doc_element.text_anchor.text_segments
            else 0
        )
        end_index = int(segment.end_index)
        response += document.text[start_index:end_index]
    return response



def main_run_batch(event, context):
    gcs_input_uri = 'gs://' + event['bucket'] + '/' + event['name']
    print('Printing the contentType: ' + event['contentType'] + ' input:' + gcs_input_uri)

    t0 = time.time()

    if(event['contentType'] == 'image/gif' or event['contentType'] == 'application/pdf' 
    or event['contentType'] == 'image/tiff' or event['contentType'] == 'image/jpeg'):
        
        doc_type, name = getDocType(gcs_input_uri)
        l_destination_uri = destination_uri + event['name'] + '/'
        print('destination_uri:' + l_destination_uri)

        gcs_documents = documentai.GcsDocuments(
                documents=[{"gcs_uri": gcs_input_uri, "mime_type": event['contentType']}]
            )

        input_config = documentai.BatchDocumentsInputConfig(gcs_documents=gcs_documents)

        # Where to write results
        output_config = documentai.DocumentOutputConfig(
            gcs_output_config={"gcs_uri": destination_uri}
        )

        request = documentai.types.document_processor_service.BatchProcessRequest(
            name=name,
            input_documents=input_config,
            document_output_config=output_config,
        )

        operation = docai_client.batch_process_documents(request)

        print("Wait for the operation to finish")
        
        operation.result(timeout=timeout)
        print("Operation finished")

        match = re.match(r"gs://([^/]+)/(.+)", l_destination_uri)
        output_bucket = match.group(1)
        prefix = match.group(2)

        # Get a pointer to the GCS bucket where the output will be placed
        bucket = storage_client.get_bucket(output_bucket)

        blob_list = list(bucket.list_blobs(prefix=prefix))
        print(f"Processing output files from prefix: {prefix}")

        for i, blob in enumerate(blob_list):
            # Download the contents of this blob as a bytes object.
            if ".json" not in blob.name:
                print("blob name " + blob.name)
                print(f"skipping non-supported file type {blob.name}")
            else:
                # Setting the output file name based on the input file name
                print("Fetching from " + blob.name + " for input_filename " + gcs_input_uri)
                #start = blob.name.rfind("/") + 1
                #end = blob.name.rfind(".") + 1
                input_filename = gcs_input_uri #blob.name[start:end:] + "gif"
                
                
                # Getting ready to read the output of the parsed document - setting up "document"
                blob_as_bytes = blob.download_as_bytes()
                document = json.loads( blob_as_bytes)
                print(document)
                #document = documentai.types.Document.from_json(blob_as_bytes)

                # Reading all entities into a dictionary to write into a BQ table
                entities_extracted_dict = {}
                entities_extracted_dict['input_file_name'] = input_filename
                entities_extracted_dict['doc_type'] = doc_type
                entities_extracted_dict['insert_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                entities = document['entities']
                if not entities:
                    entity_text = "entities returned by docai is empty" 
                    entity_type ="error"
                    print(f"{entity_type} - {entity_text}")
                    entities_extracted_dict[entity_type] = entity_text
                else:

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
                        if len(entity_normalized_value) > 0: 
                            print("Normalized text : " +
                                    entity_normalized_value)
                        
                        #entity_text = str(entity.mentionText)
                        # Normalize date format in case the entity being read is a date
                        if "date" in entity_type:
                            print(entity_text)
                            #d = datetime.strptime(entity_text, '%Y-%m-%d').date()
                            entity_text = entity_text.replace(' ', '')

                            try:
                                entity_text = parser.parse(entity_text).date().strftime('%Y-%m-%d')
                                entities_extracted_dict[entity_type] = entity_text
                                break
                            except dateutil.parser._parser.ParserError  as err:
                                entity_type ="error"
                                entity_text = f"Parser error for entity type:value:{entity_type}:value:{entity_text}" 
                                entities_extracted_dict[entity_type] = entity_text
                            
                        else:
                            entity_text = str(entity_text)
                            
                            #print("Mention text : " + entity_text)
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

                entities_extracted_dict['timer'] = int(time.time()-t0)

                print(entities_extracted_dict)
                print(f"Writing to BQ: {input_filename}")
                # Write the entities to BQ
                write_to_bq(dataset_name, table_name, entities_extracted_dict)

                delete_blob = False
                if delete_blob == True:
                    blob.delete()

                # Check business rules
                checkBusinessRule(dataset_name)

            # print(blobs)
            # Deleting the intermediate files created by the Doc AI Parser
            #blobs = bucket.list_blobs(prefix=gcs_output_uri_prefix)
            
        
        # Copy input file to archive bucket
        source_bucket = storage_client.bucket(event['bucket'])
        source_blob = source_bucket.blob(event['name'])
        destination_bucket = storage_client.bucket(gcs_archive_bucket_name)

        print(f"backup input file to: {destination_bucket.url}{event['name']}")
        blob_copy = source_bucket.copy_blob(
            source_blob, destination_bucket, event['name'])
        # delete from the input folder
        print(f"delete input file to: {source_blob.url} {event['name']}")
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



def write_single_page_pdf(inputpdf: PdfFileReader, num_page: int, filename: str) -> str:
    """
    Function that exports a page of an input pdf into a local file of a single page

    Parameters
    ----------
    inputpdf: PdfFileReader
        pdf file reader of raw input pdf
    num_page: int
        page number of input pdf to export
    filename: str
        filename of raw input

    Returns
    -------
    output_paths: list
        list of local paths where mono page pdf files are stored
    """
    outputpdf = PdfFileWriter()
    outputpdf.addPage(inputpdf.getPage(num_page))
    filepath_out = os.path.join(
        PDF_FOLDER, filename.replace(".pdf", f"_page_{num_page}.pdf")
    )
    with open(filepath_out, "wb") as f:
        outputpdf.write(f)
    return filepath_out


def split_one_pdf_pages(gcs_uri_in: str) -> Union[List[str], bool]:
    """
    Function that splits one pdf of N pages into N pdfs of 1 page

    Parameters
    ----------
    gcs_uri_in: str
        gcs uri of pdf file to split

    Returns
    -------
    output_paths: list
        list of local paths where mono page pdf files are stored
    success: bool
        True if splitting was a success, False else
    """
    if not gcs_uri_in.endswith("pdf"):
        print(f"File in {gcs_uri_in} is not a pdf file")
        local_filepath = os.path.join(ERROR_FOLDER, os.path.basename(gcs_uri_in))
        get_file(gcs_uri_in, local_filepath)
        return local_filepath, False
    filename = os.path.basename(gcs_uri_in)
    raw_data = get_data(gcs_uri_in)
    inputpdf = PdfFileReader(io.BytesIO(raw_data), strict=False)
    output_paths = []
    for num_page in range(inputpdf.numPages):
        filepath_out = write_single_page_pdf(inputpdf, num_page, filename)
        output_paths.append(filepath_out)
    return output_paths, True


def convert_to_image(filepath_in: str) -> str:
    """
    Function that converts a single page pdf file into an image
    taken from https://stackoverflow.com/questions/331918/converting-a-pdf-to-a-series-of-images-with-python

    Parameters
    ----------
    filepath_in: str
        local filepath of a pdf file

    Returns
    -------
    filepath_out: str
        local filepath of the converted jpg file
    """
    filepath_out = filepath_in.replace("pdf", "jpg")
    args = [
        "pdf2jpeg",  # actual value doesn't matter
        "-dNOPAUSE",
        "-sDEVICE=jpeg",
        "-r144",
        "-sOutputFile=" + filepath_out,
        filepath_in,
    ]
    Ghostscript(*args)
    return filepath_out


def convert_all_pdf_to_jpg(pdf_filepaths: List[str]) -> List[str]:
    """
    Function that converts a single page pdf file into an image

    Parameters
    ----------
    pdf_filepaths: List[str]
        list of local filepaths to single pages pdf files

    Returns
    -------
    jpg_filepaths: str
        list of local filepaths to single pages jpg files
    """
    jpg_filepaths = []
    for filepath_in in pdf_filepaths:
        filepath_out = convert_to_image(filepath_in)
        jpg_filepaths.append(filepath_out)
    return jpg_filepaths


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
        else:
            print(f"key/value {key} = {entities_extracted_dict[key]}")

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
        print(f'Row returned: {row_rule_matched.name} = {row_rule_matched.input_file_name}')
        row_to_insert[str(row_rule_matched.name)] = str(row_rule_matched.input_file_name)

    return row_to_insert




# Old code

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

    doc_type, name = getDocType(input)



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
