import base64
import datetime
import io
import json
import logging
import os
import re
import tempfile
import time
import traceback
from datetime import datetime
from typing import List, Union
from urllib.parse import urlparse

import dateutil
import pandas as pd
import pandas_gbq
from dateutil import parser
from ghostscript import Ghostscript
from google.api_core import retry
from google.cloud import bigquery
from google.cloud import documentai_v1 as documentai
from google.cloud import pubsub_v1
from google.cloud.documentai_v1.types.document_processor_service import \
    ProcessResponse
from PyPDF2 import PdfFileReader, PdfFileWriter

import convert 
import gcs 
import kg 
from function_variables import FunctionVariables 

var = FunctionVariables()

timeout = 300

location = os.environ.get('PARSER_LOCATION')
processor_id = os.environ.get('PROCESSOR_ID')

opts = {"api_endpoint": f"{location}-documentai.googleapis.com"}
docai_client = documentai.DocumentProcessorServiceClient(client_options=opts)


name = f"projects/660199673046/locations/eu/processors/7b9066f18d0c7366"
docai_fr_driver_license = "projects/660199673046/locations/eu/processors/cee6a410ee499b69"
docai_fr_national_id = "projects/660199673046/locations/eu/processors/57993de9b197ee1f"
docai_us_passport = "projects/660199673046/locations/eu/processors/3bd9c32d439b29cf"
docai_us_driver_license = "projects/660199673046/locations/eu/processors/57993de9b197e27c"
#docai_invoice = "projects/660199673046/locations/eu/processors/abf12796440cc270"
docai_invoice = "projects/660199673046/locations/eu/processors/1371b8b51ffedfe8"
docai_generic_form_parser = "projects/660199673046/locations/eu/processors/aa8e86c6aa939dc"

docai_CDC_parser = "projects/660199673046/locations/eu/processors/6eebfeaa729d5106"
docai_CDE_parser = "projects/660199673046/locations/eu/processors/f1d162a01e613265"

def getDocType(input: str):
    """[summary]
    Return type of the document associated with the parser address

    Args:
        input (str): [description]

    Returns:
        str: document type based on keyword find in the url
        str: docAI parser address based on keyword find in the url
    """
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

    elif input.lower().find("_generic_form_") >= 0:
        doc_type = "generic_form"
        processor_path = docai_generic_form_parser
    
    elif input.lower().find("_RI_") >= 0:
        doc_type = "_RI_"
        processor_path = docai_CDC_parser

    elif input.lower().find("_unknown_") >= 0:
        doc_type = "CDC"
        processor_path = docai_CDC_parser

    elif input.lower().find("_custom_") >= 0:
        doc_type = "CDE"
        processor_path = docai_CDC_parser

    else:
        print("Type unknown. Using generic form parser.")
        doc_type = "generic_form_parser"
        processor_path = docai_generic_form_parser

    print(f"doc_type:{doc_type} - processor_path: {processor_path}")
    return doc_type, processor_path


def docai_extract_doc_from_json(image_content):
    document = documentai.types.Document.from_json(image_content)
    return document


def parseDocAIResult(event, gcs_input_uri, uri, t0, doc_type, name, document: documentai.Document, result):
    input_filename = gcs_input_uri

    key = os.path.dirname(uri.path)[1:]
    key = key.replace(doc_type, "").replace("/", "")
    print(
        f"key: {key} - input_filename : {input_filename} - doc type : {doc_type} - EP url : {name}")

    # Reading all entities into a dictionary to write into a BQ table
    entities_extracted_dict = {}
    entities_extracted_dict['input_file_name'] = input_filename
    entities_extracted_dict['doc_type'] = doc_type
    entities_extracted_dict['key'] = key
    #entities_extracted_dict['image'] = base64.b64encode(image_content).decode('ascii')
    entities_extracted_dict['insert_date'] = datetime.now().strftime(
        '%Y-%m-%d %H:%M:%S')
    #entities_extracted_dict['result'] = json.dumps(document.entities).encode("utf-8")
    # print(str(result.document).replace('\n','\t').replace('\cr','\t'))

    try:
        if result:
            print(f"hitl: {result.human_review_status}")
            hitl = documentai.HumanReviewStatus(result.human_review_status)
            if hitl.state == documentai.HumanReviewStatus.State.SKIPPED:
                print("hitl in progress")
                print(f"hitl {hitl.state_message}")
                entities_extracted_dict['hitl'] = hitl.state_message
                print(f"hitl {hitl.human_review_operation}")
    except Exception as err:
        print("error in hitl processing")
        print(err)

    entities = document.entities
    if not entities or len(entities) == 0:
        entities_extracted_dict = log_error(entities_extracted_dict)
    else:
        print(f"entities size: {len(entities)}")

        for entity in entities:
            print(str(entity).replace('\n', '\t'))

            entity_type = cleanEntityType(entity.type_)
            entity_text = entity.mention_text
            entity_confidence = entity.confidence
            print(f"{entity_type}:{entity_text} - {entity_confidence}")

            # Normalize date format in case the entity being read is a date
            if "date" in entity_type:
                # print(entity_text.replace('\n','\t'))
                #d = datetime.strptime(entity_text, '%Y-%m-%d').date()
                entity_text = entity_text.replace(' ', '')

                try:
                    entity_text = parser.parse(
                        entity_text).date().strftime('%Y-%m-%d')
                    entities_extracted_dict[entity_type] = entity_text

                except dateutil.parser._parser.ParserError as err:
                    entity_type = "error"
                    entity_text = f"Parser error for entity type:value:{entity_type}:value:{entity_text}"
                    entities_extracted_dict[entity_type] = entity_text

            elif "amount" in entity_type:
                # TODO: FIX this code
                try:
                    entity_text = convert.convert_str_to_float(entity_text)
                except Exception as err:
                    entities_extracted_dict = log_error(
                        entities_extracted_dict, "warning float cast after replace , by . : " + entity_text)
                    entity_text = 0.0

                entities_extracted_dict[entity_type] = entity_text

            else:
                entity_text = str(entity_text)

                #print("Mention text : " + entity_text)
                entities_extracted_dict[entity_type] = entity_text

            # Creating and publishing a message via Pub Sub to validate address
            if (isContainAddress(entity_type)):
                if doc_type.find("fr") >= 0:
                    entity_address = entity_text + " France"
                else:
                    entity_address = entity_text
                message = {
                    "entity_type": entity_type,
                    "entity_text": entity_address,
                    "input_file_name": input_filename,
                }
                message_data = json.dumps(message).encode("utf-8")

                kg.sendGeoCodeRequest(message_data)
                # sendKGRequest(message_data)

            if var.SendKGRequest == True:
                if (isContainName(entity_type)):
                    message = {
                        "entity_type": entity_type,
                        "entity_text": entity_text,
                        "input_file_name": input_filename,
                    }
                    message_data = json.dumps(message).encode("utf-8")

                    kg.sendKGRequest(message_data)

    print("Read the text recognition output from the processor")
    signed_urls = []
    for page in document.pages:
        for form_field in page.form_fields:
            # print(f"form_field:{form_field}")
            entity_type = get_text(form_field.field_name, document)
            entity_type = cleanEntityType(entity_type)

            field_value = get_text(form_field.field_value, document)
            print(f"{entity_type}:{field_value} ")

            entities_extracted_dict[entity_type] = field_value
            try:

                p = documentai.Document.Page(page)
                if p.image:
                    i = documentai.Document.Page.Image(p.image)
                    signed_url = gcs.extract_signed_url_from_bytes(event, p.page_number, i.content, i.mime_type)
                    entities_extracted_dict['signed_url'] = signed_url
                    signed_urls.append(signed_url)
            except Exception as err:
                print("image error")
                print(err)

    entities_extracted_dict['timer'] = int(time.time()-t0)

    if 'signed_url' not in entities_extracted_dict:
        signed_url = gcs.generate_signed_url(
            "google.com_ml-baguette-demos-f1b859baa944.json", var.gcs_archive_bucket_name, event['name'])
        print(signed_url)
        entities_extracted_dict['signed_url'] = signed_url
    else:
        signed_url =  gcs.generate_signed_url(
            "google.com_ml-baguette-demos-f1b859baa944.json", var.gcs_archive_bucket_name, event['name'])
        print(signed_url)
        signed_urls.append(signed_url)

    print(signed_urls)
    entities_extracted_dict['signed_urls'] = signed_urls

    print(entities_extracted_dict)
    print(f"Writing to BQ: {input_filename}")
    return entities_extracted_dict




def log_error(entities_extracted_dict, entity_text="entities returned by docai is empty"):

    entity_type = "error"
    print(f"{entity_type} - {entity_text}")
    entities_extracted_dict[entity_type] = entity_text
    return entities_extracted_dict


def cleanEntityType(entity_type):
    if entity_type is None:
        return ""
    return entity_type.replace(' ', '_').replace('/', '_').lower()

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


def batch(event, gcs_input_uri):
    doc_type, name = getDocType(gcs_input_uri)
    l_destination_uri = var.destination_uri + event['name'] + '/'
    print('destination_uri:' + l_destination_uri)

    gcs_documents = documentai.GcsDocuments(
                documents=[{"gcs_uri": gcs_input_uri, "mime_type": event['contentType']}]
            )

    input_config = documentai.BatchDocumentsInputConfig(gcs_documents=gcs_documents)

        # Where to write results
    output_config = documentai.DocumentOutputConfig(
            gcs_output_config={"gcs_uri": var.destination_uri}
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
    bucket = gcs.storage_client.get_bucket(output_bucket)

    blob_list = list(bucket.list_blobs(prefix=prefix))
    print(f"Processing output files from prefix: {prefix}")
    return doc_type,blob_list


## Doc AI helper

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




def process_raw_bytes(event, gcs_input_uri, uri, image_content, t0, doc_type, name):
    document = {"content": image_content,"mime_type":event['contentType']}
    request = {"name": name, "raw_document": document }
                
    print("Wait for the operation to finish")

    result = ProcessResponse(docai_client.process_document(request=request )) #, retry= retry.Retry(deadline=60)  ,metadata=  ("jmb", "test"))
    print("Operation finished")
 
    entities_extracted_dict = parseDocAIResult(event, gcs_input_uri, uri, t0, doc_type, name, result.document, result)
    return entities_extracted_dict






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



def curl_docai_alpha(gcsFilePath: str, document_type, fileMimeType: str):
    import requests

    api = "https://eu-alpha-documentai.googleapis.com/v1alpha1/projects/google.com:ml-baguette-demos/locations/eu/documents:process"
    headers = {
        "Authorization": var.getToken(),
        "Content-Type": "application/json",
        "charset": "utf-8",
        "X-Goog-User-Project": var.get_env()

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
