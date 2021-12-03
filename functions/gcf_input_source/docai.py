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
from predict_types import predict_image_classification
import gcs
import kg
from convert import convert_date
from function_variables import FunctionVariables

var = FunctionVariables()

timeout = 300

docai_client_eu = documentai.DocumentProcessorServiceClient(
    client_options={"api_endpoint": f"eu-documentai.googleapis.com"})
docai_client_us = documentai.DocumentProcessorServiceClient()


def get_docaiclient(location: str) -> ProcessResponse:
    if(location == "eu"):
        return docai_client_eu
    else:
        return docai_client_us


name = f"projects/660199673046/locations/eu/processors/7b9066f18d0c7366"
docai_fr_driver_license = "projects/660199673046/locations/eu/processors/cee6a410ee499b69"
docai_fr_national_id = "projects/660199673046/locations/eu/processors/57993de9b197ee1f"
docai_us_passport = "projects/660199673046/locations/eu/processors/3bd9c32d439b29cf"
docai_us_driver_license = "projects/660199673046/locations/eu/processors/57993de9b197e27c"
#docai_invoice = "projects/660199673046/locations/eu/processors/abf12796440cc270"
docai_invoice = "projects/660199673046/locations/eu/processors/1371b8b51ffedfe8"
docai_generic_form_parser = "projects/660199673046/locations/eu/processors/aa8e86c6aa939dc"

docai_CDC_parser = "projects/660199673046/locations/eu/processors/6eebfeaa729d5106"
docai_CDE_RI = "projects/660199673046/locations/us/processors/172b5b9064c5a2fb"
docai_identity_fraud_detector = "projects/660199673046/locations/eu/processors/e1b002e051d94abc"
docai_contract = "projects/660199673046/locations/us/processors/9ee5a69d042731a4"

docai_processors = {
    "fr_driver_license": [docai_fr_driver_license, "eu", "fr_driver_license"],
    "fr_national_id":  [docai_fr_national_id, "eu", "fr_national_id"],
    "fr_passport":  [docai_us_passport, "eu", "fr_passport_not_yet_supported"],

    "invoice":  [docai_invoice, "eu", "invoice"],

    "us_passport":  [docai_us_passport, "eu", "us_passport"],
    "us_driver_license":  [docai_us_driver_license, "eu", "us_driver_license"],

    "_contract_": [docai_contract, "us", "_contract_"],

    "_generic_form_":  [docai_generic_form_parser, "eu", "_generic_form_"],
    "_ri_": [docai_CDE_RI, "us", "_ri_"],
    "_identity_fraud_detector_": [docai_identity_fraud_detector, "eu", "_identity_fraud_detector_"],
    "_constat_":  [docai_generic_form_parser, "eu", "_constat_"],
    "_ci_":  ["projects/660199673046/locations/us/processors/9cf695f04326387d", "us", "_ci_"]
}


def get_doctype(input: str, file_content = None):
    """[summary]
    Return type of the document associated with the parser address

    Args:
        input (str): [description]

    Returns:
        str: document type based on keyword find in the url
        str: docAI parser address based on keyword find in the url
    """

    input_lower = input.lower()
    paths = input_lower.split(os.sep)

    paths = set(paths)
    doc_type = list(set(docai_processors.keys()).intersection(paths))

    if len(doc_type) > 0:
        doc_type = doc_type[0]
        print(f"doc_type : {doc_type}")
        processor_path = docai_processors[doc_type][0]
        processor_location = docai_processors[doc_type][1]
    else:
        if (input_lower.endswith(".jpg") or input_lower.endswith(".jpg")) and file_content != None :
            print("Try to find the type of the document with AutoML")
            doc_type = predict_image_classification(file_content)
            return get_doctype(doc_type)

        else:
            doc_type = "_unknown_"
            processor_path = ""
            processor_location = ""
            print(f"_unknown_ type: {input_lower}")

    print(f"doc_type:{doc_type} - processor_path: {processor_path} - location: {processor_location}")
    return doc_type, processor_path, processor_location



def docai_extract_doc_from_json(image_content):
    document = documentai.types.Document.from_json(image_content)
    return document


def parse_docairesult(event, gcs_input_uri, uri, t0, doc_type, name, document: documentai.Document, result, key=None):
    input_filename = gcs_input_uri

    entities_extracted_dict = init_results(uri, doc_type, name, input_filename, key)

    try:
        if result:
            print(f"hitl: {result.human_review_status}")
            hitl = documentai.HumanReviewStatus(result.human_review_status)
            print(f"hitl in progress state_message: {hitl.state_message} - human_review_operation: {hitl.human_review_operation}")
            entities_extracted_dict['hitl'] = hitl.state_message
    except Exception as err:
        print(f"error in hitl processing: {input_filename} - ERROR: {err}")

    entities_extracted_dict, entities_results = parse_entities(doc_type, document, input_filename, entities_extracted_dict)

    print("Read the text recognition output from the processor")
    signed_urls = []
    for page in document.pages:
        page = documentai.Document.Page(page)
        print(f"**** Page {page.page_number} ****")

        try:
            print(f"Found {len(page.tables)} table(s):")
            process_tables = True
            if process_tables == True:
                for table in page.tables:
                    print(str(table).replace("\n",""))
                    table = documentai.Document.Page.Table(table)

                    num_rows = len(table.body_rows)
                    print(f'Table with {num_rows} rows:')
                    if num_rows > 0:
                        header_row_text, body_row_text, success = print_table_info(table, document.text)
                        if success == True:
                            confidence = 0.0
                            entities_results.append({"type":header_row_text, "value":body_row_text, "confidence":confidence})
        except Exception as err:
            print(f"ERROR table {input_filename}")
            print(err)
        # try:
        #     p = documentai.Document.Page(page)
        #     if p.image:
        #         print(f"found an image in docai result in {input_filename}")
        #         i = documentai.Document.Page.Image(p.image)

        #         signed_url_img = gcs.extract_signed_url_from_bytes(
        #             event, p.page_number, i.content, i.mime_type)

        #         entities_extracted_dict['signed_url'] = signed_url_img
        #         signed_urls.append(signed_url_img)
        # except Exception as err:
        #     print(f"image error  {input_filename}")
        #     print(err)
        
        print(f'Found {len(page.form_fields)} form fields:')
        for form_field in page.form_fields:
            entity_type = layout_to_text(form_field.field_name, document.text)
            entity_type = convert.cleanEntityType(entity_type)

            entity_text = layout_to_text(form_field.field_value, document.text)            
            entities_results.append({"type":entity_type, "value":entity_text, "confidence":0.0})
            print(f"{entity_type}:{entity_text} ")

            entities_extracted_dict[entity_type] = entity_text

    entities_extracted_dict["entities"] = entities_results
    entities_extracted_dict['timer'] = int(time.time()-t0)

    signed_url = gcs.generate_signed_url(
        "google.com_ml-baguette-demos.json", var.gcs_archive_bucket_name, event['name'])
    print(signed_url)
    
    entities_extracted_dict['signed_url'] = signed_url
    
    entities_extracted_dict['output_file_name'] = os.path.join("gs://", var.gcs_archive_bucket_name, event['name'])

    if len(signed_urls) > 0:
        print(signed_urls)
        entities_extracted_dict['signed_urls'] = signed_urls

    print(entities_extracted_dict)
    print(f"Writing to BQ: {input_filename}")
    return entities_extracted_dict

def parse_entities(doc_type, document, input_filename, entities_extracted_dict):
    entities_results = []

    entities = document.entities
    if not entities or len(entities) == 0:
        entities_extracted_dict = log_error(entities_extracted_dict, "WARNING entities returned by docai is empty", "warning")
    else:
        print(f"entities size: {len(entities)}")

        for entity in entities:
            print(str(entity).replace('\n', '\t'))
            entity = documentai.Document.Entity(entity)
            entity_type = convert.cleanEntityType(entity.type_)
            entity_text = entity.mention_text
            entity_confidence = entity.confidence
            has_normized_value = False
            try:
                nv = documentai.Document.Entity.NormalizedValue(
                    entity.normalized_value)
                if nv.text:
                    print(f"normalized_value: {nv.text}")
                    entity_text = nv.text
                    has_normized_value = True
            except Exception as err:
                print(f"warning normalized value not available: {err}")

            print(f"{entity_type}:{entity_text} - {entity_confidence}")
            if has_normized_value == True:
                print(f"using normalized value: {entity_text}")
                entity_text = str(entity_text)
                entities_extracted_dict[entity_type] = entity_text

            # Normalize date format in case the entity being read is a date
            elif "date" in entity_type:
                entity_text = convert_date(entity_text)
                if entity_text == None:
                    entity_type = "error"
                    entity_text = f"Parser error for entity type:value:{entity_type}:value:{entity_text}"
                    print(entity_text)
                
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

            entities_results.append({"type":entity_type, "value":entity_text, "confidence":entity_confidence})

            # Creating and publishing a message via Pub Sub to validate address
            if (is_contain_address(entity_type)):
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
                if (is_contain_name(entity_type)):
                    message = {
                        "entity_type": entity_type,
                        "entity_text": entity_text,
                        "input_file_name": input_filename,
                    }
                    message_data = json.dumps(message).encode("utf-8")

                    kg.sendKGRequest(message_data)
    return entities_extracted_dict, entities_results 

def print_table_info(table: dict, text: str) :
    # Print header row
    body_row_text = ''
    header_row_text = ''
    try:
        if table.header_rows  and len(table.header_rows) > 0 :
            for header_row in table.header_rows:
                for header_cell in header_row.cells:
                    header_cell_text = layout_to_text(header_cell.layout, text)
                    header_row_text += f'{repr(header_cell_text.strip())} | '
                print(f'Collumns: {header_row_text[:-3]}')
        
        # Print first body row
        if table.body_rows and len(table.body_rows) > 0 :
            for body_row in table.body_rows:
                for body_cell in body_row.cells:
                    body_cell_text = layout_to_text(body_cell.layout, text)
                    body_row_text += f'{repr(body_cell_text.strip())} | '
                print(f'First row data: {body_row_text[:-3]}\n')
            
        return header_row_text, body_row_text, True
    except Exception as err:
        print(f"ERROR in print_table_info {err}")
        print(err)

    return header_row_text, body_row_text, False

def layout_to_text(layout: dict, text: str) -> str:
    """
    Document AI identifies form fields by their offsets in the entirity of the
    document's text. This function converts offsets to a string.
    """
    response = ""
    # If a text segment spans several lines, it will
    # be stored in different text segments.
    for segment in layout.text_anchor.text_segments:
        start_index = (
            int(segment.start_index)
            if segment in layout.text_anchor.text_segments
            else 0
        )
        end_index = int(segment.end_index)
        response += text[start_index:end_index]
    return response

def init_results(uri, doc_type, name, input_filename, key=None):
    key = get_documentkey(uri, doc_type, key)
    print(
        f"key: {key} - input_filename : {input_filename} - doc type : {doc_type} - EP url : {name}")
    # Reading all entities into a dictionary to write into a BQ table
    entities_extracted_dict = {}
    entities_extracted_dict['input_file_name'] = input_filename
    entities_extracted_dict['doc_type'] = doc_type
    entities_extracted_dict['key'] = key
    entities_extracted_dict['insert_date'] = datetime.now().strftime(
        '%Y-%m-%d %H:%M:%S')
        
    return entities_extracted_dict

def get_documentkey(uri, doc_type, key):
    if key==None or key=="":
        key = os.path.dirname(uri.path)[1:]
        key = key.replace(doc_type, "").replace("/", "")
        if key == "":
            key = uri.path
            key = key.replace(doc_type, "").replace("/", "_")

            
    return key


def log_error(entities_extracted_dict, entity_text, entity_type = "error"):

    
    print(f"{entity_type} - {entity_text}")
    entities_extracted_dict[entity_type] = entity_text
    return entities_extracted_dict




# Extract shards from the text field




def batch(event, gcs_input_uri):
    doc_type, processor_path, processor_location = get_doctype(gcs_input_uri)
    l_destination_uri = var.destination_uri + event['name'] + '/'
    print('destination_uri:' + l_destination_uri)

    gcs_documents = documentai.GcsDocuments(
        documents=[{"gcs_uri": gcs_input_uri,
                    "mime_type": event['contentType']}]
    )

    input_config = documentai.BatchDocumentsInputConfig(
        gcs_documents=gcs_documents)

    # Where to write results
    output_config = documentai.DocumentOutputConfig(
        gcs_output_config={"gcs_uri": var.destination_uri}
    )

    request = documentai.types.document_processor_service.BatchProcessRequest(
        name=processor_path,
        input_documents=input_config,
        document_output_config=output_config,
    )

    operation = get_docaiclient(
        processor_location).batch_process_documents(request)

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
    return doc_type, blob_list


# Doc AI helper

def is_contain_address(entity_type):
    address_substring_tab = ["address", "adresse"]

    for address_substring in address_substring_tab:
        if address_substring in entity_type.lower():
            print("find address:" + entity_type)
            return True

    return False


def is_contain_name(entity_type):
    if "family" in entity_type.lower():
        return True
    if "given" in entity_type.lower():
        return True
    return False


def get_key(entity, key):
    if key in entity:
        return entity[key]

    return ''


def process_raw_bytes(event, gcs_input_uri, uri, image_content, t0, doc_type, name, processor_location, key=None):
    document = {"content": image_content, "mime_type": event['contentType']}
    request = {"name": name, "raw_document": document}

    print("Wait for the operation to finish")

    # , retry= retry.Retry(deadline=60)  ,metadata=  ("jmb", "test"))
    try:
        result = get_docaiclient(
            processor_location).process_document(request=request)
        print("Operation finished succefully")

    except Exception as err:
        entities_extracted_dict = log_error(init_results(uri, doc_type, name, gcs_input_uri), f"ERROR when processing document {err}")
        print(f"error in docai processing raw_bytes: {gcs_input_uri} - ERROR: {err} from request: name: " + name + " mime_type: " + event['contentType'])
        print(err)
        return entities_extracted_dict

    try:
        entities_extracted_dict = parse_docairesult(
            event, gcs_input_uri, uri, t0, doc_type, name, result.document, result, key)

    except Exception as err:
        text = f"error in parseDocAIResult : {gcs_input_uri} - ERROR: {err} from doc_type: " + doc_type + f" result: {result}" 
        entities_extracted_dict = log_error(init_results(uri, doc_type, name, gcs_input_uri), text)
        print(text)
        print(err)

    return entities_extracted_dict


def get_df(document, name, doc_type):
    lst = [[]]
    lst.pop()

    entities = document.get('entities')
    if entities:
        for entity in document['entities']:
            print(entity)

            entity_type = entity['type']
            val = entity['mentionText']
            confidence = entity['confidence']
            lst.append([entity_type, val, entity_type, confidence, name, doc_type])
    else:
        entity_type = "ERROR"
        val = "Nothing parsed"
        confidence = 1.0
        lst.append([entity_type, val, entity_type, confidence, name, doc_type])

    df = pd.DataFrame(lst, columns=['key', 'value', 'type', 'confidence', 'file', 'doc_type']
                      )
    return df


def curl_docai_alpha(gcs_file_path: str, document_type, file_mime_type: str):
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
              mime_type: '"""+file_mime_type+"""',
              gcs_source: {
                uri: '"""+gcs_file_path+"""'
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
