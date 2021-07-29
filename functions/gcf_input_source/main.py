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

def main_run(event, context):
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

    bqTableName = os.environ.get('BQ_TABLENAME', 'ERROR: Specified environment variable is not set.')
    if bqTableName in "ERROR":
        logging.fatal('inputPdfPath variable is not set exit program')
        return

    input = "gs://" + event['bucket'] +"/" + event['name']

    if input.lower().endswith(".pdf"):

        project_id = get_env()
        print(project_id)

        doc_type = 'fr_driver_license'
        row = kyc(input, doc_type, 'application/pdf')
        document = row
        df = getDF(document, input, doc_type)
    
        print(df)
        print("df json")
        print(df.to_json())
            
        print("Start Insert BQ : " + bqTableName)

        pandas_gbq.to_gbq(df, bqTableName, if_exists='append')
        print("Insert BQ done in : " + bqTableName)
    else:
        print("Unsuported extention: " + input)

    return "OK"

def getDF(document   , name, doc_type):
    lst = [[]]
    lst.pop()


    for entity in document['entities']:       
        print(entity)

        type = entity['type']
        val = entity['mentionText']
        confidence = entity['confidence']
        lst.append([type, val, type, confidence, name, doc_type ]) 

    df = pd.DataFrame(lst
                          ,columns =['key', 'value', 'type', 'confidence', 'file', 'doc_type']
                         )

    
    return  df


def bqInsert(rows_to_insert, table_id):
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
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
    return "Bearer " +credentials.token
    

def kyc(gcsFilePath : str, document_type, fileMimeType : str):
    import requests

    api = "https://eu-alpha-documentai.googleapis.com/v1alpha1/projects/google.com:ml-baguette-demos/locations/eu/documents:process"
    headers = {
        "Authorization": getToken(),
        "Content-Type" : "application/json",
        "charset"      : "utf-8" ,
        "X-Goog-User-Project"            : get_env()

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




def get_env():
    

    print(os.environ)
    if 'GCP_PROJECT' in os.environ:
       return os.environ['GCP_PROJECT']

    import google.auth

    _,project_id = google.auth.default()
    print(project_id)
    return project_id


def split_pdf(inputpdf, start_page, end_page, uri, gcs_output_uri : str, gcs_output_uri_prefix :str):
    storage_client = storage.Client()

    with io.StringIO() as stream:

        print("numPages: {}".format( inputpdf.numPages))

        output = PdfFileWriter()
        for i in range (start_page , end_page+1):
            output.addPage(inputpdf.getPage(i))
            print("add page {}".format(i))

        file = uri.path[:-4] +"-page-{}-to-{}.pdf".format( start_page, end_page )
        print(file)

        buf = io.BytesIO()
        output.write(buf)
        data =buf.getvalue()
        outputBlob =  gcs_output_uri_prefix  + file
        print("Start write:"+outputBlob)
        bucket = storage_client.get_bucket(urlparse(gcs_output_uri).hostname)

        bucket.blob(outputBlob).upload_from_string(data, content_type='application/pdf')

        stream.truncate(0)
        
    print("split finish")

def pages_split(text: str, document: dict, uri, gcs_output_uri : str, gcs_output_uri_prefix :str ):
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
        print(f"*** Pages numbers: {[p for p in pages]} ***\nText snippet: {text_entity[:100]}")
        print("type: " + entity.type_)
        start_page= pages[0]
        end_page = pages[len(pages)-1]
        print(start_page)
        print(end_page)
        
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(uri.hostname)
        blob = bucket.get_blob(uri.path[1:])

        inputpdf=  PdfFileReader(
            io.BytesIO(blob.download_as_bytes())
            ,strict=False) 
        
        split_pdf(inputpdf, start_page, end_page, uri,gcs_output_uri, gcs_output_uri_prefix + "/" + entity.type_)
         

#Synchronous processing
def process(
    project_id: str, location: str,  gcs_input_uri: str, gcs_output_uri : str, gcs_output_uri_prefix, OUTPUT_JSON_URI,  timeout: int = 300,
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
        pages_split("text", "document", uri, gcs_output_uri, gcs_output_uri_prefix)


def convertToImage(f, outputBucket):
    from pdf2image import convert_from_path

	# Set poppler path
    poppler_path = "/var/task/lib/poppler-utils-0.26/usr/bin"
    
    images = convert_from_path( f,dpi=150,poppler_path=poppler_path)
    
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(outputBucket)

    for i in range(len(images)):
        file = "/tmp/images/"+f[4:].replace("/","").replace(".pdf","").replace(":","") + '-p'+ str(i) +'.jpg'

          # Save pages as images in the pdf
        images[i].save(file, 'JPEG')
        bucket.upload_from_filename(file)

