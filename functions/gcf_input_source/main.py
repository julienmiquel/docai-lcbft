import time
from urllib.parse import urlparse

import bq
import docai
import gcs
from function_variables import FunctionVariables

var = FunctionVariables()


def main_run(event, context):
    """[summary]
        Standard cloud function entry 
    Args:
        event ([type]): event with parameters of the cloud function
        context ([type]): context of the cloud function

    Returns:
        [type]: return code of the function
    """

    # full url address
    gcs_input_uri = 'gs://' + event['bucket'] + '/' + event['name']

    if "contentType" not in event:
        print(event)
        event['contentType'] = "None"

    print('Printing the contentType: ' +
          event['contentType'] + ' input:' + gcs_input_uri)

    uri = urlparse(gcs_input_uri)
    image_content, blob = gcs.get_raw_bytes(uri)

    # start timer
    t0 = time.time()

    # if the type of the input file is an json
    # we will parse the results and store it into BQ
    if (str(event['name']).endswith(".json") or event['contentType'] == 'application/json'):
        doc_type = "hitl"
        name = "hitl"

        document = docai.docai_extract_doc_from_json(image_content)

        entities_extracted_dict = docai.parse_docairesult(
            event, gcs_input_uri, uri, t0, doc_type, name, document, None)
        # Write the entities to BQ
        bq.write_to_bq(entities_extracted_dict)

    # if the content is an image, the content will be send to docAI in synchronous way
    # Store the result in BQ
    elif (event['contentType'] in ['image/gif', 'image/tiff', 'image/jpeg',  'application/pdf']):

        doc_type, name, processor_location = docai.get_doctype(
            gcs_input_uri, image_content)

        key = None
        if blob.metadata:
            print(
                f"input_filename: {gcs_input_uri}  metadata: {blob.metadata}")
            if "key" in blob.metadata:
                key = blob.metadata["key"]

        entities_extracted_dict = docai.process_raw_bytes(
            event, gcs_input_uri, uri, image_content, t0, doc_type, name, processor_location, key)

        fraud_detection(event, gcs_input_uri, uri, image_content,
                        t0, doc_type, entities_extracted_dict)

        # Write the entities to BQ
        bq.write_to_bq(entities_extracted_dict)

    else:
        print('Cannot parse the file type')


def fraud_detection(event, gcs_input_uri, uri, image_content, t0, doc_type, entities_extracted_dict):
    if doc_type in ["fr_national_id", "us_passport", "fr_passport", "fr_driver_license"]:
        print(f"Fraud detection for {gcs_input_uri}")
        try:
            doc_type, name, processor_location = docai.get_doctype(
                "_identity_fraud_detector_")
            entities_id_extracted_dict_fraud = docai.process_raw_bytes(
                event, gcs_input_uri, uri, image_content, t0, doc_type, name, processor_location)
            entities_extracted_dict["fraud_signals_suspicious_words"] = entities_id_extracted_dict_fraud["fraud_signals_suspicious_words"]
            entities_extracted_dict["fraud_signals_is_identity_document"] = entities_id_extracted_dict_fraud["fraud_signals_is_identity_document"]
            entities_extracted_dict["fraud_signals_image_manipulation"] = entities_id_extracted_dict_fraud["fraud_signals_image_manipulation"]
            entities_extracted_dict["fraud_evidence_inconclusive_suspicious_word"] = entities_id_extracted_dict_fraud["evidence_inconclusive_suspicious_word"]
            entities_extracted_dict["fraud_signals_online_duplicate"] = entities_id_extracted_dict_fraud["fraud_signals_online_duplicate"]
            entities_extracted_dict["fraud_evidence_hostname"] = entities_id_extracted_dict_fraud["evidence_hostname"]
            entities_extracted_dict["fraud_evidence_thumbnail_url"] = entities_id_extracted_dict_fraud["evidence_thumbnail_url"]
        except Exception as err:
            print(f"ERROR in Fraud detection for {gcs_input_uri}")
            print(err)


if __name__ == '__main__':
    print('Calling from main')
    testEvent = {"bucket": var.project_id+"",
                 "contentType": "application/pdf", "name": "CI-1.pdf"}
    testContext = 'test'
    main_run(testEvent, testContext)
