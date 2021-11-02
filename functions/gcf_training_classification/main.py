import os
from dateutil import parser
from datetime import datetime

from google.cloud import storage
from google.cloud import aiplatform
from google.cloud.aiplatform import gapic as aip

def get_env():
    print(os.environ)
    if 'GCP_PROJECT' in os.environ:
        return os.environ['GCP_PROJECT']
    import google.auth
    _, project_id = google.auth.default()
    print(project_id)
    return project_id


# Reading environment variables
project_id = get_env()

# Setting variables
gcs_output_uri = os.environ.get('gcs_output_uri')
location = os.environ.get('LOCATION')
dataset_id = os.environ.get('dataset_id')

gcs_archive_bucket_name = os.environ.get('gcs_archive_bucket_name')
storage_client = storage.Client()

def main_run(event, context):
    gcs_input_uri = 'gs://' + event['bucket'] + '/' + event['name']
    print('Printing the contentType: ' + event['contentType'] + ' input:' + gcs_input_uri)

    if(".csv" in gcs_input_uri.lower()):
        # Process input file       
        dataset = create_and_import_dataset(project_id,
            location,
            dataset_id,
            gcs_input_uri,
            import_schema_uri=aiplatform.schema.dataset.ioformat.image.single_label_classification)
        
        moveAndDelete = True
        if(moveAndDelete == True):
            # Copy input file to archive bucket
            source_bucket = storage_client.bucket(event['bucket'])
            source_blob = source_bucket.blob(event['name'])
            destination_bucket = storage_client.bucket(gcs_archive_bucket_name)

            print(f"backup input file to: {destination_bucket}{event['name']}")
            blob_copy = source_bucket.copy_blob(
                source_blob, destination_bucket, event['name'])
            # delete from the input folder
            print(f"delete input file to: {source_blob} {event['name']}")
            source_blob.delete()
    else:
        print('Cannot parse the file type')



def create_and_import_dataset(
    project: str,
    location: str,
    display_name: str,
    src_uris: str,
    import_schema_uri=aiplatform.schema.dataset.ioformat.image.single_label_classification,
    sync: bool = True,
):

    aiplatform.init(project=project, location=location)

    ds = aiplatform.ImageDataset.create(
        display_name=display_name,
        gcs_source=src_uris,
        import_schema_uri=import_schema_uri,
        sync=sync,
    )

    #ds.wait()

    print(ds.display_name)
    print(ds.resource_name)
    return ds