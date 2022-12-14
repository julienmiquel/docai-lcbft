import os
import re
from google.cloud import storage

from function_variables import FunctionVariables

# Reading environment variables
var = FunctionVariables()


storage_client = storage.Client()


def create_tmp_folders():
    """
    Function that creates tmp local folders where to store intermediate files
    """
    #print("create tmp folder")
    for folder in (var.PDF_FOLDER, var.JPG_FOLDER, var.ERROR_FOLDER):
        if not os.path.exists(folder):
            os.makedirs(folder)
            print(f"make dir: {folder}")


def write_bytes(raw_bytes, image_path, mime_type):
    blob = __get_blob(image_path)
    blob.upload_from_string(raw_bytes.image.content, content_type=mime_type)
#    with  storage_client.open(image_path,'w',content_type=mime_type) as gcs_file:
#        gcs_file.write(raw_bytes.image.content.encode('utf-8'))


def send_file(path, filepath, metadata=None):
    print(f"Uploading {filepath} to {path}")
    blob = __get_blob(path)
    blob.upload_from_filename(filepath)
    if metadata != None:
        blob.metadata = metadata
        blob.patch()
    return blob


def __get_bucket(path):
    bucket_name, _ = __parse_url(path)
    return storage_client.get_bucket(bucket_name)


def get_data(path: str):
    print("Reading data from " + path)

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
    print(f"Reading data from {path} to {filepath}", path, filepath)

    blob = __get_blob(path)
    return blob.download_to_filename(filepath)


def get_raw_bytes(uri):
    bucket = storage_client.get_bucket(uri.hostname)
    blob = bucket.get_blob(uri.path[1:])
    image_content = blob.download_as_bytes()
    return image_content, blob


def backupAndDeleteInput(event, gcs_archive_bucket_name=var.gcs_archive_bucket_name):
    source_bucket = storage_client.bucket(event['bucket'])
    source_blob = source_bucket.blob(event['name'])
    destination_bucket = storage_client.bucket(gcs_archive_bucket_name)

    print(f"backup input file to: {destination_bucket.path}{event['name']}")
    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, event['name'])
    
    # delete from the input folder
    print(f"delete input file to: {source_blob.path} {event['name']}")
    source_blob.delete()
