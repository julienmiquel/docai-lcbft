import os
import re
import tempfile
from google.cloud import storage
from function_variables import FunctionVariables

# Reading environment variables
var = FunctionVariables()


storage_client = storage.Client()






def create_tmp_folders():
    """
    Function that creates tmp local folders where to store intermediate files
    """
    print("create tmp folder")
    for folder in (var.PDF_FOLDER, var.JPG_FOLDER, var.ERROR_FOLDER):
        if not os.path.exists(folder):
            os.makedirs(folder)
            print(f"make dir: {folder}")


def write_bytes(p, image_path, mime_type):
    with  storage.open(image_path,'w',content_type=mime_type) as gcs_file:
        gcs_file.write(p.image.content.encode('utf-8'))

def send_file(path, filepath):
    print(f"Uploading {filepath} to {path}" )
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


def get_raw_bytes(uri):
    bucket = storage_client.get_bucket(uri.hostname)
    blob = bucket.get_blob(uri.path[1:])
    image_content = blob.download_as_bytes()
    return image_content, blob


def backupAndDeleteInput(event, gcs_archive_bucket_name = var.gcs_archive_bucket_name):
    source_bucket = storage_client.bucket(event['bucket'])
    source_blob = source_bucket.blob(event['name'])
    destination_bucket = storage_client.bucket(gcs_archive_bucket_name)

    print(f"backup input file to: {destination_bucket.path}{event['name']}")
    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, event['name'])
    # delete from the input folder
    print(f"delete input file to: {source_blob.path} {event['name']}")
    source_blob.delete()





def generate_signed_url(service_account_file, bucket_name, object_name,
                        subresource=None, expiration=604800, http_method='GET',
                        query_parameters=None, headers=None):

    
    import binascii
    import collections
    import datetime
    import hashlib
    import sys

    # pip install six
    import six
    # pip install google-auth
    from google.oauth2 import service_account
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


def extract_signed_url_from_bytes(event, page_number , raw_bytes, mime_type):
    destination_bucket = storage_client.bucket(
                        var.gcs_archive_bucket_name)
    image_name = event['name'] + \
                        "-page-" + page_number + ".jpg"
    image_path = os.path.join(
                        var.gcs_archive_bucket_name, image_name)
    write_bytes(raw_bytes, image_path, mime_type)

    signed_url = generate_signed_url(
                        "google.com_ml-baguette-demos-f1b859baa944.json", var.gcs_archive_bucket_name, image_name)
    print("image signed url")
    print(signed_url)
    return signed_url