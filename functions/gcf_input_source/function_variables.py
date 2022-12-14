import os
import tempfile


class FunctionVariables:
    """[summary]
    All parameters variables of the function
    """    
    def get_env():
        print(os.environ)
        if 'GCP_PROJECT' in os.environ:
            return os.environ['GCP_PROJECT']
        import google.auth
        _, project_id = google.auth.default()
        print(project_id)
        return project_id
    
    def getToken():
        import google.auth
        import google.auth.transport.requests
        from google.oauth2 import service_account

        credentials = google.auth.default()[0]
        auth_req = google.auth.transport.requests.Request()
        credentials.refresh(auth_req)
        return "Bearer " + credentials.token

    genSignedUrl = False 

    gcs_output_uri_prefix = os.environ.get('GCS_OUTPUT_URI_PREFIX')
    
    gcs_output_uri = os.environ.get('gcs_output_uri')
    gcs_archive_bucket_name = os.environ.get('gcs_archive_bucket_name')

    destination_uri = f"{gcs_output_uri}/{gcs_output_uri_prefix}/"

    dataset_name = os.environ.get(
        'BQ_DATASET_NAME', 'ERROR: Specified environment variable is not set.')

    table_name = 'doc_ai_extracted_entities'

    ROOT_FOLDER = os.path.abspath(tempfile.gettempdir())
    TMP_FOLDER = os.path.join(ROOT_FOLDER, "tmp")
    PDF_FOLDER = os.path.join(TMP_FOLDER, "pdf")
    JPG_FOLDER = os.path.join(TMP_FOLDER, "jpg")
    ERROR_FOLDER = os.path.join(TMP_FOLDER, "error")


    project_id = get_env()

    #static parameters of the function
    CheckBusinessRules = False # os.environ.get('CheckBusinessRules')
    BackupAndDeleteInput = True # os.environ.get('BackupAndDeleteInput')
    SendKGRequest = False

    PROCESSOR_fr_driver_license          = os.environ.get('PROCESSOR_fr_driver_license')
    PROCESSOR_fr_national_id             = os.environ.get('PROCESSOR_fr_national_id')
    PROCESSOR_fr_passport                = os.environ.get('PROCESSOR_fr_passport')
    PROCESSOR_id_proofing                = os.environ.get('PROCESSOR_id_proofing')
    PROCESSOR_us_passport                = os.environ.get('PROCESSOR_us_passport')
    PROCESSOR_us_driver_license          = os.environ.get('PROCESSOR_us_driver_license')


    def FunctionVariables(self):
        project_id = self.get_env()
        gcs_output_uri_prefix = os.environ.get('GCS_OUTPUT_URI_PREFIX')