

//Service Account (right to call doc AI)
resource "google_service_account" "sa" {
  account_id   = var.service_account_name
  display_name = "A service account used by lcbft pipeline"
  project      = var.project

}

resource "google_project_iam_member" "project" {
  project = var.project
  role    = "roles/editor"
  member  = "serviceAccount:${google_service_account.sa.email}"
}

resource "google_project_iam_member" "serviceaccounttokencreator" {
  project = var.project
  role    = "roles/iam.serviceAccountTokenCreator"
  member  = "serviceAccount:${google_service_account.sa.email}"
}

resource "google_project_iam_member" "pubsub-publisher" {
  project = var.project
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.sa.email}"
}


resource "google_project_iam_member" "eventreceiver" {
  project = var.project
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${google_service_account.sa.email}"
}

resource "google_project_iam_member" "eventadmin" {
  project = var.project
  role    = "roles/eventarc.admin"
  member  = "serviceAccount:${google_service_account.sa.email}"
}


data "google_storage_project_service_account" "gcs_account" {
    project = var.project
}

resource "google_project_iam_member" "gcs_account_pubsub-publisher" {
  project = var.project
  role    = "roles/pubsub.publisher"
  member = "serviceAccount:${data.google_storage_project_service_account.gcs_account.email_address}"
}


// Storage staging
resource "google_storage_bucket" "input_doc" {
  name                        = format("input_doc_%s", var.env)
  location                    = var.location
  force_destroy               = true
  project                     = var.project
  uniform_bucket_level_access = true
  labels = {
    "env" : var.env
  }
}



resource "google_storage_bucket" "output_doc" {
  name                        = format("output_doc_%s", var.env)
  location                    = var.location
  force_destroy               = true
  project                     = var.project
  uniform_bucket_level_access = true
  labels = {
    "env" : var.env
  }
}




// Cloud Storage function source archive 
resource "google_storage_bucket" "bucket_source_archives" {
  name                        = format("bucket_source_archives_%s", var.env)
  location                    = var.location
  force_destroy               = true
  project                     = var.project
  uniform_bucket_level_access = true
  labels = {
    "env" : var.env
  }
}


// Google Cloud functions

// gcf_input process pdf from input folder

//Source
data "archive_file" "gcf_input_source" {
  type        = "zip"
  source_dir  = "./functions/gcf_input_source"
  output_path = "../tmp/gcf_input_source.zip"
}

resource "google_storage_bucket_object" "gcf_input_source" {
  name   = "gcf_input_source.zip"
  bucket = google_storage_bucket.bucket_source_archives.name
  source = data.archive_file.gcf_input_source.output_path
}


//function to process input documents
resource "google_cloudfunctions2_function" "gcf_input" {
  name        = format("docai-input-%s", var.env)
  description = "gcf input process input pdf"
  location      = var.region
  project     = var.project

  build_config {
    runtime          = "python39"
    entry_point      = "main_run"

    source {
      storage_source {
        bucket = google_storage_bucket.bucket_source_archives.name
        object = google_storage_bucket_object.gcf_input_source.name
      }
    }
  }

  service_config {
    max_instance_count  = 3
    available_memory    = "1024M"
    timeout_seconds     = 300
    ingress_settings = "ALLOW_INTERNAL_AND_GCLB"
    service_account_email = google_service_account.sa.email 

    environment_variables = {
        BQ_DATASET_NAME = google_bigquery_dataset.docai.dataset_id,

        PARSER_LOCATION       = var.PROCESSOR_CNI_LOCATION,
        PROCESSOR_fr_national_id             = google_document_ai_processor.fr_national.id,
        PROCESSOR_fr_driver_license          = google_document_ai_processor.fr_driver_license.id,
        PROCESSOR_fr_passport                = google_document_ai_processor.fr_passport.id,
        PROCESSOR_id_proofing                = google_document_ai_processor.id_proofing.id,
        PROCESSOR_us_passport                = google_document_ai_processor.us_passport.id,
        PROCESSOR_us_driver_license          = google_document_ai_processor.us_driver_license.id,

        TIMEOUT               = 300,
        GCS_OUTPUT_URI_PREFIX = "processed",


        gcs_output_uri = google_storage_bucket.output_doc.url ,
        gcs_archive_bucket_name : google_storage_bucket.output_doc.name
      }
  }
    
  event_trigger {
      trigger_region =  var.PROCESSOR_CNI_LOCATION # The trigger must be in the same location as the bucket
      event_type = "google.cloud.storage.object.v1.finalized"
      retry_policy = "RETRY_POLICY_DO_NOT_RETRY"
      service_account_email = google_service_account.sa.email 
      event_filters {
        attribute = "bucket"
        value = google_storage_bucket.input_doc.name
      }
    }

}
