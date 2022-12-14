

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


resource "google_storage_bucket" "results_json_dev_kyc" {
  name                        = format("results_json_%s", var.env)
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
resource "google_cloudfunctions_function" "gcf_input" {
  name        = format("gcf_input_%s", var.env)
  description = "gcf_input process input pdf"
  region      = var.region
  project     = var.project

  runtime          = "python39"
  entry_point      = "main_run"
  timeout          = 300
  max_instances    = 3
  ingress_settings = "ALLOW_INTERNAL_AND_GCLB"

  available_memory_mb   = 1024
  source_archive_bucket = google_storage_bucket.bucket_source_archives.name
  source_archive_object = google_storage_bucket_object.gcf_input_source.name

  service_account_email = google_service_account.sa.email #var.service_account_email  

  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = google_storage_bucket.input_doc.name
    failure_policy {
      retry = false
    }
  }
  labels = {
    "env" : var.env
  }

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

    GEOCODE_REQUEST_TOPICNAME = google_pubsub_topic.pubsub_geocode_topic.name ,
    KG_REQUEST_TOPICNAME      = google_pubsub_topic.pubsub_getkg_topic.name  ,

    gcs_output_uri = google_storage_bucket.output_doc.url ,
    gcs_archive_bucket_name : google_storage_bucket.output_doc.name
  }
}

  //function dedicated to process results from HITL 
resource "google_cloudfunctions_function" "gcf_input_hitl" {
  name        = format("gcf_hitl_results_%s", var.env)
  description = "gcf_input process results from HITL review"
  region      = var.region
  project     = var.project

  runtime          = "python39"
  entry_point      = "main_run"
  timeout          = 300
  max_instances    = 10
  ingress_settings = "ALLOW_INTERNAL_AND_GCLB"

  available_memory_mb   = 256
  source_archive_bucket = google_storage_bucket.bucket_source_archives.name
  source_archive_object = google_storage_bucket_object.gcf_input_source.name

  service_account_email = google_service_account.sa.email #var.service_account_email  

  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = google_storage_bucket.results_json_dev_kyc.name
    failure_policy {
      retry = false
    }
  }

  labels = {
    "env" : var.env
  }

  environment_variables = {
    BQ_DATASET_NAME = google_bigquery_dataset.docai.dataset_id,

    PARSER_LOCATION       = var.PROCESSOR_CNI_LOCATION,
    PROCESSOR_ID          = "",
    TIMEOUT               = 300,
    GCS_OUTPUT_URI_PREFIX = "processed",

    GEOCODE_REQUEST_TOPICNAME = google_pubsub_topic.pubsub_geocode_topic.name ,
    KG_REQUEST_TOPICNAME      = google_pubsub_topic.pubsub_getkg_topic.name  ,

    gcs_output_uri = google_storage_bucket.output_doc.url ,
    gcs_archive_bucket_name : google_storage_bucket.output_doc.name
  }
}


// Pub/sub - pubsub_geocode_topic

resource "google_pubsub_topic" "pubsub_geocode_topic" {
  name = format("pubsub_geocode_topic_%s", var.env)
  project     = var.project

  message_storage_policy {
    allowed_persistence_regions = [
      var.region,
    ]
  }
}






////////////////////////////
// Pub/sub - pubsub_getkg_topic

resource "google_pubsub_topic" "pubsub_getkg_topic" {
  name = format("pubsub_getkg_topic_%s", var.env)
  project     = var.project

  message_storage_policy {
    allowed_persistence_regions = [
      var.region,
    ]
  }
}
