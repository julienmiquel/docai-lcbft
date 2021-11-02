

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
resource "google_storage_bucket" "gcs_input_doc" {
  name                        = format("gcs_input_doc_%s", var.env)
  location                    = var.location
  force_destroy               = true
  project                     = var.project
  uniform_bucket_level_access = true
  labels = {
    "env" : var.env
  }
}


resource "google_storage_bucket" "gcs_output_doc" {
  name                        = format("gcs_output_doc_%s", var.env)
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


//function 
resource "google_cloudfunctions_function" "gcf_input" {
  name        = format("gcf_input_%s", var.env)
  description = "gcf_input process input pdf"
  region      = var.region
  project     = var.project

  runtime          = "python39"
  entry_point      = "main_run"
  timeout          = 300
  max_instances    = 3
  ingress_settings = "ALLOW_INTERNAL_ONLY"

  available_memory_mb   = 512
  source_archive_bucket = google_storage_bucket.bucket_source_archives.name
  source_archive_object = google_storage_bucket_object.gcf_input_source.name

  service_account_email = google_service_account.sa.email #var.service_account_email  

  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = google_storage_bucket.gcs_input_doc.name
    failure_policy {
      retry = false
    }
  }

  labels = {
    "env" : var.env
  }

  environment_variables = {
    BQ_DATASET_NAME = google_bigquery_dataset.dataset_results_docai.dataset_id,

    PARSER_LOCATION       = var.PROCESSOR_CNI_LOCATION,
    PROCESSOR_ID          = var.PROCESSOR_CNI_ID,
    TIMEOUT               = 300,
    GCS_OUTPUT_URI_PREFIX = "processed",

    GEOCODE_REQUEST_TOPICNAME = google_pubsub_topic.pubsub_geocode_topic.name ,
    KG_REQUEST_TOPICNAME      = google_pubsub_topic.pubsub_getkg_topic.name  ,

    gcs_output_uri = google_storage_bucket.gcs_output_doc.url ,
    gcs_archive_bucket_name : google_storage_bucket.gcs_output_doc.name
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




// Cloud function geocode

//Source
data "archive_file" "gcf_geocode_addresses_source" {
  type        = "zip"
  source_dir  = "./functions/gcf_geocode_addresses"
  output_path = "../tmp/gcf_geocode_addresses.zip"
}

resource "google_storage_bucket_object" "gcf_geocode_addresses_source" {
  name   = "gcf_geocode_addresses.zip"
  bucket = google_storage_bucket.bucket_source_archives.name
  source = data.archive_file.gcf_geocode_addresses_source.output_path
}


//function 
resource "google_cloudfunctions_function" "gcf_geocode_addresses" {
  name        = format("gcf_geocode_addresses_%s", var.env)
  description = "gcf_geocode_addresses process input pdf"
  region      = var.region
  project     = var.project
  
  runtime          = "python39"
  entry_point      = "main_run"
  timeout          = 300
  max_instances    = 3
  ingress_settings = "ALLOW_INTERNAL_ONLY"


  available_memory_mb   = 256
  source_archive_bucket = google_storage_bucket.bucket_source_archives.name
  source_archive_object = google_storage_bucket_object.gcf_geocode_addresses_source.name

  service_account_email = google_service_account.sa.email #var.service_account_email  

   event_trigger {
      event_type= "google.pubsub.topic.publish"
      resource= google_pubsub_topic.pubsub_geocode_topic.name 
   
    failure_policy {
      retry = false
    }
  }

  labels = {
    "env" : var.env
  }

  environment_variables = {
    BQ_DATASET_NAME = google_bigquery_dataset.dataset_results_docai.dataset_id,
    GEOCODE_REQUEST_TOPICNAME = google_pubsub_topic.pubsub_geocode_topic.name ,
    API_KEY = var.API_KEY_KG_GEOCODES
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

// Cloud function geocode

//Source
data "archive_file" "gcf_getkg_source" {
  type        = "zip"
  source_dir  = "./functions/gcf_getkg_data"
  output_path = "../tmp/gcf_getkg_data.zip"
}

resource "google_storage_bucket_object" "gcf_getkg_data_source" {
  name   = "gcf_getkg_data.zip"
  bucket = google_storage_bucket.bucket_source_archives.name
  source = data.archive_file.gcf_getkg_source.output_path
}


//function 
resource "google_cloudfunctions_function" "gcf_getkg_data" {
  name        = format("gcf_getkg_data%s", var.env)
  description = "cloud function to handle Knowledge Graph Data"
  region      = var.region
  project     = var.project

  runtime          = "python39"
  entry_point      = "main_run"
  timeout          = 300
  max_instances    = 3
  ingress_settings = "ALLOW_INTERNAL_ONLY"

  available_memory_mb   = 256
  source_archive_bucket = google_storage_bucket.bucket_source_archives.name
  source_archive_object = google_storage_bucket_object.gcf_getkg_data_source.name

  service_account_email = google_service_account.sa.email #var.service_account_email  

   event_trigger {
      event_type= "google.pubsub.topic.publish"
      resource= google_pubsub_topic.pubsub_getkg_topic.name 
   
    failure_policy {
      retry = false
    }
  }

  labels = {
    "env" : var.env
  }

  environment_variables = {
    BQ_DATASET_NAME = google_bigquery_dataset.dataset_results_docai.dataset_id,
    GEOCODE_REQUEST_TOPICNAME = google_pubsub_topic.pubsub_getkg_topic.name ,
    API_KEY = var.API_KEY_KG_GEOCODES
  }
}