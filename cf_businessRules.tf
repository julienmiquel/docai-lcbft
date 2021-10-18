// Google Cloud functions

// Pub/sub - pubsub_checkBusinessRules_topic

resource "google_pubsub_topic" "pubsub_checkBusinessRules_topic" {
  name = format("pubsub_checkBusinessRules_topic_%s", var.env)
  project     = var.project

  message_storage_policy {
    allowed_persistence_regions = [
      var.region,
    ]
  }
}


//Source
data "archive_file" "gcf_checkBusinessRules_source" {
  type        = "zip"
  source_dir  = "./functions/gcf_checkBusinessRules"
  output_path = "../tmp/gcf_checkBusinessRules.zip"
}

resource "google_storage_bucket_object" "gcf_checkBusinessRules_source" {
  name   = "gcf_checkBusinessRules.zip"
  bucket = google_storage_bucket.bucket_source_archives.name
  source = data.archive_file.gcf_checkBusinessRules_source.output_path
}


//function 
resource "google_cloudfunctions_function" "gcf_checkBusinessRules" {
  name        = format("gcf_checkBusinessRules_%s", var.env)
  description = "gcf_checkBusinessRules process input pdf"
  region      = var.region
  project     = var.project
  
  runtime          = "python39"
  entry_point      = "main_run"
  timeout          = 300
  max_instances    = 3
  ingress_settings = "ALLOW_INTERNAL_ONLY"


  available_memory_mb   = 256
  source_archive_bucket = google_storage_bucket.bucket_source_archives.name
  source_archive_object = google_storage_bucket_object.gcf_checkBusinessRules_source.name

  service_account_email = google_service_account.sa.email #var.service_account_email  

   event_trigger {
      event_type= "google.pubsub.topic.publish"
      resource= google_pubsub_topic.pubsub_checkBusinessRules_topic.name 
   
    failure_policy {
      retry = false
    }
  }

  labels = {
    "env" : var.env
  }

  environment_variables = {
    BQ_DATASET_NAME = google_bigquery_dataset.dataset_results_docai.dataset_id,
    checkBusinessRules_REQUEST_TOPICNAME = google_pubsub_topic.pubsub_checkBusinessRules_topic.name 
  }
}

