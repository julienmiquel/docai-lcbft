variable "region" {
  default = "europe-west1"
}

variable "API_KEY_KG_GEOCODES" {
  default = "AIzaSyCWo8pgbPeYZTfEvigJMP2zGUghmbNVuLg"
}
variable "project" {
  default = "google.com:finance-practice"
}

variable "location" {
  default = "EU"
}

variable "env" {
  default = "dev_lcbft_1"
}

variable "PROCESSOR_CNI_ID" {
  default = "7b9066f18d0c7366"
}

variable "PROCESSOR_CNI_LOCATION" {
  default = "eu"
}

variable "service_account_name" {
  default = "lcbft-sa-3"
}


variable "deletion_protection" {
  default = false
}

provider "google" {
  region = var.region
}

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


// Big query
resource "google_bigquery_dataset" "dataset_results_docai" {
  dataset_id    = format("bq_results_%s", var.env)
  friendly_name = "Invoices results"
  description   = "Store Document AI results"
  location      = var.location
  project       = var.project

  labels = {
    env : var.env
  }
}





resource "google_bigquery_table" "results_table" {
  dataset_id          = google_bigquery_dataset.dataset_results_docai.dataset_id
  table_id            = "results"
  project             = var.project
  deletion_protection = false

  labels = {
    env : var.env
  }

  schema = <<EOF
[
  {
    "name": "doc_type",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "type of the document"
  },
  {
    "name": "key",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "value",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "type",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  },
  {
    "name": "confidence",
    "type": "FLOAT",
    "mode": "NULLABLE",
    "description": "confidence"
  },
  {
    "name": "file",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": ""
  }
]
EOF

}


resource "google_bigquery_table" "knowledge_graph_details" {
  dataset_id          = google_bigquery_dataset.dataset_results_docai.dataset_id
  table_id            = "knowledge_graph_details"
  project             = var.project
  deletion_protection = false

  labels = {
    env : var.env
  }

  schema = <<EOF
[
  {
    "mode": "NULLABLE",
    "name": "input_file_name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "entity_type",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "entity_text",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "url",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "description",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "result_score",
    "type": "STRING"
  }
]

EOF

}


resource "google_bigquery_table" "geocodes_details" {
  dataset_id          = google_bigquery_dataset.dataset_results_docai.dataset_id
  table_id            = "geocodes_details"
  project             = var.project
  deletion_protection = false

  labels = {
    env : var.env
  }

  schema = <<EOF
[
  {
    "mode": "NULLABLE",
    "name": "input_file_name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "entity_type",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "entity_text",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "place_id",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "formatted_address",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "lat",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "lng",
    "type": "STRING"
  }
]

EOF

}



resource "google_bigquery_table" "doc_ai_extracted_entities" {
  dataset_id          = google_bigquery_dataset.dataset_results_docai.dataset_id
  table_id            = "doc_ai_extracted_entities"
  project             = var.project
  deletion_protection = false

  labels = {
    env : var.env
  }

  schema = <<EOF
[
  {
    "mode": "NULLABLE",
    "name": "input_file_name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "family_name",
    "type": "STRING",
    "description": "CI"
  },
    {
    "mode": "NULLABLE",
    "name": "date_of_birth",
    "type": "STRING",
    "description": "CI"
  },
    {
    "mode": "NULLABLE",
    "name": "document_id",
    "type": "STRING",
    "description": "CI"
  },
    {
    "mode": "NULLABLE",
    "name": "given_names",
    "type": "STRING",
    "description": "CI"
  },
  {
    "mode": "NULLABLE",
    "name": "address",
    "type": "STRING",
    "description": "CI verso"
  },
    {
    "mode": "NULLABLE",
    "name": "expiration_date",
    "type": "STRING",
    "description": "CI verso"
  },
    {
    "mode": "NULLABLE",
    "name": "issue_date",
    "type": "STRING",
    "description": "CI verso"
  },  

  


  {
    "mode": "NULLABLE",
    "name": "carrier",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "currency",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "customer_tax_id",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "delivery_date",
    "type": "DATE"
  },
  {
    "mode": "NULLABLE",
    "name": "due_date",
    "type": "DATE"
  },
  {
    "mode": "NULLABLE",
    "name": "invoice_date",
    "type": "DATE"
  },
  {
    "mode": "NULLABLE",
    "name": "invoice_id",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "net_amount",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "payment_terms",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "purchase_order",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "receiver_address",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "receiver_name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "remit_to_address",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "remit_to_name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "ship_to_address",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "ship_to_name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "supplier_address",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "supplier_name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "supplier_tax_id",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "total_amount",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "total_tax_amount",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "line_item",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "supplier_email",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "receiver_email",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "freight_amount",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "vat_tax_amount",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "supplier_phone",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "supplier_website",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "currency_exchange_rate",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "receiver_phone",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "receiver_tax_id",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "ship_from_address",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "ship_from_name",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "supplier_iban",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "supplier_registration",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "vat_tax_rate",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "receipt_date",
    "type": "DATE"
  },
  {
    "mode": "NULLABLE",
    "name": "purchase_time",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "supplier_city",
    "type": "STRING"
  }
]
EOF
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

  available_memory_mb   = 256
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

    gcs_output_uri = "gs://jm-docai-kyc-output-invoices",
    gcs_archive_bucket_name : "jm-docai-kyc-archived-invoices"
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