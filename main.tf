variable "region" {
  default = "europe-west1"
}

variable "project" {
  default = "google.com:ml-baguette-demos"
}

variable "location" {
  default = "EU"
}

variable "env" {
  default = "dev_kyc_1"
}

variable "service_account_email" {
  default = "my-documentai-sa@ml-baguette-demos.google.com.iam.gserviceaccount.com"
}

variable "service_account_name" {
  default = "lcbft-sa"
}


variable "deletion_protection" {
  default = false
}

provider "google" {
  region = var.region
}

// Big query
resource "google_bigquery_dataset" "dataset_results_docai" {
  dataset_id                  = format("bq_results_%s", var.env)
  friendly_name               = "Invoices results"
  description                 = "Store Document AI results"
  location                    = var.location
  project                     = var.project

  labels = {
        env : var.env
  }
}

resource "google_service_account" "sa" {
  account_id   = var.service_account_name
  display_name = "A service account used by lcbft pipeline"
  project                     = var.project

}


resource "google_project_iam_member" "project" {
  project                     = var.project
  role    = "roles/editor"
  member  = "serviceAccount:${google_service_account.sa.email}"

}



resource "google_bigquery_table" "results_table" {
  dataset_id = google_bigquery_dataset.dataset_results_docai.dataset_id
  table_id   = "results"
  project                     = var.project

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


// Storage staging
resource "google_storage_bucket" "gcs_input_doc" {
  name          = format("gcs_input_doc_%s", var.env)
  location      = var.location
  force_destroy = true
  project                     = var.project

  uniform_bucket_level_access = true

    labels = {
        "env" : var.env
    }

}


// Cloud Storage function source archive 
resource "google_storage_bucket" "bucket_source_archives" {
    name          = format("bucket_source_archives_%s", var.env)
    location      = var.location
    force_destroy = true
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
    name                  = format("gcf_input_%s", var.env)
    description           = "gcf_input process input pdf"
    region                = var.region
    project               = var.project

    runtime               = "python39"
    entry_point           = "main_run"
    timeout = 60
    max_instances = 3
    ingress_settings = "ALLOW_INTERNAL_ONLY"
    
    available_memory_mb   = 256
    source_archive_bucket = google_storage_bucket.bucket_source_archives.name 
    source_archive_object = google_storage_bucket_object.gcf_input_source.name

    service_account_email = google_service_account.sa.email #var.service_account_email  

    event_trigger  {
      event_type    = "google.storage.object.finalize"
      resource=  google_storage_bucket.gcs_input_doc.name 
        failure_policy {
            retry = false
        }
    }

    labels = {
        "env" : var.env
    }

  environment_variables = {
    BQ_TABLENAME = format("%s.%s",  google_bigquery_dataset.dataset_results_docai.dataset_id , google_bigquery_table.results_table.table_id )

  }
}


