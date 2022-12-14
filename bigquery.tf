
// Big query


resource "google_bigquery_dataset" "docai" {
  dataset_id    = format("bq_results_%s", var.env)
  friendly_name = "docai results"
  description   = "Store Document AI results"
  location      = var.location
  project       = var.project

  labels = {
    env : var.env
  }
}



resource "google_bigquery_table" "doc_ai_extracted_entities" {
  dataset_id          = google_bigquery_dataset.docai.dataset_id
  table_id            = "doc_ai_extracted_entities"
  project             = var.project
  deletion_protection = false

  labels = {
    env : var.env
  }

  schema = file("./functions/gcf_input_source/bq_docai_id.json") 
}

