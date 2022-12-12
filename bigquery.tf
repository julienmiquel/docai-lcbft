

// Big query
resource "google_bigquery_dataset" "dataset_docai_looker" {
  dataset_id    = format("bq_looker_%s", var.env)
  friendly_name = "Looker Dataset"
  description   = "Store Looker config"
  location      = var.location
  project       = var.project

  labels = {
    env : var.env
  }
}

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


/*
resource "google_bigquery_table" "business_rules_result" {
  dataset_id          = google_bigquery_dataset.dataset_results_docai.dataset_id
  table_id            = "business_rules_result"
  project             = var.project
  deletion_protection = false

  labels = {
    env : var.env
  }
  schema = <<EOF
[
  {
    "name": "name",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "name of business rule whose have been matched"
  },  
  {
    "mode": "NULLABLE",
    "name": "input_file_name",
    "type": "STRING"
  }
]
EOF
}


resource "google_bigquery_table" "business_rules" {
  dataset_id          = google_bigquery_dataset.dataset_results_docai.dataset_id
  table_id            = "business_rules"
  project             = var.project
  deletion_protection = false

  labels = {
    env : var.env
  }
  schema = <<EOF
[
  {
    "name": "query",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "business rule in SQL query apply"
  },
  {
    "name": "name",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "name of the business rule"
  },
  {
    "name": "trigger",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "every_insert, hourly, daily, weekly"
  },
  {
    "name": "type",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "type of business rule"
  },
  {
    "name": "description",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "plain text description description"
  }
]
EOF
}
*/

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

/*
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
*/


resource "google_bigquery_table" "doc_ai_extracted_entities" {
  dataset_id          = google_bigquery_dataset.dataset_results_docai.dataset_id
  table_id            = "doc_ai_extracted_entities"
  project             = var.project
  deletion_protection = false

  labels = {
    env : var.env
  }

  schema = file("./functions/gcf_input_source/bq_docai_extracted_entities.json") 
}

