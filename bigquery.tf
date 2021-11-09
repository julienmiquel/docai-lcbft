

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
    "type": "STRING",
    "description": "key of processed file"
  },
  {
    "mode": "NULLABLE",
    "name": "insert_date",
    "type": "DATETIME",
    "description": "date of insert"
  },  
  {
    "mode": "NULLABLE",
    "name": "doc_type",
    "type": "STRING",
    "description": "type of the document processed"
  },  
   {
    "mode": "NULLABLE",
    "name": "key",
    "type": "STRING",
    "description": "key to be share between different documents"
  },   
  {
    "mode": "NULLABLE",
    "name": "signed_url",
    "type": "STRING",
    "description": "http signed URL of the archived file"
  },    
  {
    "mode": "NULLABLE",
    "name": "result",
    "type": "STRING",
    "description": "docAI blob result"
  },      
  {
    "mode": "NULLABLE",
    "name": "image",
    "type": "BYTES",
    "description": "image stored as bytes"
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
    "type": "DATE",
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
    "type": "DATE",
    "description": "CI verso"
  },
    {
    "mode": "NULLABLE",
    "name": "issue_date",
    "type": "DATE",
    "description": "CI verso"
  },
  {
    "mode": "NULLABLE",
    "name": "error",
    "type": "STRING",
    "description": "error message"
  },
  {
    "mode": "NULLABLE",
    "name": "timer",
    "type": "INTEGER",
    "description": "processing time"
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
    "type": "FLOAT"
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
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "total_tax_amount",
    "type": "FLOAT"
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
    "type": "FLOAT"
  },
  {
    "mode": "NULLABLE",
    "name": "vat_tax_amount",
    "type": "FLOAT"
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

