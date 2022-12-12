
resource "google_document_ai_processor" "fr_driver_license" {
  display_name = "fr_driver_license"
  type = "FR_DRIVER_LICENSE_PROCESSOR"
  location                    = var.location
  project                     = var.project  
}

resource "google_document_ai_processor_default_version" "fr_driver_license" {
  processor = google_document_ai_processor.fr_driver_license.id
  version = "${google_document_ai_processor.fr_driver_license.id}/processorVersions/pretrained-next"
}



resource "google_document_ai_processor" "fr_national_id" {
  display_name = "fr_national_id"
  type = "FR_NATIONAL_ID_PROCESSOR"
  location                    = var.location
  project                     = var.project  
}

resource "google_document_ai_processor_default_version" "fr_national_id" {
  processor = google_document_ai_processor.fr_national_id.id
  version = "${google_document_ai_processor.fr_national_id.id}/processorVersions/pretrained-next"
}
