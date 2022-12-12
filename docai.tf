
//fr_driver_license
resource "google_document_ai_processor" "fr_driver_license" {
  display_name = "fr_driver_license"
  type = "FR_DRIVER_LICENSE_PROCESSOR"
  location                    = var.PROCESSOR_CNI_LOCATION
  project                     = var.project  
}

resource "google_document_ai_processor_default_version" "fr_driver_license" {
  processor = google_document_ai_processor.fr_driver_license.id
  version = "${google_document_ai_processor.fr_driver_license.id}/processorVersions/stable"
}


//fr_national
resource "google_document_ai_processor" "fr_national" {
  display_name = "fr_national"
  type = "FR_NATIONAL_ID_PROCESSOR"
  location                    = var.PROCESSOR_CNI_LOCATION
  project                     = var.project  
}

resource "google_document_ai_processor_default_version" "fr_national" {
  processor = google_document_ai_processor.fr_national.id
  version = "${google_document_ai_processor.fr_national.id}/processorVersions/stable"
}

//fr_passport
resource "google_document_ai_processor" "fr_passport" {
  display_name = "fr_passport"
  type = "FR_PASSPORT_PROCESSOR"
  location                    = var.PROCESSOR_CNI_LOCATION
  project                     = var.project  
}

resource "google_document_ai_processor_default_version" "fr_passport" {
  processor = google_document_ai_processor.fr_passport.id
  version = "${google_document_ai_processor.fr_passport.id}/processorVersions/stable"
}

//id_proofing
resource "google_document_ai_processor" "id_proofing" {
  display_name = "id_proofing"
  type = "ID_PROOFING_PROCESSOR"
  location                    = var.PROCESSOR_CNI_LOCATION
  project                     = var.project  
}

resource "google_document_ai_processor_default_version" "id_proofing" {
  processor = google_document_ai_processor.id_proofing.id
  version = "${google_document_ai_processor.id_proofing.id}/processorVersions/stable"
}



//us_driver_license
resource "google_document_ai_processor" "us_driver_license" {
  display_name = "us_driver_license"
  type = "US_DRIVER_LICENSE_PROCESSOR"
  location                    = var.PROCESSOR_CNI_LOCATION
  project                     = var.project  
}

resource "google_document_ai_processor_default_version" "us_driver_license" {
  processor = google_document_ai_processor.us_driver_license.id
  version = "${google_document_ai_processor.us_driver_license.id}/processorVersions/stable"
}


//fr_national
resource "google_document_ai_processor" "us_passport" {
  display_name = "us_passport"
  type = "US_PASSPORT_PROCESSOR"
  location                    = var.PROCESSOR_CNI_LOCATION
  project                     = var.project  
}

resource "google_document_ai_processor_default_version" "us_passport" {
  processor = google_document_ai_processor.us_passport.id
  version = "${google_document_ai_processor.us_passport.id}/processorVersions/stable"
}
