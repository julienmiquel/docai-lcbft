locals {
  services = [
    "sourcerepo.googleapis.com",
    "cloudbuild.googleapis.com",
    "run.googleapis.com",
    "iam.googleapis.com",
    "container.googleapis.com",
    "containeranalysis.googleapis.com",
    "containerregistry.googleapis.com",
    "containerscanning.googleapis.com",
    "containerfilesystem.googleapis.com" ,
    "containerthreatdetection.googleapis.com",

    "anthosconfigmanagement.googleapis.com",
    "anthosgke.googleapis.com",
    "anthosidentityservice.googleapis.com",

    "sourcerepo.googleapis.com",
    "servicenetworking.googleapis.com",
    "firestore.googleapis.com",
    "vpcaccess.googleapis.com",
    "pubsub.googleapis.com", 
    "bigquery.googleapis.com",
    "monitoring.googleapis.com",
    "cloudtrace.googleapis.com",
    "cloudtrace.googleapis.com",
    "cloudfunctions.googleapis.com",
    "documentai.googleapis.com",
    "artifactregistry.googleapis.com",
    "eventarc.googleapis.com"
  ]
}

resource "google_project_service" "enabled_service" {
  project  = var.project

  for_each = toset(local.services)
  service  = each.key
  disable_dependent_services=true
  disable_on_destroy = false
  

  provisioner "local-exec" {
    when    = create
    command = "sleep 60"

  }

  provisioner "local-exec" {
    when    = destroy
    command = "sleep 15"
  }
}