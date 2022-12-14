variable "region" {
  default = "europe-west1"
}

variable "project" {
  
}

variable "location" {
  default = "EU"
}

variable "env" {
  default = "dev"
}

variable "PROCESSOR_CNI_LOCATION" {
  default = "eu"
}

variable "service_account_name" {
  default = "demo-docai-sa"
}


variable "deletion_protection" {
  default = false
}

provider "google" {
  region = var.region
}

