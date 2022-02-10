variable "region" {
  default = "europe-west1"
}

variable "project" {
  
}


provider "google" {
  region = var.region
}

