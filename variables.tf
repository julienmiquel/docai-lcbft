variable "region" {
  default = "europe-west1"
}

variable "API_KEY_KG_GEOCODES" {
}
variable "project" {
  
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

