terraform {
  backend "gcs" {
    bucket = "bkt-dinum-c-basic-mgr-tfstate"
    prefix = "terraform/postgresql-to-bigquery-template"
  }
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~>5.9.0"
    }
  }
}
