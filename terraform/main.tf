locals {
  parent_folder_id            = "658965356947" # production folder
  postgresl_driver_remote_url = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.6/postgresql-42.2.6.jar"
}

module "project-factory" {
  source                      = "terraform-google-modules/project-factory/google"
  version                     = "~> 14.4"
  name                        = "prj-dinum-data-templates"
  org_id                      = var.org_id
  billing_account             = var.billing_account
  group_name                  = "data"
  random_project_id           = true
  budget_alert_spent_percents = [50, 75, 90]
  budget_amount               = 10
  create_project_sa           = false
  default_service_account     = "delete"
  folder_id                   = "folders/${local.parent_folder_id}"
  labels = {
    direction = var.direction
  }
  activate_apis = [
    "storage.googleapis.com",
  ]
}

resource "google_storage_bucket" "bucket" {
  project                     = module.project-factory.project_id
  name                        = "bucket-${module.project-factory.project_id}"
  location                    = var.region
  storage_class               = "REGIONAL"
  uniform_bucket_level_access = true
}

# driver postgresl
# data "http" "postgresql_driver" {
#   url = local.postgresl_driver_remote_url
# }

# resource "local_sensitive_file" "postgresql_driver_local" {
#   content  = data.http.postgresql_driver.response_body
#   filename = "${path.module}/postgresql-42.2.6.jar"
# }

# resource "google_storage_bucket_object" "postgresql_driver" {
#   name       = "postgresql-42.2.6.jar"
#   source     = "${path.module}/postgresql-42.2.6.jar"
#   bucket     = google_storage_bucket.bucket.name
#   depends_on = [local_sensitive_file.postgresql_driver_local]
# }

resource "google_artifact_registry_repository" "template-repo" {
  location      = var.region
  repository_id = "template-repository"
  description   = "Dataflow template docker repository"
  format        = "DOCKER"
}
