variable "project_name" {
  type        = string
  description = "nom du projet"
}

variable "direction" {
  type        = string
  description = "direction du projet"
}

variable "region" {
  type    = string
  default = "europe-west1"
}

variable "group_name" {
  type        = string
  description = "Google groupe associé au projet"
}

variable "schedule" {
  type        = string
  description = "expression cron de schedule du job"
}
variable "org_id" {
  description = "id de l'organisation"
  type        = number
}

variable "region" {
  type    = string
  default = "europe-west1"
}

variable "billing_account" {
  description = "Compte de facturation par défaut"
  type        = string
}
