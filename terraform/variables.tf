variable "direction" {
  type        = string
  description = "direction du projet"
}

variable "region" {
  type    = string
  default = "europe-west1"
}

variable "org_id" {
  description = "id de l'organisation"
  type        = number
}

variable "billing_account" {
  description = "Compte de facturation par défaut"
  type        = string
}
