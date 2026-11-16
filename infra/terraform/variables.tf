variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "europe-west1"
}

variable "order_image" {
  type        = string
  description = "e.g. europe-west1-docker.pkg.dev/<project>/poc/order-service:latest"
  default     = ""
}

variable "payment_image" {
  type    = string
  default = ""
}

variable "alert_email" {
  type        = string
  description = "Receives the outbox_failed alert"
}
