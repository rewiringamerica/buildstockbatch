# Terraform file to define GCP resources used by buildstock_gcp
# TODO: Clean up, add better comments, and use this in gcp.py.

variable "gcp_project" {
    type = string
    description = "GCP project to use"
    default = "buildstockbatch-dev"
}

# Can be set via -var command line flags
variable "output_bucket" {
    type = string
    description = "GCS bucket where buildstockbatch outputs should be stored"
    default = "testing"
}

# TODO: resuse topic across multiple jobs? If this will be destroyed when the job finishes,
# then each job needs a unique topic
variable "topic_name" {
    type = string
    default = "notifications"
}

# TODO: artifact registry with deletion policy? Or manually clean up images when running --clean?


provider "google" {
  # credentials = file(".json")

  # project = var.gcp_project
  project = "buildstockbatch-dev"
  # region  = "us-central1"
  # zone    = "us-central1-c"
}

# Pub/sub topic for job notifications
resource "google_pubsub_topic" "notification_topic" {
    name = var.topic_name
}

# GCS bucket
resource "google_storage_bucket" "bucket" {
    name          = var.output_bucket
    location      = "US"
    force_destroy = false

    uniform_bucket_level_access = true
}


# Output directory for results
resource "google_storage_bucket_object" "empty_folder" {
  name   = "buildstockbatch/" # folder name should end with '/'
  content = " "            # content is ignored but should be non-empty
  bucket = var.output_bucket

  depends_on = [google_storage_bucket.bucket]
}
