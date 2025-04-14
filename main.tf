resource "google_storage_bucket" "auto-expire" {
  name          = var.Bucket_name
  location      = var.location
  force_destroy = true
  project = var.Project_id

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }

  }
}