variable "Bucket_name" {
    description = "Name of Google Storage Bucket"
    type = string
    default = "zoomcamp-bucket-storage"
  }

variable "Project_id" {
    description = "Project ID"
    type = string
    default = "dezoomcamp-project1"
}

variable "location" {
  description = "Region where cloud data is stored"
  type = string
  default = "US-WEST2"
  
}