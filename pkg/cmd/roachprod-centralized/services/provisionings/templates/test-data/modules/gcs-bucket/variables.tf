variable "project_id" {
  description = "The project for this project's resources."
  type        = string
}

variable "bucket_name" {
  description = "The name of the bucket to create."
  type        = string
}

variable "location" {
  description = "The location of the bucket to create."
  type        = string
}

variable "storage_class" {
  description = "The storage class of the bucket to create."
  type        = string
}

variable "uniform_bucket_level_access" {
  description = "Whether to enable uniform bucket level access."
  type        = bool
}

variable "public_access_prevention" {
  description = "The public access prevention setting for the bucket."
  type        = string
}

variable "force_destroy" {
  description = "Whether to allow force destroying the bucket."
  type        = bool
}

variable "permissions" {
  description = "The permissions to apply to the bucket."
  type        = map(list(string))
}

variable "encryption_kms_key_name" {
  description = "The KMS key to use for encryption."
  type        = string
  default     = null
  nullable    = true
}

variable "labels" {
  description = "The labels to apply to the bucket."
  type        = map(string)
  default     = {}
}
