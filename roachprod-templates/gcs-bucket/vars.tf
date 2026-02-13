#
## Variables injected by roachprod-centralized for all templates
#
# identifier is a random string to ensure uniqueness of the resource name (e.g. abc12345).
variable "identifier" {
  type = string
}

#
## Variables calculated by roachprod-centralized and injected only if the template
## declares them as input variables
#

# prov_name is a human readable name of the provisioning with the following format:
# {template-type}-{identifier} (e.g. gcs-bucket-abc12345)
variable "prov_name" {
  type = string
}
# environment is the environment in which the resource is provisioned (e.g. staging, production).
variable "environment" {
  type = string
}
# owner is the email of the user who triggered the provisioning.
variable "owner" {
  type = string
}

#
## Template-specific variables, either pass by the user when creating the provisioning
## or part of the environment variables injected by roachprod-centralized if the template
# declares them as input variables.
#

variable "gcp_project" {
  type = string
}

variable "bucket_config" {
  type = object({
    region                      = optional(string, "us-east1")
    storage_class               = optional(string, "STANDARD")
    uniform_bucket_level_access = optional(bool, true)
    labels = optional(map(string), {
      "test_label" = "label_value"
    })
    force_destroy = optional(bool, false)
  })
  default = {}
}

variable "unused" {
  type        = string
  default     = null
  description = "This is an unused variable to test parsing"
}
