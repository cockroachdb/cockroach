locals {
  roles_combinations = merge(flatten([
    for role, members in var.permissions : [
      for member in members : {
        "${var.bucket_name} ${role} ${member}" = {
          role   = role
          member = member
        }
      }
    ]
  ])...)
}

resource "google_storage_bucket_iam_member" "role" {
  for_each = local.roles_combinations
  bucket   = var.bucket_name
  role     = each.value.role
  member   = each.value.member
}
