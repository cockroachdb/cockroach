# Load-balanced admin UI URL.
output "admin_lb_url" {
  value = "http://${google_compute_forwarding_rule.default.ip_address}:${var.http_port}/"
}

# Admin URLs for individual instances.
output "admin_urls" {
    value = "${join(", ", formatlist("http://%s:%s/", google_compute_instance.cockroach.*.network_interface.0.access_config.0.assigned_nat_ip, var.http_port))}"
}

# Load-balanced SQL URL.
output "sql_lb_url" {
  value = "postgresql://root@${google_compute_forwarding_rule.default.ip_address}:${var.sql_port}/?sslmode=disable"
}

# Postgres URLs for individual instances.
output "sql_urls" {
  value = "${join(", ", formatlist("postgresql://root@%s:%s/?sslmode=disable", google_compute_instance.cockroach.*.network_interface.0.access_config.0.assigned_nat_ip, var.sql_port))}"
}

# Google Cloud names (not DNS names) for CockroachDB instances. These can be
# used with `gcloud compute ssh`.
output "instances" {
  value = "${join(", ", google_compute_instance.cockroach.*.name)}"
}
