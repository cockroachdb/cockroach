output "admin_url" {
  value = "http://${google_compute_forwarding_rule.http_port.ip_address}:${var.http_port}/"
}

output "sql_url" {
  value = "postgresql://root@${google_compute_forwarding_rule.sql_port.ip_address}:${var.sql_port}/?sslmode=disable"
}

output "instances" {
  value = "${join(", ", google_compute_instance.cockroach.*.name)}"
}
