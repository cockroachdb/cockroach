output "port" {
  value = "${var.cockroach_port}"
}

output "fowarding_rule" {
  value = "${google_compute_forwarding_rule.default.ip_address}"
}

output "gossip_variable" {
  value = "${format("lb=%s:%s", google_compute_forwarding_rule.default.ip_address, var.cockroach_port)}"
}

output "instances" {
  value = "${join(",", google_compute_instance.cockroach.*.network_interface.0.access_config.0.nat_ip)}"
}
