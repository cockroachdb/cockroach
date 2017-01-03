output "instances" {
  value = "${join(",", google_compute_instance.cockroach.*.network_interface.0.access_config.0.assigned_nat_ip)}"
}
