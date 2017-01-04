output "controller-ip" {
  value = "${google_compute_instance.controller.0.network_interface.0.access_config.0.assigned_nat_ip}"
}
