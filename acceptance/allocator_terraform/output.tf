#output "cockroach_ips" {
#  value = "${join(",", google_compute_instance.cockroach.*.network_interface.0.access_config.0.assigned_nat_ip)}"
#}

output "instances" {
  value = "${join(",", google_compute_instance.cockroach.*.network_interface.0.access_config.0.assigned_nat_ip)}"
}

#output "admin_urls" {
#  value="${join(",", formatlist("http://%s:8080/", google_compute_instance.cockroach.*.network_interface.0.access_config.0.assigned_nat_ip))}"
#}
