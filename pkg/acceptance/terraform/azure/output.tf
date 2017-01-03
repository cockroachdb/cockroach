output "instances" {
  value = "${join(",", azurerm_public_ip.cockroach.*.ip_address)}"
}
