# Unlike the other cloud providers, this is a list of FQDNs instead of IP
# addresses. This is because of an issue preventing the querying of dynamic
# public IPs:
#
# https://github.com/hashicorp/terraform/issues/6634
output "instances" {
  value = "${join(",", azurerm_public_ip.cockroach.*.fqdn)}"
}
