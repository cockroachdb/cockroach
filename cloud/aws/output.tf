output "instances" {
  value = "${join(",", aws_instance.cockroach.*.public_dns)}"
}
