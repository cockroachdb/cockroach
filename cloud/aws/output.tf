output "elb_address" {
  value = "${format("%s:%s", aws_elb.elb.dns_name, var.cockroach_port)}"
}

output "instances" {
  value = "${join(",", aws_instance.cockroach.*.public_dns)}"
}
