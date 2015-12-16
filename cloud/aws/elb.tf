resource "aws_elb" "elb" {
  name = "${var.key_name}-elb"
  security_groups = ["${aws_security_group.default.id}"]
  availability_zones = [ "${var.aws_availability_zone}" ]
  listener {
    instance_port = "${var.cockroach_port}"
    instance_protocol = "tcp"
    lb_port = "${var.cockroach_port}"
    lb_protocol = "tcp"
  }

  health_check {
    healthy_threshold = 2
    unhealthy_threshold = 2
    timeout = 2
    interval = 5
    target = "${format("TCP:%s", "${var.cockroach_port}")}"
  }

  # Note: when there are no instances, aws_instance.cockroach.*.id has an empty
  # element, causing failed elb updates. See: https://github.com/hashicorp/terraform/issues/3581
  instances = ["${compact(split(",", join(",",aws_instance.cockroach.*.id)))}"]

  cross_zone_load_balancing = true
  idle_timeout = 400
  connection_draining = true
  connection_draining_timeout = 400
}
