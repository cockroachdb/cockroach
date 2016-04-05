resource "google_compute_http_health_check" "default" {
  name = "cockroach-health-check"
  request_path = "/"
  port = "${var.http_port}"
  check_interval_sec = 2
  healthy_threshold = 2
  unhealthy_threshold = 2
  timeout_sec = 2
}

resource "google_compute_target_pool" "default" {
  name = "cockroach-target-pool"
  # Note: when there are no instances, aws_instance.cockroach.*.id has an empty
  # element, causing failed elb updates. See: https://github.com/hashicorp/terraform/issues/3581
  instances = ["${compact(split(",", join(",",google_compute_instance.cockroach.*.self_link)))}"]
  health_checks = ["${google_compute_http_health_check.default.name}"]
}

# TODO(cdo): find a way to collapse forwarding rules into a single forwarding
# rule.
resource "google_compute_forwarding_rule" "sql_port" {
  name = "cockroach-forwarding-rule"
  target = "${google_compute_target_pool.default.self_link}"
  port_range = "${var.sql_port}"
}

resource "google_compute_forwarding_rule" "http_port" {
  name = "http-forwarding-rule"
  target = "${google_compute_target_pool.default.self_link}"
  port_range = "${var.http_port}"
}

resource "google_compute_firewall" "default" {
  name = "cockroach-firewall"
  network = "default"

  allow {
    protocol = "tcp"
    ports = ["${var.sql_port}", "${var.http_port}"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags = ["cockroach"]
}

