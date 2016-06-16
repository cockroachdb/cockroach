resource "google_compute_firewall" "default" {
  name = "${var.prefix}-cockroach-firewall"
  network = "default"

  allow {
    protocol = "tcp"
    ports = ["${var.sql_port}", "${var.http_port}", 9001 ]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags = ["cockroach"]
}

