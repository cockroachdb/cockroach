resource "google_compute_firewall" "default" {
  name = "${var.prefix}-cockroach-firewall"
  network = "default"

  allow {
    protocol = "tcp"
    ports = [26257, 8080]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags = ["cockroach"]
}

