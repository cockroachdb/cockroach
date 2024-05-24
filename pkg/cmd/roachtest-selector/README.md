# Documentation

This is the roachtest selector for selecting tests to run based on the test criteria.
The result gets uploaded to a GCS bucket so that roachtest can consume  the same.
The binary should be run as a cronjob, which keeps overwriting the GCS objects.

Reference: https://cockroachlabs.atlassian.net/wiki/x/ggBY1g
