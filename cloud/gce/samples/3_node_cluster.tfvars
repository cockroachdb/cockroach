# This configuration starts a 3-node CockroachDB cluster. You'll want to
# at least review all settings in this file.

# Number of instances of CockroachDB to start. You may want to change this.
num_instances = "3"

# The GCE project under which you want to run your cluster. You'll want to
# change this.
gce_project = "name-of-your-gce-project"

# Your JSON-format Google Cloud application credentials. You'll want to change this.
# To learn how to download your credentials, go here:
#
# https://developers.google.com/identity/protocols/application-default-credentials#howtheywork
gce_account_file = "/path/to/your/google/cloud/credentials.json"

# Full path to your "cockroach" binary. Leaving this commented results in the
# latest official CockroachDB binary being downloaded.
#cockroach_binary = "~/go/src/github.com/cockroachdb/cockroach/cockroach"

# Name of the ssh key pair to use for GCE instances.
# The public key will be passed at instance creation, and the private
# key will be used by the local ssh client.
#
# The path is expanded to: ~/.ssh/<key_name>.pub
#
# If you use `gcloud compute ssh` or `gcloud compute copy-files`, you may want
# to leave this as "google_compute_engine" for convenience. 
key_name = "google_compute_engine"

# GCE region to use.
gce_region = "us-east1"

# GCE zone to use.
gce_zone = "us-east1-b"
