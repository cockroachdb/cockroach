#!/usr/bin/env python

from shutil import rmtree
from subprocess import call

# Before running the script, fill in appropriate values for all the parameters
# above the dashed line. You should use the same values when tearing down a
# cluster that you used when setting it up.

# To get the names of your kubectl "contexts" for each of your clusters, run:
#   kubectl config get-contexts
contexts = {
    'us-central1-a': 'gke_cockroach-alex_us-central1-a_dns',
    'us-central1-b': 'gke_cockroach-alex_us-central1-b_dns',
    'us-west1-b': 'gke_cockroach-alex_us-west1-b_dns',
}

certs_dir = './certs'
ca_key_dir = './my-safe-directory'
generated_files_dir = './generated'

# ------------------------------------------------------------------------------

# Delete each cluster's special zone-scoped namespace, which transitively
# deletes all resources that were created in the namespace, along with the few
# other resources we created that weren't in that namespace
for zone, context in contexts.items():
    call(['kubectl', 'delete', 'namespace', zone, '--context', context])
    call(['kubectl', 'delete', 'secret', 'cockroachdb.client.root', '--context', context])
    call(['kubectl', 'delete', '-f', 'external-name-svc.yaml', '--context', context])
    call(['kubectl', 'delete', '-f', 'dns-lb.yaml', '--context', context])
    call(['kubectl', 'delete', 'configmap', 'kube-dns', '--namespace', 'kube-system', '--context', context])
    # Restart the DNS pods to clear out our stub-domains configuration.
    call(['kubectl', 'delete', 'pods', '-l', 'k8s-app=kube-dns', '--namespace', 'kube-system', '--context', context])

try:
    rmtree(certs_dir)
except OSError:
    pass
try:
    rmtree(ca_key_dir)
except OSError:
    pass
try:
    rmtree(generated_files_dir)
except OSError:
    pass
