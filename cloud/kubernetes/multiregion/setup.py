#!/usr/bin/env python

import distutils.spawn
import json
import os
from subprocess import check_call,check_output
from sys import exit
from time import sleep

# Before running the script, fill in appropriate values for all the parameters
# above the dashed line.

# Fill in the `contexts` map with the zones of your clusters and their
# corresponding kubectl context names.
#
# To get the names of your kubectl "contexts" for each of your clusters, run:
#   kubectl config get-contexts
#
# example:
# contexts = {
#     'us-central1-a': 'gke_cockroach-alex_us-central1-a_my-cluster',
#     'us-central1-b': 'gke_cockroach-alex_us-central1-b_my-cluster',
#     'us-west1-b': 'gke_cockroach-alex_us-west1-b_my-cluster',
# }
contexts = {
}

# Fill in the `regions` map with the zones and corresponding regions of your
# clusters.
#
# Setting regions is optional, but recommended, because it improves cockroach's
# ability to diversify data placement if you use more than one zone in the same
# region. If you aren't specifying regions, just leave the map empty.
#
# example:
# regions = {
#     'us-central1-a': 'us-central1',
#     'us-central1-b': 'us-central1',
#     'us-west1-b': 'us-west1',
# }
regions = {
}

# Paths to directories in which to store certificates and generated YAML files.
certs_dir = './certs'
ca_key_dir = './my-safe-directory'
generated_files_dir = './generated'

# Path to the cockroach binary on your local machine that you want to use
# generate certificates. Defaults to trying to find cockroach in your PATH.
cockroach_path = 'cockroach'

# ------------------------------------------------------------------------------

# First, do some basic input validation.
if len(contexts) == 0:
    exit("must provide at least one Kubernetes cluster in the `contexts` map at the top of the script")

if len(regions) != 0 and len(regions) != len(contexts):
    exit("regions not specified for all kubectl contexts (%d regions, %d contexts)" % (len(regions), len(contexts)))

try:
    check_call(["which", cockroach_path])
except:
    exit("no binary found at provided path '" + cockroach_path + "'; please put a cockroach binary in your path or change the cockroach_path variable")

for zone, context in contexts.items():
    try:
        check_call(['kubectl', 'get', 'pods', '--context', context])
    except:
        exit("unable to make basic API call using kubectl context '%s' for cluster in zone '%s'; please check if the context is correct and your Kubernetes cluster is working" % (context, zone))

# Set up the necessary directories and certificates. Ignore errors because they may already exist.
try:
    os.mkdir(certs_dir)
except OSError:
    pass
try:
    os.mkdir(ca_key_dir)
except OSError:
    pass
try:
    os.mkdir(generated_files_dir)
except OSError:
    pass

check_call([cockroach_path, 'cert', 'create-ca', '--certs-dir', certs_dir, '--ca-key', ca_key_dir+'/ca.key'])
check_call([cockroach_path, 'cert', 'create-client', 'root', '--certs-dir', certs_dir, '--ca-key', ca_key_dir+'/ca.key'])

# For each cluster, create secrets containing the node and client certificates.
# Note that we create the root client certificate in both the zone namespace
# and the default namespace so that it's easier for clients in the default
# namespace to use without additional steps.
#
# Also create a load balancer to each cluster's DNS pods.
for zone, context in contexts.items():
    check_call(['kubectl', 'create', 'namespace', zone, '--context', context])
    check_call(['kubectl', 'create', 'secret', 'generic', 'cockroachdb.client.root', '--from-file', certs_dir, '--context', context])
    check_call(['kubectl', 'create', 'secret', 'generic', 'cockroachdb.client.root', '--namespace', zone, '--from-file', certs_dir, '--context', context])
    check_call([cockroach_path, 'cert', 'create-node', '--certs-dir', certs_dir, '--ca-key', ca_key_dir+'/ca.key', 'localhost', '127.0.0.1', 'cockroachdb-public', 'cockroachdb-public.default', 'cockroachdb-public.'+zone, 'cockroachdb-public.%s.svc.cluster.local' % (zone), '*.cockroachdb', '*.cockroachdb.'+zone, '*.cockroachdb.%s.svc.cluster.local' % (zone)])
    check_call(['kubectl', 'create', 'secret', 'generic', 'cockroachdb.node', '--namespace', zone, '--from-file', certs_dir, '--context', context])
    check_call('rm %s/node.*' % (certs_dir), shell=True)

    check_call(['kubectl', 'apply', '-f', 'dns-lb.yaml', '--context', context])

# Set up each cluster to forward DNS requests for zone-scoped namespaces to the
# relevant cluster's DNS server, using load balancers in order to create a
# static IP for each cluster's DNS endpoint.
dns_ips = dict()
for zone, context in contexts.items():
    external_ip = ''
    while True:
        external_ip = check_output(['kubectl', 'get', 'svc', 'kube-dns-lb', '--namespace', 'kube-system', '--context', context, '--template', '{{range .status.loadBalancer.ingress}}{{.ip}}{{end}}'])
        if external_ip:
            break
        print  'Waiting for DNS load balancer IP in %s...' % (zone)
        sleep(10)
    print 'DNS endpoint for zone %s: %s' % (zone, external_ip)
    dns_ips[zone] = external_ip

# Update each cluster's DNS configuration with an appropriate configmap. Note
# that we have to leave the local cluster out of its own configmap to avoid
# infinite recursion through the load balancer IP. We then have to delete the
# existing DNS pods in order for the new configuration to take effect.
for zone, context in contexts.items():
    remote_dns_ips = dict()
    for z, ip in dns_ips.items():
        if z == zone:
            continue
        remote_dns_ips[z+'.svc.cluster.local'] = [ip]
    config_filename = '%s/dns-configmap-%s.yaml' % (generated_files_dir, zone)
    with open(config_filename, 'w') as f:
        f.write("""\
apiVersion: v1
kind: ConfigMap
metadata:
  name: kube-dns
  namespace: kube-system
data:
  stubDomains: |
    %s
""" % (json.dumps(remote_dns_ips)))
    check_call(['kubectl', 'apply', '-f', config_filename, '--namespace', 'kube-system', '--context', context])
    check_call(['kubectl', 'delete', 'pods', '-l', 'k8s-app=kube-dns', '--namespace', 'kube-system', '--context', context])

# Create a cockroachdb-public service in the default namespace in each cluster.
for zone, context in contexts.items():
    yaml_file = '%s/external-name-svc-%s.yaml' % (generated_files_dir, zone)
    with open(yaml_file, 'w') as f:
        check_call(['sed', 's/YOUR_ZONE_HERE/%s/g' % (zone), 'external-name-svc.yaml'], stdout=f)
    check_call(['kubectl', 'apply', '-f', yaml_file, '--context', context])

# Generate the join string to be used.
join_addrs = []
for zone in contexts:
    for i in range(3):
        join_addrs.append('cockroachdb-%d.cockroachdb.%s' % (i, zone))
join_str = ','.join(join_addrs)

# Create the cockroach resources in each cluster.
for zone, context in contexts.items():
    if zone in regions:
        locality = 'region=%s,zone=%s' % (regions[zone], zone)
    else:
        locality = 'zone=%s' % (zone)
    yaml_file = '%s/cockroachdb-statefulset-%s.yaml' % (generated_files_dir, zone)
    with open(yaml_file, 'w') as f:
        check_call(['sed', 's/JOINLIST/%s/g;s/LOCALITYLIST/%s/g' % (join_str, locality), 'cockroachdb-statefulset-secure.yaml'], stdout=f)
    check_call(['kubectl', 'apply', '-f', yaml_file, '--namespace', zone, '--context', context])

# Finally, initialize the cluster.
print 'Sleeping 30 seconds before attempting to initialize cluster to give time for volumes to be created and pods started.'
sleep(30)
for zone, context in contexts.items():
    check_call(['kubectl', 'create', '-f', 'cluster-init-secure.yaml', '--namespace', zone, '--context', context])
    # We only need run the init command in one zone given that all the zones are
    # joined together as one cluster.
    break
