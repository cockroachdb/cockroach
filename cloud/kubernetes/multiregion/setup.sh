#!/usr/bin/env bash

set -euxo pipefail

# Before running the script, fill in appropriate values for all the parameters
# and make sure that the cockroach binary is in your shell's PATH.
declare -A CONTEXTS
declare -A REGIONS

# To get the names of your kubectl "contexts" for each of your clusters, run:
#   kubectl config get-contexts
CONTEXTS[us-central1-a]=gke_cockroach-alex_us-central1-a_dns
CONTEXTS[us-central1-b]=gke_cockroach-alex_us-central1-b_dns
CONTEXTS[us-west1-b]=gke_cockroach-alex_us-west1-b_dns

# Setting REGIONS is optional, but recommended. If you aren't specifying them, remove these lines.
REGIONS[us-central1-a]=us-central1
REGIONS[us-central1-b]=us-central1
REGIONS[us-west1-b]=us-west1

CERTSDIR=certs
CAKEYDIR=my-safe-directory
GENERATEDFILESDIR=generated

# ------------------------------------------------------------------------------

# First, set up all certificates
mkdir -p ${CERTSDIR}
mkdir -p ${CAKEYDIR}
mkdir -p ${GENERATEDFILESDIR}

cockroach cert create-ca --certs-dir=${CERTSDIR} --ca-key=${CAKEYDIR}/ca.key
cockroach cert create-client root --certs-dir=${CERTSDIR} --ca-key=${CAKEYDIR}/ca.key

# For each cluster, create secrets containing the node and client certificates.
# Also create an internal load balancer to each cluster's DNS pods.
for ZONE in "${!CONTEXTS[@]}";
do
  kubectl create namespace ${ZONE} --context="${CONTEXTS[$ZONE]}"
  kubectl create secret generic cockroachdb.client.root --namespace=${ZONE} --from-file=${CERTSDIR} --context="${CONTEXTS[$ZONE]}"
  cockroach cert create-node --certs-dir=${CERTSDIR} --ca-key=${CAKEYDIR}/ca.key localhost 127.0.0.1 cockroachdb-public cockroachdb-public.default cockroachdb-public.${ZONE} cockroachdb-public.${ZONE}.svc.cluster.local *.cockroachdb *.cockroachdb.${ZONE} *.cockroachdb.${ZONE}.svc.cluster.local
  kubectl create secret generic cockroachdb.node --namespace=${ZONE} --from-file=${CERTSDIR} --context="${CONTEXTS[$ZONE]}"
  rm ${CERTSDIR}/node.*

  kubectl apply -f dns-lb.yaml --context="${CONTEXTS[$ZONE]}"
done

# Set up each cluster to forward DNS requests for zone-scoped namespaces to the
# relevant cluster's DNS server, using internal load balancers in order to
# create a static IP for each cluster's DNS endpoint.
declare -A DNS_IPS
for ZONE in "${!CONTEXTS[@]}";
do
  EXTERNAL_IP=""
  while [ -z $EXTERNAL_IP ]; do
    echo "Getting DNS load balancer IP in ${ZONE}..."
    EXTERNAL_IP=$(kubectl get svc kube-dns-lb --namespace=kube-system --context="${CONTEXTS[$ZONE]}" --template="{{range .status.loadBalancer.ingress}}{{.ip}}{{end}}")
    [ -z "$EXTERNAL_IP" ] && sleep 10
  done
  echo "DNS endpoint for zone ${ZONE}: ${EXTERNAL_IP}"
  DNS_IPS[$ZONE]=${EXTERNAL_IP}
done

# Update each cluster's DNS configuration with an appropriate configmap. Note
# that we have to leave the local cluster out of its own configmap to avoid
# infinite recursion through the load balancer IP. We then have to delete the
# existing DNS pods in order for the new configuration to take effect.
for ZONE in "${!CONTEXTS[@]}";
do
  IP_LIST=()
  for DNS_ZONE in "${!DNS_IPS[@]}";
  do
    if [ "$ZONE" != "$DNS_ZONE" ];
    then
      IP_LIST+=("\"${DNS_ZONE}.svc.cluster.local\": [\"${DNS_IPS[$DNS_ZONE]}\"]")
    fi
  done
  cat <<EOF > ${GENERATEDFILESDIR}/dns-configmap-${ZONE}.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kube-dns
  namespace: kube-system
data:
  stubDomains: |
    {$(IFS=,; echo "${IP_LIST[*]}")}
EOF
  kubectl apply -f ${GENERATEDFILESDIR}/dns-configmap-${ZONE}.yaml --namespace kube-system --context="${CONTEXTS[$ZONE]}"
  kubectl delete pods -l k8s-app=kube-dns --namespace kube-system --context="${CONTEXTS[$ZONE]}"
done

# Generate the join string to be used.
JOINSTR=""
for ZONE in "${!CONTEXTS[@]}";
do
  JOINSTR+="cockroachdb-0.cockroachdb.${ZONE},cockroachdb-1.cockroachdb.${ZONE},cockroachdb-2.cockroachdb.${ZONE},"
done

# Create the cockroach resources in each cluster.
for ZONE in "${!CONTEXTS[@]}";
do
  LOCALITY="zone=${ZONE}"
  if [[ -z "${REGIONS[$ZONE]}" ]];
  then
    LOCALITY="region=${REGIONS[$ZONE]},${LOCALITY}"
  fi
  sed "s/JOINLIST/${JOINSTR}/g;s/LOCALITYLIST/${LOCALITY}/g" cockroachdb-statefulset-secure.yaml > ${GENERATEDFILESDIR}/cockroachdb-statefulset-${ZONE}.yaml
  kubectl apply -f ${GENERATEDFILESDIR}/cockroachdb-statefulset-${ZONE}.yaml --namespace ${ZONE} --context="${CONTEXTS[$ZONE]}"
done

# Finally, initialize the cluster.
# NOTE: We may want to sleep before this to give the statefulset pods enough
# time to have the persistent volumes allocated and get started. Otherwise the
# job's pod tends to crash loop while waiting, and the backoff means it can
# take a while to actually succeed.
for ZONE in "${!CONTEXTS[@]}";
do
  kubectl create -f cluster-init-secure.yaml --namespace ${ZONE} --context="${CONTEXTS[$ZONE]}"
  # We only need run the init command in one zone given that all the zones are
  # joined together as one cluster.
  break
done
