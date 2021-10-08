#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "${0}")/.."
source build/shlib.sh

export CLOUDSDK_CORE_PROJECT=${CLOUDSDK_CORE_PROJECT-${GCEWORKER_PROJECT-cockroach-workers}}
export CLOUDSDK_COMPUTE_ZONE=${GCEWORKER_ZONE-${CLOUDSDK_COMPUTE_ZONE-us-east1-b}}
NAME=${GCEWORKER_NAME-gceworker-$(id -un)}

cmd=${1-}
if [[ "${cmd}" ]]; then
  shift
fi

case "${cmd}" in
    gcloud)
    gcloud "$@"
    ;;
    create)
    if [[ "${COCKROACH_DEV_LICENSE:-}" ]]; then
      echo "Using dev license key from \$COCKROACH_DEV_LICENSE"
    else
      echo -n "Enter your dev license key (if any): "
      read COCKROACH_DEV_LICENSE
    fi

    gcloud compute instances \
           create "${NAME}" \
           --machine-type "custom-24-32768" \
           --network "default" \
           --maintenance-policy "MIGRATE" \
           --image-project "ubuntu-os-cloud" \
           --image-family "ubuntu-1804-lts" \
           --boot-disk-size "100" \
           --boot-disk-type "pd-ssd" \
           --boot-disk-device-name "${NAME}" \
           --scopes "cloud-platform"
    gcloud compute firewall-rules create "${NAME}-mosh" --allow udp:60000-61000

    # Retry while vm and sshd start up.
    retry gcloud compute ssh "${NAME}" --command=true

    gcloud compute scp --recurse "build/bootstrap" "${NAME}:bootstrap"
    gcloud compute ssh "${NAME}" --ssh-flag="-A" --command="./bootstrap/bootstrap-debian.sh"

    if [[ "$COCKROACH_DEV_LICENSE" ]]; then
      gcloud compute ssh "${NAME}" --command="echo COCKROACH_DEV_LICENSE=$COCKROACH_DEV_LICENSE >> ~/.bashrc_bootstrap"
    fi

    # Install automatic shutdown after ten minutes of operation without a
    # logged in user. To disable this, `sudo touch /.active`.
    gcloud compute ssh "${NAME}" --command="sudo cp bootstrap/autoshutdown.cron.sh /root/; echo '* * * * * /root/autoshutdown.cron.sh 10' | sudo crontab -i -"

    ;;
    start)
    gcloud compute instances start "${NAME}"
    echo "waiting for node to finish starting..."
    # Wait for vm and sshd to start up.
    retry gcloud compute ssh "${NAME}" --command=true || true
    # SSH into the node, since that's probably why we started it.
    gcloud compute ssh "${NAME}" --ssh-flag="-A" "$@"
    ;;
    stop)
    read -r -p "This will stop the VM. Are you sure? [yes] " response
    # Convert to lowercase.
    response=$(echo "$response" | tr '[:upper:]' '[:lower:]')
    if [[ $response != "yes" ]]; then
      echo Aborting
      exit 1
    fi
    gcloud compute instances stop "${NAME}"
    ;;
    delete|destroy)
    read -r -p "This will delete the VM! Are you sure? [yes] " response
    # Convert to lowercase.
    response=$(echo "$response" | tr '[:upper:]' '[:lower:]')
    if [[ $response != "yes" ]]; then
      echo Aborting
      exit 1
    fi
    status=0
    gcloud compute firewall-rules delete "${NAME}-mosh" --quiet || status=$((status+1))
    gcloud compute instances delete "${NAME}" --quiet || status=$((status+1))
    exit ${status}
    ;;
    ssh)
    gcloud compute ssh "${NAME}" --ssh-flag="-A" "$@"
    ;;
    mosh)
    # An alternative solution would be to run gcloud compute config-ssh after
    # starting or creating the vm, which adds stanzas to ~/.ssh/config that
    # make `ssh $HOST` (and by extension, hopefully, mosh).
    read -r -a arr <<< "$(gcloud compute ssh "${NAME}" --dry-run)"
    host="${arr[-1]}"
    unset 'arr[${#arr[@]}-1]'
    mosh --ssh=$(printf '%q' "${arr}") $host
    ;;
    scp)
    # Example: $0 scp gceworker-youruser:go/src/github.com/cockroachdb/cockroach/cockroach-data/logs gcelogs --recurse
    retry gcloud compute scp "$@"
    ;;
    ip)
    gcloud compute instances describe --format="value(networkInterfaces[0].accessConfigs[0].natIP)" "${NAME}"
    ;;
    sync)
    if ! hash unison 2>/dev/null; then
      echo 'unison not found (on macOS, run `brew install unison`)' >&2
      exit 1
    fi
    if ! hash unison-fsmonitor 2>/dev/null; then
      echo 'unison-fsmonitor not installed (on macOS, run `brew install eugenmayer/dockersync/unox`)'
      exit 1
    fi
    if (( $# == 0 )); then
      host=.  # Sync the Cockroach repo by default.
      worker=go/src/github.com/cockroachdb/cockroach
    elif (( $# == 2 )); then
      host=$1
      worker=$2
    else
      echo "usage: $0 mount [HOST-PATH WORKER-PATH]" >&2
      exit 1
    fi
    read -p "Warning: sync will overwrite files on the GCE worker with your local copy. Continue? (Y/n) "
    if [[ "$REPLY" && "$REPLY" != [Yy] ]]; then
      exit 1
    fi
    tmpfile=$(mktemp)
    trap 'rm -f ${tmpfile}' EXIT
    gcloud compute config-ssh --ssh-config-file "$tmpfile" > /dev/null
    unison "$host" "ssh://${NAME}.${CLOUDSDK_COMPUTE_ZONE}.${CLOUDSDK_CORE_PROJECT}/$worker" \
      -sshargs "-F ${tmpfile}" -auto -prefer "$host" -repeat watch \
      -ignore 'Path .git' \
      -ignore 'Path bin*' \
      -ignore 'Path build/builder_home' \
      -ignore 'Path pkg/sql/parser/gen' \
      -ignore 'Path pkg/ui/node_modules' \
      -ignore 'Path pkg/ui/.cache-loader' \
      -ignore 'Path cockroach-data' \
      -ignore 'Name *.d' \
      -ignore 'Name *.o' \
      -ignore 'Name zcgo_flags*.go'
    ;;
    *)
    echo "$0: unknown command: ${cmd}, use one of create, start, stop, delete, ssh, or sync"
    exit 1
    ;;
esac
