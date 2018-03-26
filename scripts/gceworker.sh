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
    create)
    gcloud compute instances \
           create "${NAME}" \
           --machine-type "custom-24-32768" \
           --network "default" \
           --maintenance-policy "MIGRATE" \
           --image-project "ubuntu-os-cloud" \
           --image-family "ubuntu-1604-lts" \
           --boot-disk-size "100" \
           --boot-disk-type "pd-ssd" \
           --boot-disk-device-name "${NAME}" \
           --scopes "default,cloud-platform"

    # Retry while vm and sshd to start up.
    retry gcloud compute ssh "${NAME}" --command=true

    gcloud compute copy-files "build/bootstrap" "${NAME}:bootstrap"
    gcloud compute ssh "${NAME}" --ssh-flag="-A" --command="./bootstrap/bootstrap-debian.sh"

    # Install automatic shutdown after ten minutes of operation without a
    # logged in user. To disable this, `sudo touch /.active`.
    gcloud compute ssh "${NAME}" --command="sudo cp bootstrap/autoshutdown.cron.sh /root/; echo '* * * * * /root/autoshutdown.cron.sh 10' | sudo crontab -i -"

    ;;
    start)
    gcloud compute instances start "${NAME}"
    ;;
    stop)
    gcloud compute instances stop "${NAME}"
    ;;
    delete)
    gcloud compute instances delete "${NAME}"
    ;;
    ssh)
    retry gcloud compute ssh "${NAME}" --ssh-flag="-A" "$@"
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
      -ignore 'Path bin*' \
      -ignore 'Path build/builder_home' \
      -ignore 'Path pkg/sql/parser/gen' \
      -ignore 'Path cockroach-data' \
      -ignore 'Name zcgo_flags*.go'
    ;;
    *)
    echo "$0: unknown command: ${cmd}, use one of create, start, stop, delete, ssh, or sync"
    exit 1
    ;;
esac
