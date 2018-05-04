#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "${0}")/.."
source build/shlib.sh

export CLOUDSDK_CORE_PROJECT=${CLOUDSDK_CORE_PROJECT-${GCEWORKER_PROJECT-cockroach-workers}}
export CLOUDSDK_COMPUTE_ZONE=${GCEWORKER_ZONE-${CLOUDSDK_COMPUTE_ZONE-us-east1-b}}
NAME=${GCEWORKER_NAME-gceworker-win-$(id -un)}

cmd=${1-}
if [[ "${cmd}" ]]; then
  shift
fi

reset_password() {
  gcloud compute reset-windows-password --quiet "${NAME}"
}

case "${cmd}" in
    create)
    gcloud compute instances \
           create "${NAME}" \
           --machine-type "n1-standard-4" \
           --network "default" \
           --maintenance-policy "MIGRATE" \
           --image-project "windows-cloud" \
           --image-family "windows-2016" \
           --boot-disk-size "100" \
           --boot-disk-type "pd-ssd" \
           --boot-disk-device-name "${NAME}" \
           --scopes "default,cloud-platform"
    retry reset_password
    ;;
    start)
    gcloud compute instances start "${NAME}"
    ;;
    stop)
    gcloud compute instances stop "${NAME}"
    ;;
    delete|destroy)
    gcloud compute instances delete "${NAME}"
    ;;
    ssh)
    retry gcloud compute ssh "${NAME}" --ssh-flag="-A" "$@"
    ;;
    ip)
    gcloud compute instances describe --format="value(networkInterfaces[0].accessConfigs[0].natIP)" "${NAME}"
    ;;
    reset-password)
    reset_password
    ;;
    *)
    echo "$0: unknown command: ${cmd}, use one of create, start, stop, delete, or reset-password"
    exit 1
    ;;
esac
