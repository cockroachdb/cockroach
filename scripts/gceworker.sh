#!/usr/bin/env bash

set -euxo pipefail

export CLOUDSDK_CORE_PROJECT=${CLOUDSDK_CORE_PROJECT-${GOOGLE_PROJECT-cockroach-$(id -un)}}
export CLOUDSDK_COMPUTE_ZONE=${GCEWORKER_ZONE-${CLOUDSDK_COMPUTE_ZONE-us-east1-b}}
GOVERSION=${GOVERSION-1.7.1}

name=${GCEWORKER_NAME-gceworker$(echo "${GOVERSION}" | tr -d '.')}

case ${1-} in
    create)
    gcloud compute instances \
           create "${name}" \
           --machine-type "custom-32-32768" \
           --network "default" \
           --maintenance-policy "MIGRATE" \
           --image "/ubuntu-os-cloud/ubuntu-1604-xenial-v20160830" \
           --boot-disk-size "100" \
           --boot-disk-type "pd-ssd" \
           --boot-disk-device-name "${name}"
    sleep 20 # avoid SSH timeout on copy-files

    gcloud compute copy-files "$(dirname "${0}")" "${name}:scripts"
    gcloud compute ssh "${name}" --ssh-flag="-A" --command="GOVERSION=${GOVERSION} ./scripts/bootstrap-debian.sh"

    # Install automatic shutdown after ten minutes of operation without a
    # logged in user. To disable this, `sudo touch /.active`.
    gcloud compute ssh "${name}" --command="sudo cp scripts/autoshutdown.cron.sh /root/; echo '* * * * * /root/autoshutdown.cron.sh 10' | sudo crontab -i -"

    ;;
    start)
    gcloud compute instances start "${name}"
    ;;
    stop)
    gcloud compute instances stop "${name}"
    ;;
    destroy)
    gcloud compute instances delete "${name}"
    ;;
    ssh)
    shift
    gcloud compute ssh "${name}" --ssh-flag="-A" -- "$@"
    ;;
    *)
    echo "$0: unknown command: ${1-}, use one of create, start, stop, destroy, or ssh"
    exit 1
    ;;
esac
