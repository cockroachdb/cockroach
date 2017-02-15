#!/usr/bin/env bash

set -euo pipefail

export CLOUDSDK_CORE_PROJECT=${CLOUDSDK_CORE_PROJECT-${GOOGLE_PROJECT-cockroach-$(id -un)}}
export CLOUDSDK_COMPUTE_ZONE=${GCEWORKER_ZONE-${CLOUDSDK_COMPUTE_ZONE-us-east1-b}}
NAME=${GCEWORKER_NAME-gceworker}

case ${1-} in
    create)
    gcloud compute instances \
           create "${NAME}" \
           --machine-type "custom-24-32768" \
           --network "default" \
           --maintenance-policy "MIGRATE" \
           --image-project "ubuntu-os-cloud" \
           --image "ubuntu-1604-xenial-v20170113" \
           --boot-disk-size "100" \
           --boot-disk-type "pd-ssd" \
           --boot-disk-device-name "${NAME}"

    # Retry while vm and sshd to start up.
    "$(dirname "${0}")/travis_retry.sh" gcloud compute ssh "${NAME}" --command=true

    gcloud compute copy-files "$(dirname "${0}")/../build/bootstrap" "${NAME}:bootstrap"
    gcloud compute copy-files "$(dirname "${0}")/../build/parallelbuilds-"* "${NAME}:bootstrap"
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
    shift
    gcloud compute ssh "${NAME}" --ssh-flag="-A" -- "$@"
    ;;
    *)
    echo "$0: unknown command: ${1-}, use one of create, start, stop, delete, or ssh"
    exit 1
    ;;
esac
