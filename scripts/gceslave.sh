#!/bin/bash

set -eu

project=${COCKROACH_PROJECT}
zone="us-east1-b"
name="gceslave"

case $1 in
    create)
    gcloud compute --project "cockroach-tschottdorf" \
           instances create "gceslave" \
           --zone "${zone}" \
           --machine-type "custom-32-65536" \
           --network "default" \
           --maintenance-policy "MIGRATE" \
           --image "/debian-cloud/debian-8-jessie-v20160803" \
           --boot-disk-size "100" \
           --boot-disk-type "pd-ssd" \
           --boot-disk-device-name "${name}"
    sleep 20 # avoid SSH timeout on copy-files

    gcloud compute copy-files --zone "${zone}" . "${name}:scripts"
    gcloud compute --project "${project}" ssh "${name}" --zone "${zone}" ./scripts/bootstrap-debian.sh
    # Install automatic shutdown. This can improve - ideally should abort a
    # pending shutdown if a user is logged in (`pgrep -f systemd-shutdownd`
    # should be helpful) so that you don't have to manually cancel it if
    # logging out for just a minute.
    gcloud compute --project "${project}" ssh "${name}" --zone "${zone}" \
    "echo '* * * * * w -hs | grep -c pts &> /dev/null || [[ -f /.active ]] || shutdown -h +10' | sudo crontab -"

    ;;
    start)
    gcloud compute --project "${project}" instances start "${name}" --zone "${zone}"
    ;;
    stop)
    gcloud compute --project "${project}" instances stop "${name}" --zone "${zone}"
    ;;
    destroy)
    gcloud compute --project "${project}" instances delete "${name}" --zone "${zone}"
    ;;
    ssh)
    shift
    gcloud compute --project "${project}" ssh "${name}" --zone "${zone}" "$@"
    ;;
    *)
    echo "$0: unknown command: $1"
    ;;
esac
