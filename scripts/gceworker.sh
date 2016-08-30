#!/usr/bin/env bash

set -euxo pipefail

export CLOUDSDK_CORE_PROJECT=${CLOUDSDK_CORE_PROJECT-${GOOGLE_PROJECT-cockroach-$(id -un)}}
export CLOUDSDK_COMPUTE_ZONE=${GCEWORKER_ZONE-${CLOUDSDK_COMPUTE_ZONE-us-east1-b}}

name=${GCEWORKER_NAME-gceworker}

cd "$(dirname "${0}")"

case ${1-} in
    create)
    gcloud compute instances \
           create "${name}" \
           --machine-type "custom-32-65536" \
           --network "default" \
           --maintenance-policy "MIGRATE" \
           --image "/debian-cloud/debian-8-jessie-v20160803" \
           --boot-disk-size "100" \
           --boot-disk-type "pd-ssd" \
           --boot-disk-device-name "${name}"
    sleep 20 # avoid SSH timeout on copy-files

    gcloud compute copy-files . "${name}:scripts"
    gcloud compute ssh "${name}" ./scripts/bootstrap-debian.sh
    # Install automatic shutdown after ten minutes of operation without a
    # logged in user. To disable this, `sudo touch /.active`.
    # This is much more intricate than it looks. A few complications which
    # are addressed in these few commands:
    # - Once a shutdown is close enough, ssh logins are not allowed any more;
    #   hence we preventively remove the /etc/nologin file.
    # - `pgrep` will never match itself, but it will match its parent process
    #   and so we require an exact match for systemd-shutdownd's full path.
    # - calling shutdown with a later date cancels the previous shutdown, so
    #   once a shutdown has been scheduled, we don't want to keep scheduling
    #   later ones or we will never actually shut down - hence the pgrep.
    # - This is invoked via `sh -c`, and so no `bash` features must be used.
    gcloud compute ssh "${name}" \
      "echo '* * * * * rm -f /etc/nologin; (w -hs | (grep -q pts && /sbin/shutdown -c --no-wall) || [ -f /.active ] || pgrep -flx /lib/systemd/systemd-shutdownd || /sbin/shutdown --no-wall -h +10) >>/root/idle.log 2>&1' | sudo crontab -"

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
    gcloud compute ssh "${name}" "$@"
    ;;
    *)
    echo "$0: unknown command: ${1-}, use one of create, start, stop, destroy, or ssh"
    exit 1
    ;;
esac
