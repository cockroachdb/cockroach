#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "${0}")/.."
source build/shlib.sh

export CLOUDSDK_CORE_PROJECT=${CLOUDSDK_CORE_PROJECT-${GCEWORKER_PROJECT-cockroach-workers}}
export CLOUDSDK_COMPUTE_ZONE=${GCEWORKER_ZONE-${CLOUDSDK_COMPUTE_ZONE-us-east1-b}}

USER_ID=$(id -un)
NAME=${GCEWORKER_NAME-gceworker-${USER_ID//.}}
FQNAME="${NAME}.${CLOUDSDK_COMPUTE_ZONE}.${CLOUDSDK_CORE_PROJECT}"

cmd=${1-}
if [[ "${cmd}" ]]; then
  shift
fi

function start_and_wait() {
    gcloud compute instances start "${1}"
    echo "waiting for node to finish starting..."
    # Wait for vm and sshd to start up.
    retry gcloud compute ssh "${1}" --command=true || true
}

function createVM() {
    if [[ "${COCKROACH_DEV_LICENSE:-}" ]]; then
      echo "Using dev license key from \$COCKROACH_DEV_LICENSE"
    else
      echo -n "Enter your dev license key (if any): "
      read -r COCKROACH_DEV_LICENSE
    fi

    gsuite_account_for_label="$(gcloud auth list --filter=status:ACTIVE --format='value(account)' | sed -e 's/@/__at__/g' -e 's/\./__dot__/g')"
    gcloud compute instances create "${NAME}" \
      --machine-type "n2-custom-24-32768" \
      --network "default" \
      --maintenance-policy "MIGRATE" \
      --image-project "ubuntu-os-cloud" \
      --image-family "ubuntu-2004-lts" \
      --boot-disk-size "250" \
      --boot-disk-type "pd-ssd" \
      --boot-disk-device-name "${NAME}" \
      --scopes "cloud-platform" \
      --labels "created-by=${gsuite_account_for_label:0:63}" \
      --metadata=startup-script-url=https://raw.githubusercontent.com/cockroachdb/cockroach/master/build/bootstrap/refresh-autoshutdown.cron.sh
    gcloud compute firewall-rules create "${NAME}-mosh" --allow udp:60000-61000

    # wait a bit to let gcloud create the instance before retrying
    sleep 60
    # Retry while vm and sshd start up.
    retry gcloud compute ssh "${NAME}" --command=true

    gcloud compute scp --recurse "build/bootstrap" "${NAME}:bootstrap"
    gcloud compute ssh "${NAME}" --ssh-flag="-A" --command="./bootstrap/bootstrap-debian.sh"

    if [[ "$COCKROACH_DEV_LICENSE" ]]; then
      gcloud compute ssh "${NAME}" --command="echo COCKROACH_DEV_LICENSE=$COCKROACH_DEV_LICENSE >> ~/.bashrc_bootstrap"
    fi

    # Install automatic shutdown after ten minutes of operation without a
    # logged in user. To disable this for 2 days, `sudo touch /.active`.
    gcloud compute ssh "${NAME}" --command="sudo cp bootstrap/autoshutdown.cron.sh /root/; echo '* * * * * /root/autoshutdown.cron.sh 10' | sudo crontab -i -"
    gcloud compute ssh "${NAME}" --command="sudo reboot"
}

function startVM() {
    start_and_wait "${NAME}"
    if ! gcloud compute config-ssh > /dev/null; then
	    echo "WARNING: Unable to invoke config-ssh, you may not be able to 'ssh ${FQNAME}'"
    fi

    # SSH into the node, since that's probably why we started it.
    echo "****************************************"
    echo "Hint: you should also be able to directly invoke:"
    echo "ssh ${FQNAME}"
    echo "  or"
    echo "mosh ${FQNAME}"
    echo "instead of '$0 ssh'."
    echo
    if [ -z "${GCEWORKER_START_SSH_COMMAND-}" ]; then
	    echo "Connecting via SSH."
	    echo "Set GCEWORKER_START_SSH_COMMAND=mosh to use mosh instead"
    fi
    echo "****************************************"
    $0 "${GCEWORKER_START_SSH_COMMAND-ssh}"
}

function stopVM() {
    read -r -p "This will stop the VM. Are you sure? [yes] " response
    # Convert to lowercase.
    response=$(echo "$response" | tr '[:upper:]' '[:lower:]')
    if [[ $response != "yes" ]]; then
      echo Aborting
      exit 1
    fi
    gcloud compute instances stop "${NAME}"
}

function suspendVM() {
    read -r -p "This will pause the VM. Are you sure? [yes] " response
    response=$(echo "$response" | tr '[:upper:]' '[:lower:]')
    if [[ $response != "yes" ]]; then
      echo Aborting
      exit 1
    fi
    gcloud compute instances suspend "${NAME}"
}

function resumeVM() {
    gcloud compute instances resume "${NAME}"
    echo "waiting for node to finish starting..."
    # Wait for vm and sshd to start up.
    retry gcloud compute ssh "${NAME}" --command=true || true

    # Rewrite the SSH config, since the VM may now be bound to a new ephemeral IP address.
    if ! gcloud compute config-ssh > /dev/null; then
      echo "WARNING: Unable to invoke config-ssh, you may not be able to 'ssh ${FQNAME}'"
    fi

    # SSH into the node, since that's probably why we resumed it.
    $0 ssh
}

function resetVM() {
    read -r -p "This will hard reset (\"powercycle\") the VM. Are you sure? [yes] " response
    # Convert to lowercase.
    response=$(echo "$response" | tr '[:upper:]' '[:lower:]')
    if [[ $response != "yes" ]]; then
      echo Aborting
      exit 1
    fi
    gcloud compute instances reset "${NAME}"
}

function destroyVM() {
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
}

case "${cmd}" in
    create)
      createVM
      ;;
    start)
      startVM
      ;;
    stop)
      stopVM
      ;;
    suspend)
      suspendVM
      ;;
    resume)
      resumeVM
      ;;
    reset)
      resetVM
      ;;
    delete|destroy)
      destroyVM
      ;;
    ssh)
      gcloud compute ssh "${NAME}" --ssh-flag="-A" "$@"
      ;;
    mosh)
      mosh "${FQNAME}"
      ;;
    gcloud)
      gcloud "$@"
      ;;
    get)
      rpath="${1}"
      # Check whether we have an absolute path like /foo, ~foo, or ~/foo.
      # If not, base the path relative to the CRDB repo.
      if [[ "${rpath:0:1}" != / && "${rpath:0:2}" != ~[/a-z] ]]; then
          rpath="go/src/github.com/cockroachdb/cockroach/${rpath}"
      fi
      from="${NAME}:${rpath}"
      shift
      gcloud compute scp --recurse "${from}" "$@"
      ;;
    put)
      # scp allows one or more sources, followed by a single destination. (With no
      # destination path we'll default to the home directory.)
      if (( $# < 1 )); then
        echo "usage: $0 put sourcepath [sourcepath...] destpath"
        echo "or:    $0 put sourcepath"
        exit 1
      elif (( $# == 1 )); then
        lpath="${1}"
        rpath="~"
      else
        lpath="${@:1:$#-1}"
        rpath="${@: -1}"
      fi
      to="${NAME}:${rpath}"
      gcloud compute scp --recurse "${lpath}" "${to}"
      ;;
    ip)
      gcloud compute instances describe --format="value(networkInterfaces[0].accessConfigs[0].natIP)" "${NAME}"
      ;;
    vscode)
      start_and_wait "${NAME}"
      HOST="$(gcloud compute ssh --dry-run "${NAME}" | awk '{print $NF}')"
      code --wait --remote "ssh-remote+$HOST" "$@"
      ;;
    status)
      gcloud compute instances describe "${NAME}" --format="table(name,status,lastStartTimestamp,lastStopTimestamp)"
      ;;
    *)
      echo "$0: unknown command: ${cmd}, use one of create, start, stop, resume, suspend, delete, status, ssh, get, put, or sync"
      exit 1
      ;;
esac
