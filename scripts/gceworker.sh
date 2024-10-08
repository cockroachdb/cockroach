#!/usr/bin/env bash

# Copyright 2016 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


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

function user_domain_suffix() {
  gcloud auth list --limit 1 --filter="status:ACTIVE account:@cockroachlabs.com" --format="value(account)" | sed 's/[@\.\-]/_/g'
}

function start_and_wait() {
    gcloud compute instances start "${1}"
    if [ -z "${GCEWORKER_NO_FIREWALL_WARNING-}" ]; then
	      cat <<EOF
	      Note: gceworkers are not to be exposed to the public internet[1].
	      At home or at an office, you may use the below to allowlist your current IP address:
	      $0 update-firewall.

        This warning can be suppressed via GCEWORKER_NO_FIREWALL_WARNING=true.

        [1]: https://cockroachlabs.slack.com/archives/C0HM2DZ34/p1719878745576399
EOF
    fi
    echo "waiting for node to finish starting..."
    # Wait for vm and sshd to start up.
    retry gcloud compute ssh "${1}" --command=true || true
}

function refresh_ssh_config() {
    IP=$($0 ip)
    if ! grep -q "${FQNAME}" ~/.ssh/config; then
      USER_DOMAIN_SUFFIX="$(user_domain_suffix)"
      echo "No alias found for ${FQNAME} in ~/.ssh/config. Creating one for ${USER_DOMAIN_SUFFIX} now with the instance external ip."
      echo "Host ${FQNAME}
  HostName ${IP}
  User ${USER_DOMAIN_SUFFIX}
  IdentityFile $HOME/.ssh/google_compute_engine
  UserKnownHostsFile=$HOME/.ssh/google_compute_known_hosts
  IdentitiesOnly=yes
  CheckHostIP=no" >> ~/.ssh/config
    else
      sed -i"" -e "/Host ${FQNAME}/,/HostName/ s/HostName .*/HostName ${IP}/" ~/.ssh/config
    fi
}

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

    gsuite_account_for_label="$(gcloud auth list | \
        grep '^\*' | \
        sed -e 's/\* *//' -e 's/@/__at__/g' -e 's/\./__dot__/g'\
        )"
    gcloud compute instances \
           create "${NAME}" \
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
           --metadata enable-oslogin=TRUE,block-project-ssh-keys=TRUE
    gcloud compute firewall-rules create "${NAME}-mosh" --allow udp:60000-61000

    # wait a bit to let gcloud create the instance before retrying
    sleep 30
    # Retry while vm and sshd start up.
    start_and_wait "${NAME}"

    gcloud compute scp --recurse "build/bootstrap" "${NAME}:bootstrap"
    gcloud compute ssh "${NAME}" --ssh-flag="-A" --command="./bootstrap/bootstrap-debian.sh"

    if [[ "$COCKROACH_DEV_LICENSE" ]]; then
      gcloud compute ssh "${NAME}" --command="echo COCKROACH_DEV_LICENSE=$COCKROACH_DEV_LICENSE >> ~/.bashrc_bootstrap"
    fi
    # https://cockroachlabs.slack.com/archives/C023S0V4YEB/p1725353536265029?thread_ts=1673575342.188709&cid=C023S0V4YEB
    gcloud compute ssh "${NAME}" --command="echo ROACHPROD_EMAIL_DOMAIN=developer.gserviceaccount.com >> ~/.bashrc_bootstrap"

    # Install automatic shutdown after ten minutes of operation without a
    # logged in user. To disable this, `sudo touch /.active`.
    gcloud compute ssh "${NAME}" --command="sudo cp bootstrap/autoshutdown.cron.sh /root/; echo '* * * * * /root/autoshutdown.cron.sh 10' | sudo crontab -i -"

    ;;
    update-firewall)
    MY_IP="$(curl -4 -s https://icanhazip.com/)"
    RULE="$(whoami)-home-ssh-rule"
    gcloud compute firewall-rules delete --quiet "$RULE" || true
    gcloud compute firewall-rules create --quiet "$RULE" \
      --network=default \
      --allow=tcp:22 \
      --source-ranges="$MY_IP/32" \
      --direction=INGRESS \
      --priority=0
    ;;
    start)
    start_and_wait "${NAME}"
    refresh_ssh_config

    # SSH into the node, since that's probably why we started it.
    echo "****************************************"
    echo "Hint: you should also be able to directly invoke:"
    echo "ssh ${FQNAME}"
    echo "  or"
    echo "mosh ${FQNAME}"
    echo "if needed instead of '$0 (ssh|mosh)'."
    echo "If this does not work, try removing the section for your gceworker from ~/.ssh/config"
    echo "and invoke '$0 start' again to recreate it."
    echo
    if [ -z "${GCEWORKER_START_SSH_COMMAND-}" ]; then
	echo "Connecting via SSH."
	echo "Set GCEWORKER_START_SSH_COMMAND=mosh to use mosh instead"
    fi
    echo "****************************************"
    $0 ${GCEWORKER_START_SSH_COMMAND-ssh}
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
    suspend)
    read -r -p "This will pause the VM. Are you sure? [yes] " response
    response=$(echo "$response" | tr '[:upper:]' '[:lower:]')
    if [[ $response != "yes" ]]; then
      echo Aborting
      exit 1
    fi
    gcloud compute instances suspend "${NAME}"
    ;;
    resume)
    gcloud compute instances resume "${NAME}"
    echo "waiting for node to finish starting..."
    # Wait for vm and sshd to start up.
    retry gcloud compute ssh "${NAME}" --command=true || true
    refresh_ssh_config
    # SSH into the node, since that's probably why we resumed it.
    $0 ssh
    ;;
    reset)
    read -r -p "This will hard reset (\"powercycle\") the VM. Are you sure? [yes] " response
    # Convert to lowercase.
    response=$(echo "$response" | tr '[:upper:]' '[:lower:]')
    if [[ $response != "yes" ]]; then
      echo Aborting
      exit 1
    fi
    gcloud compute instances reset "${NAME}"
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
      mosh "$(user_domain_suffix)@${FQNAME}"
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
    gcloud compute scp --recurse ${lpath} "${to}"
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
    unison "$host" "ssh://${NAME}.${CLOUDSDK_COMPUTE_ZONE}.${CLOUDSDK_CORE_PROJECT}/$worker" \
      -sshargs "-F ${tmpfile}" -auto -prefer "$host" -repeat watch \
      -ignore 'Path .localcluster.certs*' \
      -ignore 'Path .git' \
      -ignore 'Path _bazel*' \
      -ignore 'Path bazel-out*' \
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
    vscode)
    start_and_wait "${NAME}"
    HOST=$(gcloud compute ssh --dry-run ${NAME} | awk '{print $NF}')
    code --wait --remote ssh-remote+$HOST "$@"
    ;;
    status)
    gcloud compute instances describe ${NAME} --format="table(name,status,lastStartTimestamp,lastStopTimestamp)"
    ;;
    *)
    echo "$0: unknown command: ${cmd}, use one of create, start, stop, resume, suspend, delete, status, ssh, get, put, or sync"
    exit 1
    ;;
esac
