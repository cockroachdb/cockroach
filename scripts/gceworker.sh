#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

cd "$(dirname "${0}")/.."
source build/shlib.sh

export CLOUDSDK_CORE_PROJECT=${CLOUDSDK_CORE_PROJECT-${GCEWORKER_PROJECT-cockroach-workers}}
export CLOUDSDK_COMPUTE_ZONE=${GCEWORKER_ZONE-${CLOUDSDK_COMPUTE_ZONE-us-east1-b}}

USER_ID=$(id -un)
NAME=${GCEWORKER_NAME-gceworker-${USER_ID//./}}
FQNAME="${NAME}.${CLOUDSDK_COMPUTE_ZONE}.${CLOUDSDK_CORE_PROJECT}"

# Derive the region from the zone by stripping the trailing zone letter.
REGION="${CLOUDSDK_COMPUTE_ZONE%-*}"

# IMAGE_FAMILY can be used to override the image when creating a gceworker.
# For example:
#   IMAGE_FAMILY=ubuntu-2410-amd64 scripts/gceworker.sh create
#
# Note that ubuntu-2404-lts-amd64 is the only image that we know will consistently
# work with respect to our build or scripts.
IMAGE_FAMILY=${IMAGE_FAMILY-ubuntu-2404-lts-amd64}

cmd=${1-}
if [[ "${cmd}" ]]; then
	shift
fi

function get_ip() {
	gcloud compute instances describe --format="value(networkInterfaces[0].accessConfigs[0].natIP)" "${NAME}"
}

function user_domain_suffix() {
	# Trim the account name to 32 characters to respect the Linux username length limit.
	gcloud auth list --limit 1 --filter="status:ACTIVE account:@cockroachlabs.com" --format="value(account)" | sed 's/[@\.\-]/_/g' | cut -c1-32
}

function gcloud_compute_ssh() {
	gcloud compute ssh --tunnel-through-iap --ssh-flag="-o StrictHostKeyChecking=no" --ssh-flag="-o UserKnownHostsFile=/dev/null" "$@"
}

function start_and_wait() {
	gcloud compute instances start "${1}"
	echo "waiting for node to finish starting..."
	# Wait for vm and sshd to start up.
	retry gcloud_compute_ssh "${1}" --command=true || true
}

function refresh_ssh_config() {
	# Remove any stale entry for this instance.
	if grep -q "${FQNAME}" ~/.ssh/config 2>/dev/null; then
		sed -i"" -e "/Host ${FQNAME}/,/^$/d" ~/.ssh/config
	fi
	USER_DOMAIN_SUFFIX="$(user_domain_suffix)"
	echo "Writing SSH config entry for ${FQNAME} with IAP tunnel proxy."
	echo "
Host ${FQNAME}
  HostName ${NAME}
  User ${USER_DOMAIN_SUFFIX}
  ProxyCommand gcloud compute start-iap-tunnel %h %p --listen-on-stdin --zone=${CLOUDSDK_COMPUTE_ZONE} --project=${CLOUDSDK_CORE_PROJECT} --quiet
  IdentityFile $HOME/.ssh/google_compute_engine
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null" >>~/.ssh/config
}

function grant_ssh_access() {
	local user_email
	user_email=$(gcloud config get-value account 2>/dev/null)
	if [[ -z "${user_email}" || "${user_email}" == "(unset)" ]]; then
		echo "Error: no active gcloud account found. Run 'gcloud auth login' first." >&2
		exit 1
	fi
	echo "Granting OS Login access to ${user_email} on ${NAME}..."
	gcloud compute instances add-iam-policy-binding "${NAME}" \
		--zone="${CLOUDSDK_COMPUTE_ZONE}" \
		--member="user:${user_email}" \
		--role="roles/compute.osAdminLogin" \
		--project="${CLOUDSDK_CORE_PROJECT}" \
		--quiet
}

function ensure_created_with_label() {
	local current
	current=$(gcloud compute instances describe "${NAME}" --format="value(labels.created-with)" 2>/dev/null)
	if [[ -z "${current}" ]]; then
		gcloud compute instances add-labels "${NAME}" --labels="created-with=gceworker-legacy" > /dev/null
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
		read -r COCKROACH_DEV_LICENSE
	fi

	gsuite_account_for_label="$(
		gcloud auth list |
			grep '^\*' |
			sed -e 's/\* *//' -e 's/@/__at__/g' -e 's/\./__dot__/g'
	)"
	gcloud compute instances \
		create "${NAME}" \
		--machine-type "n2-custom-24-32768" \
		--network "cockroach-workers-vpc" \
		--subnet "cockroach-workers-vpc-${REGION}" \
		--tags "gceworker" \
		--maintenance-policy "MIGRATE" \
		--image-project "ubuntu-os-cloud" \
		--image-family "${IMAGE_FAMILY}" \
		--boot-disk-size "250" \
		--boot-disk-type "pd-ssd" \
		--boot-disk-device-name "${NAME}" \
		--service-account "cockroach-worker@cockroach-workers.iam.gserviceaccount.com" \
		--scopes "cloud-platform" \
		--labels "created-by=${gsuite_account_for_label:0:63},created-with=gceworker" \
		--metadata enable-oslogin=TRUE,block-project-ssh-keys=TRUE

	grant_ssh_access

	# wait a bit to let gcloud create the instance before retrying
	sleep 30
	# Retry while vm and sshd start up.
	start_and_wait "${NAME}"

	gcloud compute scp --tunnel-through-iap --recurse "build/bootstrap" "${NAME}:bootstrap"
	gcloud_compute_ssh "${NAME}" --ssh-flag="-A" --command="./bootstrap/bootstrap-debian.sh"

	if [[ "$COCKROACH_DEV_LICENSE" ]]; then
		gcloud_compute_ssh "${NAME}" --command="echo COCKROACH_DEV_LICENSE=$COCKROACH_DEV_LICENSE >> ~/.bashrc_bootstrap"
	fi
	# https://cockroachlabs.slack.com/archives/C023S0V4YEB/p1725353536265029?thread_ts=1673575342.188709&cid=C023S0V4YEB
	gcloud_compute_ssh "${NAME}" --command="echo ROACHPROD_EMAIL_DOMAIN=developer.gserviceaccount.com >> ~/.bashrc_bootstrap"

	# Install automatic shutdown after ten minutes of operation without a
	# logged in user. To disable this, `sudo touch /.active`.
	gcloud_compute_ssh "${NAME}" --command="sudo cp bootstrap/autoshutdown.cron.sh /root/; echo '* * * * * /root/autoshutdown.cron.sh 10' | sudo crontab -i -"

	;;
start)
	ensure_created_with_label
	start_and_wait "${NAME}"
	refresh_ssh_config

	# SSH into the node, since that's probably why we started it.
	echo "****************************************"
	echo "Hint: you can also connect directly via:"
	echo "gcloud compute ssh ${NAME} --tunnel-through-iap"
	echo "if needed instead of '$0 ssh'."
	echo "****************************************"
	$0 ssh
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
	ensure_created_with_label
	gcloud compute instances resume "${NAME}"
	# This conveniently waits until the VM is ready and then drops
	# us into a ssh session.
	$0 start
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
delete | destroy)
	read -r -p "This will delete the VM! Are you sure? [yes] " response
	# Convert to lowercase.
	response=$(echo "$response" | tr '[:upper:]' '[:lower:]')
	if [[ $response != "yes" ]]; then
		echo Aborting
		exit 1
	fi
	gcloud compute instances delete "${NAME}" --quiet
	;;
ssh)
	ensure_created_with_label
	gcloud_compute_ssh "${NAME}" --ssh-flag="-A" "$@"
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
	gcloud compute scp --tunnel-through-iap --recurse "${from}" "$@"
	;;
put)
	# scp allows one or more sources, followed by a single destination. (With no
	# destination path we'll default to the home directory.)
	if (($# < 1)); then
		echo "usage: $0 put sourcepath [sourcepath...] destpath"
		echo "or:    $0 put sourcepath"
		exit 1
	elif (($# == 1)); then
		lpaths=("${1}")
		rpath="~"
	else
		lpaths=("${@:1:$#-1}")
		rpath="${@: -1}"
	fi
	to="${NAME}:${rpath}"
	gcloud compute scp --tunnel-through-iap --recurse "${lpaths[@]}" "${to}"
	;;
ip)
	get_ip
	;;
vscode)
	start_and_wait "${NAME}"
	refresh_ssh_config
	code --wait --remote ssh-remote+"${FQNAME}" "$@"
	;;
status)
	gcloud compute instances describe "$NAME" --format="table(name,status,lastStartTimestamp,lastStopTimestamp)"
	;;
update-hosts)
	NEW_IP="$(get_ip)"
	HOSTS_FILE="/etc/hosts"

	# Step 1: Remove any existing gceworker line.
	sudo sed -i '' "/${NAME}\.local/d" "${HOSTS_FILE}"
	# Step 2: Insert the new line at the end unconditionally
	echo "${NEW_IP} ${NAME}.local" | sudo tee -a ${HOSTS_FILE} > /dev/null
	;;
migrate)
	# Verify the VM exists.
	if ! gcloud compute instances describe "${NAME}" --zone="${CLOUDSDK_COMPUTE_ZONE}" --project="${CLOUDSDK_CORE_PROJECT}" > /dev/null 2>&1; then
		echo "Error: VM ${NAME} does not exist." >&2
		exit 1
	fi

	# Check current network.
	current_network=$(gcloud compute instances describe "${NAME}" \
		--zone="${CLOUDSDK_COMPUTE_ZONE}" \
		--project="${CLOUDSDK_CORE_PROJECT}" \
		--format="value(networkInterfaces[0].network)")
	if [[ "${current_network}" == *"cockroach-workers-vpc"* ]]; then
		echo "VM ${NAME} is already on cockroach-workers-vpc. Nothing to do."
		exit 0
	fi

	echo "This will migrate ${NAME} to the cockroach-workers-vpc network."
	echo ""
	echo "  The following changes will be made:"
	echo "    - Stop the VM (if running)"
	echo "    - Update NIC to cockroach-workers-vpc / cockroach-workers-vpc-${REGION}"
	echo "    - Apply created-with=gceworker-legacy label"
	echo "    - Grant OS Login admin access"
	echo "    - Update SSH config for IAP tunneling"
	echo ""
	read -r -p "Are you sure? [yes] " response
	response=$(echo "$response" | tr '[:upper:]' '[:lower:]')
	if [[ $response != "yes" ]]; then
		echo Aborting
		exit 1
	fi

	# Stop the VM if it's not already stopped.
	vm_status=$(gcloud compute instances describe "${NAME}" \
		--zone="${CLOUDSDK_COMPUTE_ZONE}" \
		--project="${CLOUDSDK_CORE_PROJECT}" \
		--format="value(status)")
	if [[ "${vm_status}" == "RUNNING" || "${vm_status}" == "SUSPENDED" ]]; then
		echo "VM is ${vm_status}, stopping..."
		gcloud compute instances stop "${NAME}" \
			--zone="${CLOUDSDK_COMPUTE_ZONE}" \
			--project="${CLOUDSDK_CORE_PROJECT}"
	else
		echo "VM is already ${vm_status}, skipping stop."
	fi

	echo "Updating network interface to cockroach-workers-vpc..."
	gcloud compute instances network-interfaces update "${NAME}" \
		--zone="${CLOUDSDK_COMPUTE_ZONE}" \
		--project="${CLOUDSDK_CORE_PROJECT}" \
		--network="cockroach-workers-vpc" \
		--subnetwork="cockroach-workers-vpc-${REGION}" \
		--network-interface="nic0"

	ensure_created_with_label
	grant_ssh_access
	refresh_ssh_config

	echo ""
	echo "Migration complete. Run '$0 start' to start the VM on the new network."
	;;
*)
	echo "$0: unknown command: ${cmd}, use one of create, start, stop, resume, suspend, reset, delete, status, ssh, get, put, ip, vscode, update-hosts, migrate, or gcloud"
	exit 1
	;;
esac
