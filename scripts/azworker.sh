#!/usr/bin/env bash
#
# Prerequisites:
# - Cockroach Labs employees: ask an admin to create an Azure account for you.
# - Install the Azure XPlat CLI: https://github.com/Azure/azure-xplat-cli
# - Run "azure login" and sign in to your Azure account.

set -euo pipefail

LOCATION=${LOCATION-eastus}
MACHINE_SIZE=${MACHINE_SIZE-Standard_F16}
USER=${USER-$(id -un)}
CLUSTER=azworker-${USER}
NAME=${AZWORKER_NAME-${CLUSTER}}

# Names for various resources just reuse cluster/vm name depending on scope.
RG=${CLUSTER}
NET=${CLUSTER}
SUBNET=${CLUSTER}
NIC=${NAME}
IP=${NAME}
DOMAIN=cockroach-${NAME}
FQDN=${DOMAIN}.${LOCATION}.cloudapp.azure.com

# TODO(radu): switch to the newer "az" CLI.

case ${1-} in
  create)
    if azure group show "${RG}" >/dev/null 2>/dev/null; then
      echo "Resource group ${RG} already exists; adding worker VM to it"
    else
      echo "Creating resource group ${RG}"
      azure group create "${RG}" -l "${LOCATION}"
      azure network vnet create -g "${RG}" -n "${NET}" -a 192.168.0.0/16 -l "${LOCATION}"
      azure network vnet subnet create -g "${RG}" -e "${NET}" -n "${SUBNET}" -a 192.168.1.0/24
    fi

    azure network public-ip create -g "${RG}" -n "${IP}" -l "${LOCATION}" -d "${DOMAIN}"
    azure network nic create -g "${RG}" -n "${NIC}" -l "${LOCATION}" -p "${IP}" --subnet-name "${SUBNET}" --subnet-vnet-name "${NET}"

    azure vm create \
        --resource-group "${RG}" \
        --name "${NAME}" \
        --location "${LOCATION}" \
        --os-type linux \
        --image-urn canonical:UbuntuServer:16.04-LTS:latest \
        --ssh-publickey-file ~/.ssh/id_rsa.pub \
        --admin-username "${USER}" \
        --vm-size "${MACHINE_SIZE}" \
        --nic-name "${NIC}" \
        --vnet-name "${NET}" \
        --vnet-subnet-name "${SUBNET}"

    # Clear any cached host keys for this hostname and accept the new one.
    ssh-keygen -R "${FQDN}"
    # Retry while vm and sshd to start up.
    "$(dirname "${0}")/travis_retry.sh" ssh -o StrictHostKeyChecking=no "${USER}@${FQDN}" true

    rsync -az "$(dirname "${0}")/../build/bootstrap/" "${USER}@${FQDN}:bootstrap/"
    rsync -az "$(dirname "${0}")/../build/parallelbuilds-"* "${USER}@${FQDN}:bootstrap/"
    rsync -az "$(dirname "${0}")/../build/disable-hyperv-timesync.sh" "${USER}@${FQDN}:bootstrap/"
    ssh -A "${USER}@${FQDN}" ./bootstrap/bootstrap-debian.sh
    ssh -A "${USER}@${FQDN}" ./bootstrap/disable-hyperv-timesync.sh
    ssh -A "${USER}@${FQDN}" ./bootstrap/install-azure-cli.sh

    # Copy the azure config (including credentials!).
    ssh -A "${USER}@${FQDN}" "mkdir .azure"
    rsync -az ~/.azure/*.json "${USER}@${FQDN}:.azure/"

    # Set up tmux configuration (for persistent SSH).
    if [ -e ~/.tmux.conf ]; then
      rsync -azL ~/.tmux.conf "${USER}@${FQDN}:./"
    else
      # Present a color terminal and disable tmux scrollback.
      ssh -A "${USER}@${FQDN}" "echo 'set -g default-terminal screen-256color' > .tmux.conf"
      ssh -A "${USER}@${FQDN}" "echo 'set -g terminal-overrides \"xterm*:XT:smcup@:rmcup@\"' >> .tmux.conf"
    fi

    # shellcheck disable=SC2029
    ssh "${USER}@${FQDN}" "echo '* * * * * /home/radu/bootstrap/autoshutdown.cron.sh 10 az vm deallocate --resource-group \"${RG}\" --name \"${NAME}\"' | crontab -"

    echo "VM now running at ${FQDN}"
    ;;
  start)
    azure vm start "${RG}" "${NAME}"
    ;;
  stop)
    azure vm deallocate "${RG}" "${NAME}"
    ;;
  delete)
    # The straightforward thing to do would be to first delete the VM, then
    # check if there are any virtual machines left in the group. However, the
    # deleted VM doesn't disappear right away from the listing. So we instead
    # count the initial number of VMs.
    NUM_VMS=$(azure group show "${RG}" | (grep -c "Type *: *virtualMachines" || true))
    azure vm delete "${RG}" "${NAME}"
    if [ "$NUM_VMS" -gt 1 ]; then
      azure network nic delete -g "${RG}" -n "${NIC}" -q
      azure network public-ip delete -g "${RG}" -n "${IP}" -q
      echo "Resource group ${RG} still contains VMs; leaving in place"
    else
      echo "Deleting resource group ${RG}"
      azure group delete "${RG}" -q
    fi
    ;;
  ssh)
    shift
    # shellcheck disable=SC2029
    ssh -A "${USER}@${FQDN}" "$@"
    ;;
  sshmux)
    shift
    if [ -z "${1:-}" ]; then
      ssh -A "${USER}@${FQDN}" "tmux list-sessions"
    else
      # shellcheck disable=SC2029
      ssh -A -t "${USER}@${FQDN}" "tmux attach -t $1 || tmux new-session -s $1"
    fi
    ;;
  *)
    if [ -n "${1:-}" ]; then
      echo "$0: unknown command: ${1-}"
    fi
    cat <<EOF
Usage:
  $0 create
     Creates a new azure worker VM.

  $0 start
     Powers on an azure worker VM.

  $0 stop
     Powers off an azure worker VM.

  $0 delete
     Deletes an azure worker VM.

  $0 ssh
     SSH into an azure worker VM.

  $0 sshmux <session-name>
     Creates or reconnects to a persistent SSH session with the given name.

  $0 sshmux
     List persistent SSH sessions.

For all commands, worker VM name can be customized via AZWORKER_NAME.
EOF
    exit 1
    ;;
esac
