#!/usr/bin/env bash
#
# Prerequisites:
# - Cockroach Labs employees: ask an admin to create an Azure account for you.
# - Install the Azure XPlat CLI 2.0:
#   https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest
# - Run "az login" and sign in to your Azure account.

set -euo pipefail

cd "$(dirname "${0}")/.." && source build/shlib.sh

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
    if [ $(az group show -n "${RG}" | wc -l) -gt 0 ]; then
      echo "Resource group ${RG} already exists; adding worker VM to it"
    else
      echo "Creating resource group ${RG}"
      az group create -n "${RG}" -l "${LOCATION}"
      az network vnet create -g "${RG}" -n "${NET}" --address-prefixes 192.168.0.0/16 -l "${LOCATION}"
      az network vnet subnet create -g "${RG}" --vnet-name "${NET}" -n "${SUBNET}" --address-prefix 192.168.1.0/24
    fi

    az network public-ip create -g "${RG}" -n "${IP}" -l "${LOCATION}" --dns-name "${DOMAIN}"
    az network nic create -g "${RG}" -n "${NIC}" -l "${LOCATION}" --public-ip-address "${IP}" --subnet "${SUBNET}" --vnet-name "${NET}"

    az vm create \
        --resource-group "${RG}" \
        --name "${NAME}" \
        --location "${LOCATION}" \
        --image canonical:UbuntuServer:16.04-LTS:latest \
        --ssh-key-value ~/.ssh/id_rsa.pub \
        --admin-username "${USER}" \
        --size "${MACHINE_SIZE}" \
        --nics "${NIC}"

    # Clear any cached host keys for this hostname and accept the new one.
    ssh-keygen -R "${FQDN}"
    # Retry while vm and sshd to start up.
    retry ssh -o StrictHostKeyChecking=no "${USER}@${FQDN}" true

    rsync -az "build/bootstrap/" "${USER}@${FQDN}:bootstrap/"
    rsync -az "build/disable-hyperv-timesync.sh" "${USER}@${FQDN}:bootstrap/"
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
    ssh "${USER}@${FQDN}" "echo '* * * * * /home/${USER}/bootstrap/autoshutdown.cron.sh 10 az vm deallocate --resource-group \"${RG}\" --name \"${NAME}\"' | crontab -"

    echo "VM now running at ${FQDN}"
    ;;
  start)
    az vm start -g "${RG}" -n "${NAME}"
    ;;
  stop)
    az vm deallocate -g "${RG}" -n "${NAME}"
    ;;
  delete|destroy)
    # The straightforward thing to do would be to first delete the VM, then
    # check if there are any virtual machines left in the group. However, the
    # deleted VM doesn't disappear right away from the listing. So we instead
    # count the initial number of VMs.
    NUM_VMS=$(az vm list -g "${RG}" | (grep -c "\"type\":.*virtualMachines" || true))
    az vm delete -g "${RG}" -n "${NAME}"
    if [ "$NUM_VMS" -gt 1 ]; then
      az network nic delete -g "${RG}" -n "${NIC}"
      az network public-ip delete -g "${RG}" -n "${IP}"
      echo "Resource group ${RG} still contains VMs; leaving in place"
    else
      echo "Deleting resource group ${RG}"
      az group delete -n "${RG}" --yes
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
