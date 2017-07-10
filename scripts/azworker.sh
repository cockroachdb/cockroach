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
        --image-urn canonical:UbuntuServer:16.04.0-LTS:latest \
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

    # TODO(bdarnell): autoshutdown.cron.sh does not work on azure. It
    # halts the VM, but halting the VM doesn't stop billing. The VM
    # must instead be "deallocated" with the azure API.
    # Install automatic shutdown after ten minutes of operation without a
    # logged in user. To disable this, `sudo touch /.active`.
    #ssh "${USER}@${FQDN}" "sudo cp bootstrap/autoshutdown.cron.sh /root/; echo '* * * * * /root/autoshutdown.cron.sh 10' | sudo crontab -i -"

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
    if [ $NUM_VMS -gt 1 ]; then
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
    *)
    echo "$0: unknown command: ${1-}, use one of create, start, stop, delete, or ssh"
    exit 1
    ;;
esac
