#!/usr/bin/env bash
#
# On a Debian/Ubuntu system, disables the Hyper-V time sync, which can cause
# backward time jumps.
# Note that by default (at least with the canonical image for Ubuntu on Azure),
# NTP is already enabled (via systemd-timesyncd).

# According to lsvmbus, the time sync class ID is:
TIME_SYNC_CLASS_ID="{9527e630-d0ae-497b-adce-e80ab0175caf}"

# hv bus devices are all listed under:
VMBUS_DIR="/sys/bus/vmbus/devices"

# Each device folder contains many plaintext files, including:
CLASS_ID_FILE="class_id"

# The device can be unbound by writing its ID to:
UNBIND_PATH="/sys/bus/vmbus/drivers/hv_util/unbind"

full_path=$(grep ${TIME_SYNC_CLASS_ID} ${VMBUS_DIR}/*/${CLASS_ID_FILE})
if [ "$?" -ne "0" ]; then
  echo "Time sync device not found"
  exit 1
fi
dev_id="$(echo ${full_path} | cut -d"/" -f6)"
echo -n "${dev_id}" | sudo tee "${UNBIND_PATH}" > /dev/null
if [ "$?" -ne "0" ]; then
  echo "Time sync device already unbound, or error encountered."
  exit 1
else
  echo "Time sync disabled."
  exit 0
fi
