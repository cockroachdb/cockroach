#!/usr/bin/env bash

set -euxo pipefail

# Disabling hyper-v time synchronization means unbinding the device.
# To find the device, one can use:
# $ curl -O https://raw.githubusercontent.com/torvalds/linux/master/tools/hv/lsvmbus
# $ python lsvmbus -vv | grep -w "Time Synchronization" -A 3
#   VMBUS ID 11: Class_ID = {9527e630-d0ae-497b-adce-e80ab0175caf} - [Time Synchronization]
#     Device_ID = {2dd1ce17-079e-403c-b352-a1921ee207ee}
#     Sysfs path: /sys/bus/vmbus/devices/2dd1ce17-079e-403c-b352-a1921ee207ee
#     Rel_ID=11, target_cpu=0
# echo 2dd1ce17-079e-403c-b352-a1921ee207ee | sudo tee /sys/bus/vmbus/drivers/hv_util/unbind

# According to lsvmbus, the time sync class ID is:
TIME_SYNC_CLASS_ID='{9527e630-d0ae-497b-adce-e80ab0175caf}'

# hv bus devices are all listed under:
VMBUS_DIR=/sys/bus/vmbus/devices

# Each device folder contains many plaintext files, including:
CLASS_ID_FILE=class_id

# The device can be unbound by writing its ID to:
UNBIND_PATH=/sys/bus/vmbus/drivers/hv_util/unbind

if ! full_path=$(grep ${TIME_SYNC_CLASS_ID} ${VMBUS_DIR}/*/${CLASS_ID_FILE}); then
  echo 'Time sync device not found'
  exit 1
fi
dev_id=$(echo "${full_path}" | cut -d/ -f6)

if ! echo -n "${dev_id}" | sudo tee ${UNBIND_PATH} > /dev/null; then
  echo 'Time sync device already unbound, or error encountered.'
fi

# Force an NTP time sync.
sudo apt-get -qqy update
sudo apt-get -qqy install ntpdate
# Disable systemd-timesyncd
sudo timedatectl set-ntp false
if systemctl is-active -q ntp.service; then
  sudo service ntp stop
  sudo ntpdate -b ntp.ubuntu.com
  sudo service ntp start
else
  sudo ntpdate -b ntp.ubuntu.com
  # Reenable systemd-timesyncd.
  sudo timedatectl set-ntp true
fi
