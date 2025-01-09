// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package azure

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"text/template"
)

// Startup script used to find/format/mount all local or attached disks.
// Each disk is mounted as /data<disknum>, and, in addition, a symlink
// created from /mnt/data<disknum> to the mount point.
// azureStartupArgs specifies template arguments for the setup template.
type azureStartupArgs struct {
	RemoteUser           string // The uname for /data* directories.
	AttachedDiskLun      *int   // Use attached disk, with specified LUN; Use local ssd if nil.
	DisksInitializedFile string // File to touch when disks are initialized.
	OSInitializedFile    string // File to touch when OS is initialized.
	StartupLogs          string // File to redirect startup script output logs.
	DiskControllerNVMe   bool   // Interface data disk via NVMe
}

const azureStartupTemplate = `#!/bin/bash

# Script for setting up a Azure machine for roachprod use.
# ensure any failure fails the entire script
set -eux

# Redirect output to stdout/err and a log file
exec &> >(tee -a {{ .StartupLogs }})

# Log the startup of the script with a timestamp
echo "startup script starting: $(date -u)"
mount_opts="defaults"

devices=()
{{if .DiskControllerNVMe}}
# Setup nvme network storage, need to remove nvme OS disk from the device list.
devices=($(realpath -qe /dev/disk/by-id/nvme-* | grep -v "nvme0n1" | sort -u))
{{else if .AttachedDiskLun}}
# Setup network attached storage
devices=("/dev/disk/azure/scsi1/lun{{.AttachedDiskLun}}")
{{end}}

if (( ${#devices[@]} == 0 ));
then
  # Use /mnt directly.
  echo "No attached or NVME disks found, creating /mnt/data1"
  mkdir -p /mnt/data1
  chown {{.RemoteUser}} /mnt/data1
else
  for d in "${!devices[@]}"; do
    disk=${devices[$d]}
    mount="/data$((d+1))"
    sudo mkdir -p "${mount}"
    sudo mkfs.ext4 -F "${disk}"
    sudo mount -o "${mount_opts}" "${disk}" "${mount}"
    echo "${disk} ${mount} ext4 ${mount_opts} 1 1" | sudo tee -a /etc/fstab
    ln -s "${mount}" "/mnt/$(basename $mount)"
    tune2fs -m 0 ${disk}
  done
  chown {{.RemoteUser}} /data*
fi

# increase the number of concurrent unauthenticated connections to the sshd
# daemon. See https://en.wikibooks.org/wiki/OpenSSH/Cookbook/Load_Balancing.
# By default, only 10 unauthenticated connections are permitted before sshd
# starts randomly dropping connections.
sh -c 'echo "MaxStartups 64:30:128" >> /etc/ssh/sshd_config'
# Crank up the logging for issues such as:
# https://github.com/cockroachdb/cockroach/issues/36929
sed -i'' 's/LogLevel.*$/LogLevel DEBUG3/' /etc/ssh/sshd_config
# N.B. RSA SHA1 is no longer supported in the latest versions of OpenSSH. Existing tooling, e.g.,
# jepsen still relies on it for authentication.
sudo sh -c 'echo "PubkeyAcceptedAlgorithms +ssh-rsa" >> /etc/ssh/sshd_config'
service sshd restart
# increase the default maximum number of open file descriptors for
# root and non-root users. Load generators running a lot of concurrent
# workers bump into this often.
sh -c 'echo "root - nofile 1048576\n* - nofile 1048576" > /etc/security/limits.d/10-roachprod-nofiles.conf'

# Send TCP keepalives every minute since GCE will terminate idle connections
# after 10m. Note that keepalives still need to be requested by the application
# with the SO_KEEPALIVE socket option.
cat <<EOF > /etc/sysctl.d/99-roachprod-tcp-keepalive.conf
net.ipv4.tcp_keepalive_time=60
net.ipv4.tcp_keepalive_intvl=60
net.ipv4.tcp_keepalive_probes=5
EOF

# N.B. Ubuntu 22.04 changed the location of tcpdump to /usr/bin. Since existing tooling, e.g.,
# jepsen uses /usr/sbin, we create a symlink.
# See https://ubuntu.pkgs.org/22.04/ubuntu-main-amd64/tcpdump_4.99.1-3build2_amd64.deb.html
sudo ln -s /usr/bin/tcpdump /usr/sbin/tcpdump

# Uninstall unattended-upgrades
systemctl stop unattended-upgrades
sudo rm -rf /var/log/unattended-upgrades
apt-get purge -y unattended-upgrades

# Disable Hyper-V Time Synchronization device
# See https://www.cockroachlabs.com/docs/stable/deploy-cockroachdb-on-microsoft-azure#step-3-synchronize-clocks
curl -O https://raw.githubusercontent.com/torvalds/linux/master/tools/hv/lsvmbus
ts_dev_id=$(python3 lsvmbus -vv | grep -w "Time Synchronization" -A 3 | grep "Device_ID" | awk -F '[{}]' '{print $2}')
echo $ts_dev_id | sudo tee /sys/bus/vmbus/drivers/hv_utils/unbind

# Override the chrony config. In particular,
# log aggressively when clock is adjusted (0.01s)
# and exclusively use a single time server.
sudo cat <<EOF > /etc/chrony/chrony.conf
keyfile /etc/chrony/chrony.keys
commandkey 1
driftfile /var/lib/chrony/chrony.drift
log tracking measurements statistics
logdir /var/log/chrony
maxupdateskew 100.0
dumponexit
dumpdir /var/lib/chrony
logchange 0.01
hwclockfile /etc/adjtime
rtcsync
server time1.google.com iburst
server time2.google.com iburst
server time3.google.com iburst
server time4.google.com iburst
makestep 0.1 3
EOF

sudo /etc/init.d/chrony restart
sudo chronyc -a waitsync 30 0.01 | sudo tee -a /root/chrony.log

# Enable core dumps
cat <<EOF > /etc/security/limits.d/core_unlimited.conf
* soft core unlimited
* hard core unlimited
root soft core unlimited
root hard core unlimited
EOF

mkdir -p /mnt/data1/cores
chmod a+w /mnt/data1/cores
CORE_PATTERN="/mnt/data1/cores/core.%e.%p.%h.%t"
echo "$CORE_PATTERN" > /proc/sys/kernel/core_pattern
sed -i'~' 's/enabled=1/enabled=0/' /etc/default/apport
sed -i'~' '/.*kernel\\.core_pattern.*/c\\' /etc/sysctl.conf
echo "kernel.core_pattern=$CORE_PATTERN" >> /etc/sysctl.conf
sysctl --system  # reload sysctl settings

sudo sed -i 's/#LoginGraceTime .*/LoginGraceTime 0/g' /etc/ssh/sshd_config
sudo service ssh restart

touch {{ .DisksInitializedFile }}
touch {{ .OSInitializedFile }}
`

// evalStartupTemplate evaluates startup template defined above and returns
// a cloud-init base64 encoded custom data used to configure VM.
//
// Errors in startup files are hard to debug.  If roachprod create does not complete,
// CTRL-c while roachprod waiting for initialization to complete (otherwise, roachprod
// tries to destroy partially created cluster).
// Then, ssh to one of the machines:
//  1. /var/log/cloud-init-output.log contains the output of all the steps
//     performed by cloud-init, including the steps performed by above script.
//  2. You can extract uploaded script and try executing/debugging it via:
//     sudo cloud-init query userdata > script.sh
func evalStartupTemplate(args azureStartupArgs) (string, error) {
	cloudInit := bytes.NewBuffer(nil)
	encoder := base64.NewEncoder(base64.StdEncoding, cloudInit)
	gz := gzip.NewWriter(encoder)
	t := template.Must(template.New("start").Parse(azureStartupTemplate))
	if err := t.Execute(gz, args); err != nil {
		return "", err
	}
	if err := gz.Close(); err != nil {
		return "", err
	}
	if err := encoder.Close(); err != nil {
		return "", err
	}
	return cloudInit.String(), nil
}
