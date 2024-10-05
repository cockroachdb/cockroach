// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aws

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
)

// Both M5 and I3 machines expose their EBS or local SSD volumes as NVMe block
// devices, but the actual device numbers vary a bit between the two types.
// This user-data script will create a filesystem, mount the data volume, and
// chmod 777.
// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/nvme-ebs-volumes.html
//
// This is a template because the instantiator needs to optionally configure the
// mounting options. The script cannot take arguments since it is to be invoked
// by the aws tool which cannot pass args.
const awsStartupScriptTemplate = `#!/usr/bin/env bash
# Script for setting up a AWS machine for roachprod use.

set -x

if [ -e {{ .DisksInitializedFile }} ]; then
  echo "Already initialized, exiting."
  exit 0
fi

sudo apt-get update
sudo apt-get install -qy --no-install-recommends mdadm

{{ if not .Zfs }}
mount_opts="defaults,nofail"
{{if .ExtraMountOpts}}mount_opts="${mount_opts},{{.ExtraMountOpts}}"{{end}}
{{ end }}

use_multiple_disks='{{if .UseMultipleDisks}}true{{end}}'

mount_prefix="/mnt/data"

# if the use_multiple_disks is not set and there are more than 1 disk (excluding the boot disk),
# then the disks will be selected for RAID'ing. If there are both EC2 NVMe Instance Storage and
# EBS, RAID'ing in this case can cause performance differences. So, to avoid this,
# EC2 NVMe Instance Storage are ignored.
# Scenarios: 
#   (local SSD = 0, Network Disk - 1)  - no RAID'ing and mount network disk
#   (local SSD = 1, Network Disk - 0)  - no RAID'ing and mount local SSD
#   (local SSD >= 1, Network Disk = 1) - no RAID'ing and mount network disk
#   (local SSD > 1, Network Disk = 0)  - local SSDs selected for RAID'ing
#   (local SSD >= 0, Network Disk > 1) - network disks selected for RAID'ing
# Keep track of the Local SSDs and EBS volumes for RAID'ing
local_disks=()
ebs_volumes=()

{{ if .Zfs }}
	apt-get update -q
	apt-get install -yq zfsutils-linux
{{ end }}

# On different machine types, the drives are either called nvme... or xvdd.
for d in $(ls /dev/nvme?n1 /dev/xvdd); do
{{ if .Zfs }}
  # Check if the disk is already part of a zpool or mounted; skip if so.
  (zpool list -v -P | grep ${d} > /dev/null) || (mount | grep ${d} > /dev/null)
{{ else }}
  # Skip already mounted disks.
  mount | grep ${d} > /dev/null
{{ end }}
  if [ $? -ne 0 ]; then
		if udevadm info --query=property --name=${d} | grep "ID_MODEL=Amazon Elastic Block Store">/dev/null; then
			ebs_volumes+=("${d}")
		else
			local_disks+=("${d}")
		fi
    echo "Disk ${d} not mounted, need to mount..."
  else
    echo "Disk ${d} already mounted, skipping..."
  fi
done

# use only EBS volumes if available and ignore EC2 NVMe Instance Storage
disks=()
if [ "${#ebs_volumes[@]}" -gt "0" ]; then
  echo "Using only EBS disks: ${ebs_volumes[@]}"
	disks=("${ebs_volumes[@]}")
else
	echo "Using only local disks: ${local_disks[@]}"
	disks=("${local_disks[@]}")
fi

if [ "${#disks[@]}" -eq "0" ]; then
  mountpoint="${mount_prefix}1"
  echo "No disks mounted, creating ${mountpoint}"
  mkdir -p ${mountpoint}
  chmod 777 ${mountpoint}
elif [ "${#disks[@]}" -eq "1" ] || [ -n "$use_multiple_disks" ]; then
  disknum=1
  for disk in "${disks[@]}"
  do
    mountpoint="${mount_prefix}${disknum}"
    disknum=$((disknum + 1 ))
    echo "Mounting ${disk} at ${mountpoint}"
    mkdir -p ${mountpoint}
{{ if .Zfs }}
    zpool create -f $(basename $mountpoint) -m ${mountpoint} ${disk}
    # NOTE: we don't need an /etc/fstab entry for ZFS. It will handle this itself.
{{ else }}
    mkfs.ext4 -F ${disk}
    mount -o ${mount_opts} ${disk} ${mountpoint}
    echo "${disk} ${mountpoint} ext4 ${mount_opts} 1 1" | tee -a /etc/fstab
    tune2fs -m 0 ${disk}
{{ end }}
    chmod 777 ${mountpoint}
  done
else
  mountpoint="${mount_prefix}1"
  echo "${#disks[@]} disks mounted, creating ${mountpoint} using RAID 0"
  mkdir -p ${mountpoint}
{{ if .Zfs }}
  zpool create -f $(basename $mountpoint) -m ${mountpoint} ${disks[@]}
  # NOTE: we don't need an /etc/fstab entry for ZFS. It will handle this itself.
{{ else }}
  raiddisk="/dev/md0"
  mdadm --create ${raiddisk} --level=0 --raid-devices=${#disks[@]} "${disks[@]}"
  mkfs.ext4 -F ${raiddisk}
  mount -o ${mount_opts} ${raiddisk} ${mountpoint}
  echo "${raiddisk} ${mountpoint} ext4 ${mount_opts} 1 1" | tee -a /etc/fstab
  tune2fs -m 0 ${raiddisk}
{{ end }}
  chmod 777 ${mountpoint}
fi

# Print the block device and FS usage output. This is useful for debugging.
lsblk
df -h
{{ if .Zfs }}
zpool list
{{ end }}

sudo apt-get install -qy chrony

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
server 169.254.169.123 prefer iburst
makestep 0.1 3
EOF

sudo /etc/init.d/chrony restart
sudo chronyc -a waitsync 30 0.01 | sudo tee -a /root/chrony.log

# sshguard can prevent frequent ssh connections to the same host. Disable it.
sudo service sshguard stop
# increase the number of concurrent unauthenticated connections to the sshd
# daemon. See https://en.wikibooks.org/wiki/OpenSSH/Cookbook/Load_Balancing.
# By default, only 10 unauthenticated connections are permitted before sshd
# starts randomly dropping connections.
sudo sh -c 'echo "MaxStartups 64:30:128" >> /etc/ssh/sshd_config'
# Crank up the logging for issues such as:
# https://github.com/cockroachdb/cockroach/issues/36929
sudo sed -i'' 's/LogLevel.*$/LogLevel DEBUG3/' /etc/ssh/sshd_config
# N.B. RSA SHA1 is no longer supported in the latest versions of OpenSSH. Existing tooling, e.g.,
# jepsen still relies on it for authentication, so we need to enable it.
# FIPS is still on Ubuntu 20.04 however, so don't enable if using FIPS.
{{ if not .EnableFIPS }}
sudo sh -c 'echo "PubkeyAcceptedAlgorithms +ssh-rsa" >> /etc/ssh/sshd_config'
{{ end }}
sudo service sshd restart
# increase the default maximum number of open file descriptors for
# root and non-root users. Load generators running a lot of concurrent
# workers bump into this often.
sudo sh -c 'echo "root - nofile 1048576\n* - nofile 1048576" > /etc/security/limits.d/10-roachprod-nofiles.conf'

# N.B. Ubuntu 22.04 changed the location of tcpdump to /usr/bin. Since existing tooling, e.g.,
# jepsen uses /usr/sbin, we create a symlink.
# See https://ubuntu.pkgs.org/22.04/ubuntu-main-amd64/tcpdump_4.99.1-3build2_amd64.deb.html
# FIPS is still on Ubuntu 20.04 however, so don't enable if using FIPS.
{{ if not .EnableFIPS }}
sudo ln -s /usr/bin/tcpdump /usr/sbin/tcpdump
{{ end }}

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

# set hostname according to the name used by roachprod. There's host
# validation logic that relies on this -- see comment on cluster_synced.go
sudo hostnamectl set-hostname {{.VMName}}

{{ if .EnableFIPS }}
sudo ua enable fips --assume-yes
{{ end }}

sudo sed -i 's/#LoginGraceTime .*/LoginGraceTime 0/g' /etc/ssh/sshd_config
sudo service ssh restart

sudo touch {{ .DisksInitializedFile }}
`

// writeStartupScript writes the startup script to a temp file.
// Returns the path to the file.
// After use, the caller should delete the temp file.
//
// extraMountOpts, if not empty, is appended to the default mount options. It is
// a comma-separated list of options for the "mount -o" flag.
func writeStartupScript(
	name string, extraMountOpts string, fileSystem string, useMultiple bool, enableFips bool,
) (string, error) {
	type tmplParams struct {
		VMName               string
		ExtraMountOpts       string
		UseMultipleDisks     bool
		Zfs                  bool
		EnableFIPS           bool
		DisksInitializedFile string
	}

	args := tmplParams{
		VMName:               name,
		ExtraMountOpts:       extraMountOpts,
		UseMultipleDisks:     useMultiple,
		Zfs:                  fileSystem == vm.Zfs,
		EnableFIPS:           enableFips,
		DisksInitializedFile: vm.DisksInitializedFile,
	}

	tmpfile, err := os.CreateTemp("", "aws-startup-script")
	if err != nil {
		return "", err
	}
	defer tmpfile.Close()

	t := template.Must(template.New("start").Parse(awsStartupScriptTemplate))
	if err := t.Execute(tmpfile, args); err != nil {
		return "", err
	}
	return tmpfile.Name(), nil
}

// runCommand is used to invoke an AWS command.
func (p *Provider) runCommand(l *logger.Logger, args []string) ([]byte, error) {

	if p.Profile != "" {
		args = append(args[:len(args):len(args)], "--profile", p.Profile)
	}
	var stderrBuf bytes.Buffer
	cmd := exec.Command("aws", args...)
	cmd.Stderr = &stderrBuf
	output, err := cmd.Output()
	if err != nil {
		if exitErr := (*exec.ExitError)(nil); errors.As(err, &exitErr) {
			l.Printf("%s", string(exitErr.Stderr))
		}
		return nil, errors.Wrapf(err, "failed to run: aws %s: stderr: %v",
			strings.Join(args, " "), stderrBuf.String())
	}
	return output, nil
}

// runJSONCommand invokes an aws command and parses the json output.
func (p *Provider) runJSONCommand(l *logger.Logger, args []string, parsed interface{}) error {
	// Force json output in case the user has overridden the default behavior.
	args = append(args[:len(args):len(args)], "--output", "json")
	rawJSON, err := p.runCommand(l, args)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(rawJSON, &parsed); err != nil {
		return errors.Wrapf(err, "failed to parse json %s", rawJSON)
	}

	return nil
}

// regionMap collates VM instances by their region.
func regionMap(vms vm.List) (map[string]vm.List, error) {
	// Fan out the work by region
	byRegion := make(map[string]vm.List)
	for _, m := range vms {
		region, err := zoneToRegion(m.Zone)
		if err != nil {
			return nil, err
		}
		byRegion[region] = append(byRegion[region], m)
	}
	return byRegion, nil
}

// zoneToRegion converts an availability zone like us-east-2a to the zone name us-east-2
func zoneToRegion(zone string) (string, error) {
	return zone[0 : len(zone)-1], nil
}
