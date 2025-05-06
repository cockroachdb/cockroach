// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ibm

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
)

// Startup script used to find/format/mount all local or attached disks.
// Each disk is mounted as /data<disknum>, and, in addition, a symlink
// created from /mnt/data<disknum> to the mount point.

// startupArgs specifies template arguments for the setup template.
type startupArgs struct {
	vm.StartupArgs
	ExtraMountOpts   string
	UseMultipleDisks bool
}

const startupTemplate = `#!/usr/bin/env bash

# Script for setting up an IBM machine for roachprod use.

function setup_disks() {
	mount_prefix="/mnt/data"
	use_multiple_disks='{{if .UseMultipleDisks}}true{{end}}'

{{ if not .Zfs }}
	mount_opts="defaults,nofail"
	{{if .ExtraMountOpts}}mount_opts="${mount_opts},{{.ExtraMountOpts}}"{{end}}
{{ else }}
 	apt-get update -q
	apt-get install -yq zfsutils-linux
{{ end }}

	# List the attached data disks
	disks=()
	for d in $(find /dev/disk/by-id/ -type l -not -name *cloud-init* -not -name *part* -exec readlink -f {} \;); do
		mounted="no"

		# Check if the disk is already mounted; skip if so.
{{ if .Zfs }}
		if (zpool list -v -P | grep -q ${d}) || (mount | grep -q ${d}); then
			mounted="yes"
		fi
{{ else }}
		if mount | grep -q ${d}; then
			mounted="yes"
		fi
{{ end }}

		if [ "$mounted" = "no" ]; then
			disks+=("${d}")
			echo "Disk ${d} is not mounted, mounting it"
		else
			echo "Disk ${d} is already mounted, skipping"
		fi
	done

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
			disknum=$(( disknum + 1 ))
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
	sudo touch {{ .DisksInitializedFile }}
}

{{ template "head_utils" . }}
{{ template "apt_packages" . }}

# Initialize disks.
setup_disks

{{ template "ulimits" . }}
{{ template "tcpdump" . }}
{{ template "keepalives" . }}
{{ template "cron_utils" . }}
{{ template "chrony_utils" . }}
{{ template "timers_services_utils" . }}
{{ template "core_dumps_utils" . }}
{{ template "hostname_utils" . }}
{{ template "fips_utils" . }}
{{ template "ssh_utils" . }}
{{ template "node_exporter" . }}
{{ template "ebpf_exporter" . }}

sudo touch {{ .OSInitializedFile }}
`

// startupScript returns the startup script for the given arguments.
func (p *Provider) startupScript(args startupArgs) (string, error) {
	data := bytes.NewBuffer(nil)

	err := vm.GenerateStartupScript(data, startupTemplate, args)
	if err != nil {
		return "", err
	}

	return data.String(), nil
}
