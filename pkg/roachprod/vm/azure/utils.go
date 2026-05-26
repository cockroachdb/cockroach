// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package azure

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"io"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
)

// Startup script used to find/format/mount all local or attached disks.
// Each disk is mounted at /mnt/data<disknum>.
// azureStartupArgs specifies template arguments for the setup template.
// We define a local type in case we need to add more provider-specific params in
// the future.
type azureStartupArgs struct {
	vm.StartupArgs
}

const azureStartupTemplate = `#!/bin/bash

# Script for setting up a Azure machine for roachprod use.

{{ template "head_utils" . }}
{{ template "apt_packages" . }}

# Provider specific disk logic
function detect_disks() {
	# Azure-specific disk detection - returns list of available disks
	local local_or_network=()
	local disks=()

	# First, try to find NVMe disks (excluding boot disk nvme0n1)
	if [ "$(ls /dev/disk/by-id/nvme-* 2>/dev/null | wc -l)" -gt "0" ]; then
		local_or_network=($(realpath -qe /dev/disk/by-id/nvme-* | grep -v "nvme0n1" | sort -u))
		if [ "${#local_or_network[@]}" -gt "0" ]; then
			echo "Using NVMe disks: ${local_or_network[@]}" >&2
		fi
	fi

	# If no NVMe disks found, try SCSI attached storage
	if [ "${#local_or_network[@]}" -eq "0" ] && [ "$(ls /dev/disk/azure/scsi1/lun* 2>/dev/null | wc -l)" -gt "0" ]; then
		local_or_network=($(ls /dev/disk/azure/scsi1/lun*))
		echo "Using SCSI disks: ${local_or_network[@]}" >&2
	fi

	for l in ${local_or_network[@]}; do
		d=$(readlink -f $l)
		mounted="no"
{{ if eq .Filesystem "zfs" }}
		# Check if the disk is already part of a zpool or mounted; skip if so.
		if (zpool list -v -P | grep -q ${d}) || (mount | grep -q ${d}); then
			mounted="yes"
		fi
{{ else }}
		# Skip already mounted disks.
		if mount | grep -q ${d}; then
			mounted="yes"
		fi
{{ end }}
		if [ "$mounted" == "no" ]; then
			disks+=("${d}")
			echo "Disk ${d} not mounted, need to mount..." >&2
		else
			echo "Disk ${d} already mounted, skipping..." >&2
		fi
	done

	# Return disks array by printing each element (only if non-empty)
	if [ "${#disks[@]}" -gt 0 ]; then
		printf '%s\n' "${disks[@]}"
	fi
}

# Common disk setup logic that calls the above detect_disks function
{{ template "setup_disks_utils" . }}

# Disable Hyper-V Time Synchronization device
# See https://www.cockroachlabs.com/docs/stable/deploy-cockroachdb-on-microsoft-azure#step-3-synchronize-clocks
curl -O https://raw.githubusercontent.com/torvalds/linux/master/tools/hv/lsvmbus
ts_dev_id=$(python3 lsvmbus -vv | grep -w "Time Synchronization" -A 3 | grep "Device_ID" | awk -F '[{}]' '{print $2}')
echo $ts_dev_id | sudo tee /sys/bus/vmbus/drivers/hv_utils/unbind

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

	err := generateStartupScript(gz, args)
	if err != nil {
		return "", errors.Wrapf(err, "unable to generate startup script")
	}

	if err := gz.Close(); err != nil {
		return "", err
	}

	if err := encoder.Close(); err != nil {
		return "", err
	}
	return cloudInit.String(), nil
}

func generateStartupScript(w io.Writer, args azureStartupArgs) error {
	return vm.GenerateStartupScript(w, azureStartupTemplate, args)
}
