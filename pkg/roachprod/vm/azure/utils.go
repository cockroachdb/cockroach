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
// Each disk is mounted as /data<disknum>, and, in addition, a symlink
// created from /mnt/data<disknum> to the mount point.
// azureStartupArgs specifies template arguments for the setup template.
type azureStartupArgs struct {
	vm.StartupArgs
	AttachedDiskLun    *int // Use attached disk, with specified LUN; Use local ssd if nil.
	DiskControllerNVMe bool // Interface data disk via NVMe
}

const azureStartupTemplate = `#!/bin/bash

# Script for setting up a Azure machine for roachprod use.

function setup_disks() {
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
		chown {{.SharedUser}} /mnt/data1
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
		chown {{.SharedUser}} /data*
	fi
	sudo touch {{ .DisksInitializedFile }}
}

{{ template "head_utils" . }}
{{ template "apt_packages" . }}

# Initialize disks.
setup_disks

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
