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
// We define a local type in case we need to add more provider-specific params in
// the future.
type startupArgs struct {
	vm.StartupArgs
}

const startupTemplate = `#!/usr/bin/env bash

# Script for setting up an IBM machine for roachprod use.

{{ template "head_utils" . }}
{{ template "apt_packages" . }}

# Provider specific disk logic
function detect_disks() {
	# IBM-specific disk detection - returns list of available disks
	local disks=()

	# List the attached data disks
	for d in $(find /dev/disk/by-id/ -type l -not -name *cloud-init* -not -name *part* -exec readlink -f {} \;); do
		mounted="no"

		# Check if the disk is already mounted; skip if so.
{{ if eq .Filesystem "zfs" }}
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
			echo "Disk ${d} is not mounted, need to mount..." >&2
		else
			echo "Disk ${d} is already mounted, skipping..." >&2
		fi
	done

	# Return disks array by printing each element (only if non-empty)
	if [ "${#disks[@]}" -gt 0 ]; then
		printf '%s\n' "${disks[@]}"
	fi
}

# Common disk setup logic that calls the above detect_disks function
{{ template "setup_disks_utils" . }}

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
