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

function setup_disks() {
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
		mounted="no"
{{ if .Zfs }}
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

sudo touch {{ .OSInitializedFile }}
`

// writeStartupScript writes the startup script to a temp file.
// Returns the path to the file.
// After use, the caller should delete the temp file.
//
// extraMountOpts, if not empty, is appended to the default mount options. It is
// a comma-separated list of options for the "mount -o" flag.
func writeStartupScript(
	name string,
	extraMountOpts string,
	fileSystem string,
	useMultiple bool,
	enableFips bool,
	remoteUser string,
) (string, error) {
	type tmplParams struct {
		vm.StartupArgs
		ExtraMountOpts   string
		UseMultipleDisks bool
	}

	args := tmplParams{
		StartupArgs: vm.DefaultStartupArgs(
			vm.WithVMName(name),
			vm.WithSharedUser(remoteUser),
			vm.WithZfs(fileSystem == vm.Zfs),
			vm.WithEnableFIPS(enableFips),
			vm.WithChronyServers([]string{"169.254.169.123"}),
		),
		ExtraMountOpts:   extraMountOpts,
		UseMultipleDisks: useMultiple,
	}

	tmpfile, err := os.CreateTemp("", "aws-startup-script")
	if err != nil {
		return "", err
	}
	defer tmpfile.Close()

	err = vm.GenerateStartupScript(tmpfile, awsStartupScriptTemplate, args)
	if err != nil {
		return "", errors.Wrapf(err, "unable to generate startup script")
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
			l.Printf("%s", exitErr)
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
