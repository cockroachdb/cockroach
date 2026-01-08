// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aws

import (
	"bytes"
	"context"
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

{{ template "head_utils" . }}
{{ template "apt_packages" . }}

# Provider specific disk logic
function detect_disks() {
	# AWS-specific disk detection - returns list of available disks
	# If both EC2 NVMe Instance Storage and EBS exist, prioritize EBS volumes to avoid
	# performance differences when RAID'ing mixed disk types.
	# Scenarios:
	#   (local SSD = 0, Network Disk - 1)  - no RAID'ing and mount network disk
	#   (local SSD = 1, Network Disk - 0)  - no RAID'ing and mount local SSD
	#   (local SSD >= 1, Network Disk = 1) - no RAID'ing and mount network disk
	#   (local SSD > 1, Network Disk = 0)  - local SSDs selected for RAID'ing
	#   (local SSD >= 0, Network Disk > 1) - network disks selected for RAID'ing
	local local_disks=()
	local ebs_volumes=()
	local disks=()

	# On different machine types, the drives are either called nvme... or xvdd.
	for d in $(ls /dev/nvme?n1 /dev/xvdd); do
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
			if udevadm info --query=property --name=${d} | grep "ID_MODEL=Amazon Elastic Block Store">/dev/null; then
				ebs_volumes+=("${d}")
			else
				local_disks+=("${d}")
			fi
			echo "Disk ${d} not mounted, need to mount..." >&2
		else
			echo "Disk ${d} already mounted, skipping..." >&2
		fi
	done

	# use only EBS volumes if available and ignore EC2 NVMe Instance Storage
	if [ "${#ebs_volumes[@]}" -gt "0" ]; then
		echo "Using only EBS disks: ${ebs_volumes[@]}" >&2
		disks=("${ebs_volumes[@]}")
	else
		echo "Using only local disks: ${local_disks[@]}" >&2
		disks=("${local_disks[@]}")
	fi

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

// writeStartupScript writes the startup script to a temp file.
// Returns the path to the file.
// After use, the caller should delete the temp file.
//
// extraMountOpts, if not empty, is appended to the default mount options. It is
// a comma-separated list of options for the "mount -o" flag.
func writeStartupScript(
	name string,
	extraMountOpts string,
	fileSystem vm.Filesystem,
	useMultiple bool,
	enableFips bool,
	remoteUser string,
	bootDiskOnly bool,
) (string, error) {

	// We define a local type in case we need to add more provider-specific params in
	// the future.
	type tmplParams struct {
		vm.StartupArgs
	}

	args := tmplParams{
		StartupArgs: vm.DefaultStartupArgs(
			vm.WithVMName(name),
			vm.WithSharedUser(remoteUser),
			vm.WithFilesystem(fileSystem),
			vm.WithExtraMountOpts(extraMountOpts),
			vm.WithUseMultipleDisks(useMultiple),
			vm.WithBootDiskOnly(bootDiskOnly),
			vm.WithEnableFIPS(enableFips),
			vm.WithChronyServers([]string{"169.254.169.123"}),
		),
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
	return p.runCommandWithContext(context.Background(), l, args)
}

// runCommand is used to invoke an AWS command.
func (p *Provider) runCommandWithContext(
	ctx context.Context, l *logger.Logger, args []string,
) ([]byte, error) {

	if p.Profile != "" {
		args = append(args[:len(args):len(args)], "--profile", p.Profile)
	}

	credentialsEnv, err := p.getEnvironmentAWSCredentials()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get AWS credentials")
	}

	var stderrBuf bytes.Buffer
	cmd := exec.CommandContext(ctx, "aws", args...)
	cmd.Stderr = &stderrBuf
	cmd.Env = append(os.Environ(), credentialsEnv...)
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
	return p.runJSONCommandWithContext(context.Background(), l, args, parsed)
}

// runJSONCommandWithContext invokes an aws command and parses the json output.
func (p *Provider) runJSONCommandWithContext(
	ctx context.Context, l *logger.Logger, args []string, parsed interface{},
) error {
	// Force json output in case the user has overridden the default behavior.
	args = append(args[:len(args):len(args)], "--output", "json")
	rawJSON, err := p.runCommandWithContext(ctx, l, args)
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
