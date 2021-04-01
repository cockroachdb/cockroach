// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package aws

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"os/exec"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm"
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
sudo apt-get update
sudo apt-get install -qy --no-install-recommends mdadm

mount_opts="defaults"
{{if .ExtraMountOpts}}mount_opts="${mount_opts},{{.ExtraMountOpts}}"{{end}}

use_multiple_disks='{{if .UseMultipleDisks}}true{{end}}'

disks=()
mount_prefix="/mnt/data"

# On different machine types, the drives are either called nvme... or xvdd.
for d in $(ls /dev/nvme?n1 /dev/xvdd); do
  if ! mount | grep ${d}; then
    disks+=("${d}")
    echo "Disk ${d} not mounted, creating..."
  else
    echo "Disk ${d} already mounted, skipping..."
  fi
done


if [ "${#disks[@]}" -eq "0" ]; then
  mountpoint="${mount_prefix}1"
  echo "No disks mounted, creating ${mountpoint}"
  mkdir -p ${mountpoint}
  chmod 777 ${mountpoint}
elif [ "${#disks[@]}" -eq "1" ] || [ -n "use_multiple_disks" ]; then
  disknum=1
  for disk in "${disks[@]}"
  do
    mountpoint="${mount_prefix}${disknum}"
    disknum=$((disknum + 1 ))
    echo "Creating ${mountpoint}"
    mkdir -p ${mountpoint}
    mkfs.ext4 -F ${disk}
    mount -o ${mount_opts} ${disk} ${mountpoint}
    chmod 777 ${mountpoint}
    echo "${disk} ${mountpoint} ext4 ${mount_opts} 1 1" | tee -a /etc/fstab
  done
else
  mountpoint="${mount_prefix}1"
  echo "${#disks[@]} disks mounted, creating ${mountpoint} using RAID 0"
  mkdir -p ${mountpoint}
  raiddisk="/dev/md0"
  mdadm --create ${raiddisk} --level=0 --raid-devices=${#disks[@]} "${disks[@]}"
  mkfs.ext4 -F ${raiddisk}
  mount -o ${mount_opts} ${raiddisk} ${mountpoint}
  chmod 777 ${mountpoint}
  echo "${raiddisk} ${mountpoint} ext4 ${mount_opts} 1 1" | tee -a /etc/fstab
fi

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
sudo service sshd restart
# increase the default maximum number of open file descriptors for
# root and non-root users. Load generators running a lot of concurrent
# workers bump into this often.
sudo sh -c 'echo "root - nofile 1048576\n* - nofile 1048576" > /etc/security/limits.d/10-roachprod-nofiles.conf'

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

sudo touch /mnt/data1/.roachprod-initialized
`

// writeStartupScript writes the startup script to a temp file.
// Returns the path to the file.
// After use, the caller should delete the temp file.
//
// extraMountOpts, if not empty, is appended to the default mount options. It is
// a comma-separated list of options for the "mount -o" flag.
func writeStartupScript(extraMountOpts string, useMultiple bool) (string, error) {
	type tmplParams struct {
		ExtraMountOpts   string
		UseMultipleDisks bool
	}

	args := tmplParams{ExtraMountOpts: extraMountOpts, UseMultipleDisks: useMultiple}

	tmpfile, err := ioutil.TempFile("", "aws-startup-script")
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
func (p *Provider) runCommand(args []string) ([]byte, error) {

	if p.opts.Profile != "" {
		args = append(args[:len(args):len(args)], "--profile", p.opts.Profile)
	}
	var stderrBuf bytes.Buffer
	cmd := exec.Command("aws", args...)
	cmd.Stderr = &stderrBuf
	output, err := cmd.Output()
	if err != nil {
		if exitErr := (*exec.ExitError)(nil); errors.As(err, &exitErr) {
			log.Println(string(exitErr.Stderr))
		}
		return nil, errors.Wrapf(err, "failed to run: aws %s: stderr: %v",
			strings.Join(args, " "), stderrBuf.String())
	}
	return output, nil
}

// runJSONCommand invokes an aws command and parses the json output.
func (p *Provider) runJSONCommand(args []string, parsed interface{}) error {
	// Force json output in case the user has overridden the default behavior.
	args = append(args[:len(args):len(args)], "--output", "json")
	rawJSON, err := p.runCommand(args)
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
