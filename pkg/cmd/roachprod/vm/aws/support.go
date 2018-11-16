// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package aws

import (
	"encoding/json"
	"log"
	"os/exec"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm"
	"github.com/pkg/errors"
)

// Both M5 and I3 machines expose their EBS or local SSD volumes as NVMe block devices, but
// the actual device numbers vary a bit between the two types.
// This user-data script will create a filesystem, mount the data volume, and chmod 777.
// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/nvme-ebs-volumes.html
const awsStartupScript = `#!/usr/bin/env bash
set -x
sudo apt-get update
sudo apt-get install -qy --no-install-recommends mdadm

disks=()
mountpoint="/mnt/data1"
for d in $(ls /dev/nvme?n1); do
  if ! mount | grep ${d}; then
    disks+=("${d}")
    echo "Disk ${d} not mounted, creating..."
  else
    echo "Disk ${d} already mounted, skipping..."
  fi
done
if [ "${#disks[@]}" -eq "0" ]; then
  echo "No disks mounted, creating ${mountpoint}"
  mkdir -p ${mountpoint}
  chmod 777 ${mountpoint}
elif [ "${#disks[@]}" -eq "1" ]; then
  echo "One disk mounted, creating ${mountpoint}"
  mkdir -p ${mountpoint}
  disk=${disks[0]}
  mkfs.ext4 -E nodiscard ${disk}
  mount -o discard,defaults ${disk} ${mountpoint}
  chmod 777 ${mountpoint}
  echo "${disk} ${mountpoint} ext4 discard,defaults 1 1" | tee -a /etc/fstab
else
  echo "${#disks[@]} disks mounted, creating ${mountpoint} using RAID 0"
  mkdir -p ${mountpoint}
  raiddisk="/dev/md0"
  mdadm --create ${raiddisk} --level=0 --raid-devices=${#disks[@]} "${disks[@]}"
  mkfs.ext4 -E nodiscard ${raiddisk}
  mount -o discard,defaults ${raiddisk} ${mountpoint}
  chmod 777 ${mountpoint}
  echo "${raiddisk} ${mountpoint} ext4 discard,defaults 1 1" | tee -a /etc/fstab
fi

sudo apt-get install -qy chrony
echo -e "\nserver 169.254.169.123 prefer iburst" | sudo tee -a /etc/chrony/chrony.conf
echo -e "\nmakestep 0.1 3" | sudo tee -a /etc/chrony/chrony.conf
sudo /etc/init.d/chrony restart
sudo chronyc -a waitsync 30 0.01 | sudo tee -a /root/chrony.log

# increase the default maximum number of open file descriptors for
# root and non-root users. Load generators running a lot of concurrent
# workers bump into this often.
sudo sh -c 'echo "root - nofile 65536\n* - nofile 65536" > /etc/security/limits.d/10-roachprod-nofiles.conf'
sudo touch /mnt/data1/.roachprod-initialized
`

// runCommand is used to invoke an AWS command for which no output is expected.
func runCommand(args []string) error {
	cmd := exec.Command("aws", args...)

	_, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Println(string(exitErr.Stderr))
		}
		return errors.Wrapf(err, "failed to run: aws %s", strings.Join(args, " "))
	}
	return nil
}

// runJSONCommand invokes an aws command and parses the json output.
func runJSONCommand(args []string, parsed interface{}) error {
	// force json output in case the user has overridden the default behavior
	args = append(args[:len(args):len(args)], "--output", "json")
	cmd := exec.Command("aws", args...)

	rawJSON, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Println(string(exitErr.Stderr))
		}
		return errors.Wrapf(err, "failed to run: aws %s", strings.Join(args, " "))
	}

	if err := json.Unmarshal(rawJSON, &parsed); err != nil {
		return errors.Wrapf(err, "failed to parse json %s", rawJSON)
	}

	return nil
}

// splitMap splits a list of `key:value` pairs into a map.
func splitMap(data []string) (map[string]string, error) {
	ret := make(map[string]string, len(data))
	for _, part := range data {
		parts := strings.Split(part, ":")
		if len(parts) != 2 {
			return nil, errors.Errorf("Could not split Region:AMI: %s", part)
		}
		ret[parts[0]] = parts[1]
	}
	return ret, nil
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
