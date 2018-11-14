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

package gce

import (
	"io/ioutil"
)

// Startup script used to find/format/mount all local SSDs in GCE.
// Each disk is mounted to /mnt/data<disknum> and chmoded to all users.
const gceLocalSSDStartupScript = `#!/usr/bin/env bash
disknum=0
# Assume google.
for d in $(ls /dev/disk/by-id/google-local-ssd-*); do
  let "disknum++"
  grep -e "${d}" /etc/fstab > /dev/null
  if [ $? -ne 0 ]; then
    echo "Disk ${disknum}: ${d} not mounted, creating..."
    mountpoint="/mnt/data${disknum}"
    sudo mkdir -p "${mountpoint}"
    sudo mkfs.ext4 -F ${d}
    sudo mount -o discard,defaults ${d} ${mountpoint}
    echo "${d} ${mountpoint} ext4 discard,defaults 1 1" | sudo tee -a /etc/fstab
  else
    echo "Disk ${disknum}: ${d} already mounted, skipping..."
  fi
done
if [ "${disknum}" -eq "0" ]; then
  echo "No disks mounted, creating /mnt/data1"
  sudo mkdir -p /mnt/data1
fi

sudo chmod 777 /mnt/data1
# sshguard can prevent frequent ssh connections to the same host. Disable it.
sudo service sshguard stop
# increase the default maximum number of open file descriptors for
# root and non-root users. Load generators running a lot of concurrent
# workers bump into this often.
sudo sh -c 'echo "root - nofile 65536\n* - nofile 65536" > /etc/security/limits.d/10-roachprod-nofiles.conf'
sudo touch /mnt/data1/.roachprod-initialized

# Send TCP keepalives every minute since GCE will terminate idle connections
# after 10m. Note that keepalives still need to be requested by the application
# with the SO_KEEPALIVE socket option.
cat <<EOF > /etc/sysctl.d/99-roachprod-tcp-keepalive.conf
net.ipv4.tcp_keepalive_time=60
net.ipv4.tcp_keepalive_intvl=60
net.ipv4.tcp_keepalive_probes=5
EOF
sysctl --system  # reload sysctl settings
`

// write the startup script to a temp file.
// Returns the path to the file.
// After use, the caller should delete the temp file.
func writeStartupScript() (string, error) {
	tmpfile, err := ioutil.TempFile("", "gce-startup-script")
	if err != nil {
		return "", err
	}
	defer tmpfile.Close()

	if _, err := tmpfile.WriteString(gceLocalSSDStartupScript); err != nil {
		return "", err
	}
	return tmpfile.Name(), nil
}
