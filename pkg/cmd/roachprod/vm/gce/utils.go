// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gce

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm"
	"github.com/cockroachdb/errors"
)

const (
	dnsProject = "cockroach-shared"
	dnsZone    = "roachprod"
)

// Subdomain is the DNS subdomain to in which to maintain cluster node names.
var Subdomain = func() string {
	if d, ok := os.LookupEnv("ROACHPROD_DNS"); ok {
		return d
	}
	return "roachprod.crdb.io"
}()

// Startup script used to find/format/mount all local SSDs in GCE.
// Each disk is mounted to /mnt/data<disknum> and chmoded to all users.
//
// This is a template because the instantiator needs to optionally configure the
// mounting options. The script cannot take arguments since it is to be invoked
// by the gcloud tool which cannot pass args.
const gceLocalSSDStartupScriptTemplate = `#!/usr/bin/env bash
# Script for setting up a GCE machine for roachprod use.

mount_opts="discard,defaults"
{{if .ExtraMountOpts}}mount_opts="${mount_opts},{{.ExtraMountOpts}}"{{end}}

disknum=0
for d in $(ls /dev/disk/by-id/google-local-*); do
  let "disknum++"
  grep -e "${d}" /etc/fstab > /dev/null
  if [ $? -ne 0 ]; then
    echo "Disk ${disknum}: ${d} not mounted, creating..."
    mountpoint="/mnt/data${disknum}"
    sudo mkdir -p "${mountpoint}"
    sudo mkfs.ext4 -F ${d}
    sudo mount -o ${mount_opts} ${d} ${mountpoint}
    echo "${d} ${mountpoint} ext4 ${mount_opts} 1 1" | sudo tee -a /etc/fstab
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
sudo sh -c 'echo "root - nofile 65536\n* - nofile 65536" > /etc/security/limits.d/10-roachprod-nofiles.conf'

# Send TCP keepalives every minute since GCE will terminate idle connections
# after 10m. Note that keepalives still need to be requested by the application
# with the SO_KEEPALIVE socket option.
cat <<EOF > /etc/sysctl.d/99-roachprod-tcp-keepalive.conf
net.ipv4.tcp_keepalive_time=60
net.ipv4.tcp_keepalive_intvl=60
net.ipv4.tcp_keepalive_probes=5
EOF

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
func writeStartupScript(extraMountOpts string) (string, error) {
	type tmplParams struct {
		ExtraMountOpts string
	}

	args := tmplParams{ExtraMountOpts: extraMountOpts}

	tmpfile, err := ioutil.TempFile("", "gce-startup-script")
	if err != nil {
		return "", err
	}
	defer tmpfile.Close()

	t := template.Must(template.New("start").Parse(gceLocalSSDStartupScriptTemplate))
	if err := t.Execute(tmpfile, args); err != nil {
		return "", err
	}
	return tmpfile.Name(), nil
}

// SyncDNS replaces the configured DNS zone with the supplied hosts.
func SyncDNS(vms vm.List) error {
	if Subdomain == "" {
		return nil
	}

	f, err := ioutil.TempFile(os.ExpandEnv("$HOME/.roachprod/"), "dns.bind")
	if err != nil {
		return err
	}
	defer f.Close()
	defer func() {
		if err := os.Remove(f.Name()); err != nil {
			fmt.Fprintf(os.Stderr, "removing %s failed: %v", f.Name(), err)
		}
	}()
	for _, vm := range vms {
		if len(vm.Name) < 60 {
			fmt.Fprintf(f, "%s 60 IN A %s\n", vm.Name, vm.PublicIP)
		}
	}
	f.Close()

	args := []string{"--project", dnsProject, "dns", "record-sets", "import",
		"-z", dnsZone, "--delete-all-existing", "--zone-file-format", f.Name()}
	cmd := exec.Command("gcloud", args...)
	output, err := cmd.CombinedOutput()

	return errors.Wrapf(err, "Command: gcloud %s\nOutput: %s", args, output)
}

// GetUserAuthorizedKeys retreives reads a list of user public keys from the
// gcloud cockroach-ephemeral project and returns them formatted for use in
// an authorized_keys file.
func GetUserAuthorizedKeys() (authorizedKeys []byte, err error) {
	var outBuf bytes.Buffer
	// The below command will return a stream of user:pubkey as text.
	cmd := exec.Command("gcloud", "compute", "project-info", "describe",
		"--project=cockroach-ephemeral",
		"--format=value(commonInstanceMetadata.ssh-keys)")
	cmd.Stderr = os.Stderr
	cmd.Stdout = &outBuf
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	// Initialize a bufio.Reader with a large enough buffer that we will never
	// expect a line prefix when processing lines and can return an error if a
	// call to ReadLine ever returns a prefix.
	var pubKeyBuf bytes.Buffer
	r := bufio.NewReaderSize(&outBuf, 1<<16 /* 64 kB */)
	for {
		line, isPrefix, err := r.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if isPrefix {
			return nil, fmt.Errorf("unexpectedly failed to read public key line")
		}
		if len(line) == 0 {
			continue
		}
		colonIdx := bytes.IndexRune(line, ':')
		if colonIdx == -1 {
			return nil, fmt.Errorf("malformed public key line %q", string(line))
		}
		// Skip users named "root" or "ubuntu" which don't correspond to humans
		// and should be removed from the gcloud project.
		if name := string(line[:colonIdx]); name == "root" || name == "ubuntu" {
			continue
		}
		pubKeyBuf.Write(line[colonIdx+1:])
		pubKeyBuf.WriteRune('\n')
	}
	return pubKeyBuf.Bytes(), nil
}
