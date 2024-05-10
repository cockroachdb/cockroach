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
	"os"
	"os/exec"
	"slices"
	"sort"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
	"golang.org/x/crypto/ssh"
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

const gceDiskStartupScriptTemplate = `#!/usr/bin/env bash
# Script for setting up a GCE machine for roachprod use.

set -x

function setup_disks() {
  first_setup=$1

	{{ if not .Zfs }}
	mount_opts="defaults,nofail"
	{{if .ExtraMountOpts}}mount_opts="${mount_opts},{{.ExtraMountOpts}}"{{end}}
	{{ end }}
	
	use_multiple_disks='{{if .UseMultipleDisks}}true{{end}}'
	
	disks=()
	mount_prefix="/mnt/data"
	
	{{ if .Zfs }}
	apt-get update -q
	apt-get install -yq zfsutils-linux
	
	# For zfs, we use the device names under /dev instead of the device
	# links under /dev/disk/by-id/google-local* for local ssds, because
	# there is an issue where the links for the zfs partitions which are
	# created under /dev/disk/by-id/ when we run "zpool create ..." are
	# inaccurate.
	for d in $(ls /dev/nvme?n? /dev/disk/by-id/google-persistent-disk-[1-9]); do
		zpool list -v -P | grep ${d} > /dev/null
		if [ $? -ne 0 ]; then
	{{ else }}
	# if the use_multiple_disks is not set and there are more than 1 disk (excluding the boot disk),
	# then the disks will be selected for RAID'ing. If there are both Local SSDs and Persistent disks,
	# RAID'ing in this case can cause performance differences. So, to avoid this, local SSDs are ignored.
	# Scenarios:
	#   (local SSD = 0, Persistent Disk - 1) - no RAID'ing and Persistent Disk mounted
	#   (local SSD = 1, Persistent Disk - 0) - no RAID'ing and local SSD mounted
	#   (local SSD >= 1, Persistent Disk = 1) - no RAID'ing and Persistent Disk mounted
	#   (local SSD > 1, Persistent Disk = 0) - local SSDs selected for RAID'ing
	#   (local SSD >= 0, Persistent Disk > 1) - network disks selected for RAID'ing
	disk_list=()
	if [ "$(ls /dev/disk/by-id/google-persistent-disk-[1-9]|wc -l)" -eq "0" ]; then
		disk_list=$(ls /dev/disk/by-id/google-local-*)
	else
		echo "Only persistent disks are selected."
		disk_list=$(ls /dev/disk/by-id/google-persistent-disk-[1-9])
	fi
	for d in ${disk_list}; do
		if ! mount | grep ${d}; then
	{{ end }}
			disks+=("${d}")
			echo "Disk ${d} not mounted, need to mount..."
		else
			echo "Disk ${d} already mounted, skipping..."
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
			disknum=$((disknum + 1 ))
			echo "Mounting ${disk} at ${mountpoint}"
			mkdir -p ${mountpoint}
	{{ if .Zfs }}
			zpool create -f $(basename $mountpoint) -m ${mountpoint} ${disk}
			# NOTE: we don't need an /etc/fstab entry for ZFS. It will handle this itself.
	{{ else }}
			mkfs.ext4 -q -F ${disk}
			mount -o ${mount_opts} ${disk} ${mountpoint}
			if [ "$first_setup" = "true" ]; then
				echo "${d} ${mountpoint} ext4 ${mount_opts} 1 1" | tee -a /etc/fstab
			fi
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
		mdadm -q --create ${raiddisk} --level=0 --raid-devices=${#disks[@]} "${disks[@]}"
		mkfs.ext4 -q -F ${raiddisk}
		mount -o ${mount_opts} ${raiddisk} ${mountpoint}
		if [ "$first_setup" = "true" ]; then
			echo "${raiddisk} ${mountpoint} ext4 ${mount_opts} 1 1" | tee -a /etc/fstab
		fi
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

	mkdir -p /mnt/data1/cores
	chmod a+w /mnt/data1/cores

	sudo touch {{ .DisksInitializedFile }}
}

if [ -e {{ .DisksInitializedFile }} ]; then
  echo "OS and disks already initialized, exiting."
  exit 0
fi

if [ -e {{ .OSInitializedFile }} ]; then
  echo "OS already initialized, only initializing disks."
  # Initialize disks, but don't write fstab entries again.
  setup_disks false
  exit 0
fi

# Initialize disks and write fstab entries.
setup_disks true

# sshguard can prevent frequent ssh connections to the same host. Disable it.
systemctl stop sshguard
systemctl mask sshguard
# increase the number of concurrent unauthenticated connections to the sshd
# daemon. See https://en.wikibooks.org/wiki/OpenSSH/Cookbook/Load_Balancing.
# By default, only 10 unauthenticated connections are permitted before sshd
# starts randomly dropping connections.
sudo sh -c 'echo "MaxStartups 64:30:128" >> /etc/ssh/sshd_config'
# Crank up the logging for issues such as:
# https://github.com/cockroachdb/cockroach/issues/36929
sudo sed -i'' 's/LogLevel.*$/LogLevel DEBUG3/' /etc/ssh/sshd_config
# FIPS is still on Ubuntu 20.04 however, so don't enable if using FIPS.
{{ if not .EnableFIPS }}
sudo sh -c 'echo "PubkeyAcceptedAlgorithms +ssh-rsa" >> /etc/ssh/sshd_config'
{{ end }}
sudo service sshd restart
# increase the default maximum number of open file descriptors for
# root and non-root users. Load generators running a lot of concurrent
# workers bump into this often.
sudo sh -c 'echo "root - nofile 1048576\n* - nofile 1048576" > /etc/security/limits.d/10-roachprod-nofiles.conf'

# N.B. Ubuntu 22.04 changed the location of tcpdump to /usr/bin. Since existing tooling, e.g.,
# jepsen uses /usr/sbin, we create a symlink.
# See https://ubuntu.pkgs.org/22.04/ubuntu-main-amd64/tcpdump_4.99.1-3build2_amd64.deb.html
# FIPS is still on Ubuntu 20.04 however, so don't create if using FIPS.
{{ if not .EnableFIPS }}
sudo ln -s /usr/bin/tcpdump /usr/sbin/tcpdump
{{ end }}

# Send TCP keepalives every minute since GCE will terminate idle connections
# after 10m. Note that keepalives still need to be requested by the application
# with the SO_KEEPALIVE socket option.
cat <<EOF > /etc/sysctl.d/99-roachprod-tcp-keepalive.conf
net.ipv4.tcp_keepalive_time=60
net.ipv4.tcp_keepalive_intvl=60
net.ipv4.tcp_keepalive_probes=5
EOF

sudo apt-get update -q
sudo apt-get install -qy chrony

# Uninstall some packages to prevent them running cronjobs and similar jobs in parallel
systemctl stop unattended-upgrades
apt-get purge -y unattended-upgrades

{{ if not .EnableCron }}
systemctl stop cron
systemctl mask cron
{{ end }}

# Override the chrony config. In particular,
# log aggressively when clock is adjusted (0.01s)
# and exclusively use google's time servers.
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
server metadata.google.internal prefer iburst
makestep 0.1 3
EOF

sudo /etc/init.d/chrony restart
sudo chronyc -a waitsync 30 0.01 | sudo tee -a /root/chrony.log

for timer in apt-daily-upgrade.timer apt-daily.timer e2scrub_all.timer fstrim.timer man-db.timer e2scrub_all.timer ; do
  systemctl mask $timer
done

for service in apport.service atd.service; do
  systemctl stop $service
  systemctl mask $service
done

# Enable core dumps, do this last, something above resets /proc/sys/kernel/core_pattern
# to just "core".
cat <<EOF > /etc/security/limits.d/core_unlimited.conf
* soft core unlimited
* hard core unlimited
root soft core unlimited
root hard core unlimited
EOF

cat <<'EOF' > /bin/gzip_core.sh
#!/bin/sh
exec /bin/gzip -f - > /mnt/data1/cores/core.$1.$2.$3.$4.gz
EOF
chmod +x /bin/gzip_core.sh

CORE_PATTERN="|/bin/gzip_core.sh %e %p %h %t"
echo "$CORE_PATTERN" > /proc/sys/kernel/core_pattern
sed -i'~' 's/enabled=1/enabled=0/' /etc/default/apport
sed -i'~' '/.*kernel\\.core_pattern.*/c\\' /etc/sysctl.conf
echo "kernel.core_pattern=$CORE_PATTERN" >> /etc/sysctl.conf

sysctl --system  # reload sysctl settings

{{ if .EnableFIPS }}
sudo ua enable fips --assume-yes
{{ end }}

sudo -u {{ .SharedUser }} bash -c "mkdir ~/.ssh && chmod 700 ~/.ssh"
sudo -u {{ .SharedUser }} bash -c 'echo "{{ .PublicKey }}" >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys'

sudo touch {{ .OSInitializedFile }}
`

// writeStartupScript writes the startup script to a temp file.
// Returns the path to the file.
// After use, the caller should delete the temp file.
//
// extraMountOpts, if not empty, is appended to the default mount options. It is
// a comma-separated list of options for the "mount -o" flag.
func writeStartupScript(
	extraMountOpts string, fileSystem string, useMultiple bool, enableFIPS bool, enableCron bool,
) (string, error) {
	type tmplParams struct {
		ExtraMountOpts       string
		UseMultipleDisks     bool
		Zfs                  bool
		EnableFIPS           bool
		SharedUser           string
		PublicKey            string
		EnableCron           bool
		OSInitializedFile    string
		DisksInitializedFile string
	}

	publicKey, err := config.SSHPublicKey()
	if err != nil {
		return "", err
	}

	args := tmplParams{
		ExtraMountOpts:       extraMountOpts,
		UseMultipleDisks:     useMultiple,
		Zfs:                  fileSystem == vm.Zfs,
		EnableFIPS:           enableFIPS,
		SharedUser:           config.SharedUser,
		PublicKey:            publicKey,
		EnableCron:           enableCron,
		OSInitializedFile:    vm.OSInitializedFile,
		DisksInitializedFile: vm.DisksInitializedFile,
	}

	tmpfile, err := os.CreateTemp("", "gce-startup-script")
	if err != nil {
		return "", err
	}
	defer tmpfile.Close()

	t := template.Must(template.New("start").Parse(gceDiskStartupScriptTemplate))
	if err := t.Execute(tmpfile, args); err != nil {
		return "", err
	}
	return tmpfile.Name(), nil
}

// SyncDNS replaces the configured DNS zone with the supplied hosts.
func SyncDNS(l *logger.Logger, vms vm.List) error {
	if Subdomain == "" {
		return nil
	}

	f, err := os.CreateTemp(os.ExpandEnv("$HOME/.roachprod/"), "dns.bind")
	if err != nil {
		return err
	}
	defer f.Close()

	// Keep imported zone file in dry run mode.
	defer func() {
		if err := os.Remove(f.Name()); err != nil {
			l.Errorf("removing %s failed: %v", f.Name(), err)
		}
	}()

	var zoneBuilder strings.Builder
	for _, vm := range vms {
		entry, err := vm.ZoneEntry()
		if err != nil {
			l.Printf("WARN: skipping: %s\n", err)
			continue
		}
		zoneBuilder.WriteString(entry)
	}
	fmt.Fprint(f, zoneBuilder.String())
	f.Close()

	args := []string{"--project", dnsProject, "dns", "record-sets", "import",
		f.Name(), "-z", dnsZone, "--delete-all-existing", "--zone-file-format"}
	cmd := exec.Command("gcloud", args...)
	output, err := cmd.CombinedOutput()

	return errors.Wrapf(err, "Command: %s\nOutput: %s\nZone file contents:\n%s", cmd, output, zoneBuilder.String())
}

type AuthorizedKey struct {
	User    string
	Key     ssh.PublicKey
	Comment string
}

// Format formats an authorized key for display. When `maxLen` is 0,
// we return the entire key, truncating it to that length
// otherwise. The comment associated with the key, if available, is
// always displayed.
func (k AuthorizedKey) Format(maxLen int) string {
	formatted := string(ssh.MarshalAuthorizedKey(k.Key))
	// Drop new line character if present. We add it when formatting a
	// set of keys in `AsSSSH` or `AsProjectMetadata`.
	formatted = strings.TrimSuffix(formatted, "\n")

	if maxLen > 0 {
		formatted = formatted[:maxLen] + "..."
	}

	var comment string
	if k.Comment != "" {
		comment = fmt.Sprintf(" %s", k.Comment)
	}

	return fmt.Sprintf("%s%s", formatted, comment)
}

func (k AuthorizedKey) String() string {
	return k.Format(0)
}

type AuthorizedKeys []AuthorizedKey

// AsSSH returns a marshaled version of the authorized keys in a
// format that can be used as SSH's `authorized_keys` file.
func (ak AuthorizedKeys) AsSSH() []byte {
	var buf bytes.Buffer

	for _, k := range ak {
		buf.WriteString(k.String() + "\n")
	}

	return buf.Bytes()
}

// AsProjectMetadata returns a marshaled version of the authorized
// keys in a format that can be pushed to GCE's project metadata
// storage.
func (ak AuthorizedKeys) AsProjectMetadata() []byte {
	var buf bytes.Buffer

	for _, k := range ak {
		buf.WriteString(fmt.Sprintf("%s:%s\n", k.User, k.String()))
	}

	return buf.Bytes()
}

// GetUserAuthorizedKeys retrieves reads a list of user public keys from the
// gcloud cockroach-ephemeral project and returns them formatted for use in
// an authorized_keys file.
func GetUserAuthorizedKeys() (AuthorizedKeys, error) {
	var outBuf bytes.Buffer
	// The below command will return a stream of user:pubkey as text.
	cmd := exec.Command("gcloud", "compute", "project-info", "describe",
		fmt.Sprintf("--project=%s", DefaultProject()),
		"--format=value(commonInstanceMetadata.ssh-keys)")
	cmd.Stderr = os.Stderr
	cmd.Stdout = &outBuf

	if err := cmd.Run(); err != nil {
		return nil, err
	}

	var authorizedKeys AuthorizedKeys
	scanner := bufio.NewScanner(&outBuf)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		colonIdx := strings.IndexRune(line, ':')
		if colonIdx == -1 {
			return nil, fmt.Errorf("malformed public key line %q", line)
		}

		user := line[:colonIdx]
		key := line[colonIdx+1:]

		if !isValidSSHUser(user) {
			continue
		}

		pubKey, comment, _, _, err := ssh.ParseAuthorizedKey([]byte(key))
		if err != nil {
			return nil, fmt.Errorf("failed to parse public key in project metadata: %w\n%s", err, key)
		}
		authorizedKeys = append(authorizedKeys, AuthorizedKey{User: user, Key: pubKey, Comment: comment})
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read public keys from project metadata: %w", err)
	}

	// For consistency, return keys sorted by username.
	sort.Slice(authorizedKeys, func(i, j int) bool {
		return authorizedKeys[i].User < authorizedKeys[j].User
	})

	return authorizedKeys, nil
}

// AddUserAuthorizedKey adds the authorized key provided to the set of
// keys installed on clusters managed by roachprod. Currently, these
// keys are stored in the project metadata for the roachprod's
// `DefaultProject`.
func AddUserAuthorizedKey(ak AuthorizedKey) error {
	existingKeys, err := GetUserAuthorizedKeys()
	if err != nil {
		return err
	}

	if !isValidSSHUser(ak.User) {
		return fmt.Errorf("invalid SSH key username: %s", ak.User)
	}

	newKeys := append(existingKeys, ak)
	return SetUserAuthorizedKeys(newKeys)
}

// SetUserAuthorizedKeys updates the default project metadata with the
// keys provided. Note that this overwrites any existing keys -- all
// existing keys need to be passed in the `keys` list provided in
// order for them to continue to exist after this function is called.
func SetUserAuthorizedKeys(keys AuthorizedKeys) (retErr error) {
	tmpFile, err := os.CreateTemp("", "ssh-keys-*")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer func() {
		retErr = errors.CombineErrors(retErr, os.Remove(tmpFile.Name()))
	}()

	if err := os.WriteFile(tmpFile.Name(), keys.AsProjectMetadata(), 0444); err != nil {
		return fmt.Errorf("failed to write to temp file: %w", err)
	}

	cmd := exec.Command("gcloud", "compute", "project-info", "add-metadata",
		fmt.Sprintf("--project=%s", DefaultProject()),
		fmt.Sprintf("--metadata-from-file=ssh-keys=%s", tmpFile.Name()),
	)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("error running `gcloud` command (output above): %w", err)
	}

	return nil
}

// isValidSSHUser returns whether the username provided is a valid
// username for the purposes of the shared pool of SSH public keys to
// be added to clusters. We don't add public keys to the root user or
// the shared user (the shared user's `authorized_keys` is managed by
// roachprod).
func isValidSSHUser(user string) bool {
	return user != config.RootUser && user != config.SharedUser
}

// Extracted from https://cloud.google.com/compute/docs/regions-zones#available
var SupportedT2AZones = []string{
	"asia-southeast1-b", "asia-southeast1-c",
	"europe-west4-a", "europe-west4-b", "europe-west4-c",
	"us-central1-a", "us-central1-b", "us-central1-f",
}

// Used mainly in support of https://github.com/cockroachdb/cockroach/issues/122035.
func IsSupportedT2AZone(zones []string) bool {
	for _, zone := range zones {
		if slices.Index(SupportedT2AZones, zone) == -1 {
			return false
		}
	}
	return true
}
