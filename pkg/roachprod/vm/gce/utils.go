// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
	"golang.org/x/crypto/ssh"
)

const gceDiskStartupScriptTemplate = `#!/usr/bin/env bash
# Script for setting up a GCE machine for roachprod use.

function setup_disks() {
	first_setup=$1

{{ if not .Zfs }}
	mount_opts="defaults,nofail"
	{{if .ExtraMountOpts}}mount_opts="${mount_opts},{{.ExtraMountOpts}}"{{end}}
{{ end }}

	use_multiple_disks='{{if .UseMultipleDisks}}true{{end}}'

	mount_prefix="/mnt/data"

	# if the use_multiple_disks is not set and there are more than 1 disk (excluding the boot disk),
	# then the disks will be selected for RAID'ing. If there are both Local SSDs and Persistent disks,
	# RAID'ing in this case can cause performance differences. So, to avoid this, local SSDs are ignored.
	# Scenarios:
	#   (local SSD = 0, Persistent Disk - 1) - no RAID'ing and Persistent Disk mounted
	#   (local SSD = 1, Persistent Disk - 0) - no RAID'ing and local SSD mounted
	#   (local SSD >= 1, Persistent Disk = 1) - no RAID'ing and Persistent Disk mounted
	#   (local SSD > 1, Persistent Disk = 0) - local SSDs selected for RAID'ing
	#   (local SSD >= 0, Persistent Disk > 1) - network disks selected for RAID'ing
	local_or_persistent=()
	disks=()

{{ if .Zfs }}
	apt-get update -q
	apt-get install -yq zfsutils-linux
{{ end }}

	# N.B. we assume 0th disk is the boot disk.
	if [ "$(ls /dev/disk/by-id/google-persistent-disk-[1-9]|wc -l)" -gt "0" ]; then
		local_or_persistent=$(ls /dev/disk/by-id/google-persistent-disk-[1-9])
		echo "Using only persistent disks: ${local_or_persistent[@]}"
	else
		local_or_persistent=$(ls /dev/disk/by-id/google-local-*)
		echo "Using only local disks: ${local_or_persistent[@]}"
	fi

	for l in ${local_or_persistent}; do
		d=$(readlink -f $l)
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

{{ template "head_utils" . }}
{{ template "apt_packages" . }}

sudo -u {{ .SharedUser }} bash -c "mkdir -p ~/.ssh && chmod 700 ~/.ssh"
sudo -u {{ .SharedUser }} bash -c 'echo "{{ .PublicKey }}" >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys'

if [ -e {{ .OSInitializedFile }} ]; then
  echo "OS already initialized, only initializing disks."
  # Initialize disks, but don't write fstab entries again.
  setup_disks false
  exit 0
fi

# Initialize disks and write fstab entries.
setup_disks true

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
	extraMountOpts string, fileSystem string, useMultiple bool, enableFIPS bool, enableCron bool,
) (string, error) {
	type tmplParams struct {
		vm.StartupArgs
		ExtraMountOpts   string
		UseMultipleDisks bool
		PublicKey        string
	}

	publicKey, err := config.SSHPublicKey()
	if err != nil {
		return "", err
	}

	args := tmplParams{
		StartupArgs: vm.DefaultStartupArgs(
			vm.WithSharedUser(config.SharedUser),
			vm.WithEnableCron(enableCron),
			vm.WithZfs(fileSystem == vm.Zfs),
			vm.WithEnableFIPS(enableFIPS),
			vm.WithChronyServers([]string{"metadata.google.internal"}),
		),
		ExtraMountOpts:   extraMountOpts,
		UseMultipleDisks: useMultiple,
		PublicKey:        publicKey,
	}

	tmpfile, err := os.CreateTemp("", "gce-startup-script")
	if err != nil {
		return "", err
	}
	defer tmpfile.Close()

	err = vm.GenerateStartupScript(tmpfile, gceDiskStartupScriptTemplate, args)
	if err != nil {
		return "", errors.Wrapf(err, "unable to generate startup script")
	}

	return tmpfile.Name(), nil
}

// SyncDNS implements the InfraProvider interface.
func (p *Provider) SyncDNS(l *logger.Logger, vms vm.List) error {
	return p.dnsProvider.syncPublicDNS(l, vms)
}

// DNSDomain implements the InfraProvider interface.
func (p *Provider) DNSDomain() string {
	return p.dnsProvider.publicDomain
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

// GetUserAuthorizedKeys implements the InfraProvider interface.
func (p *Provider) GetUserAuthorizedKeys() (AuthorizedKeys, error) {
	var outBuf bytes.Buffer
	// The below command will return a stream of user:pubkey as text.
	cmd := exec.Command("gcloud", "compute", "project-info", "describe",
		"--project="+p.metadataProject,
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
		// N.B. Below, we skip over invalid public keys as opposed to failing. Since we don't control how these keys are
		// uploaded, it's possible for a key to become invalid.
		// N.B. This implies that an operation like `AddUserAuthorizedKey` has the side effect of removing invalid
		// keys, since they are skipped here, and the result is then uploaded via `SetUserAuthorizedKeys`.
		colonIdx := strings.IndexRune(line, ':')
		if colonIdx == -1 {
			fmt.Fprintf(os.Stderr, "WARN: malformed public key line %q\n", line)
			continue
		}

		user := line[:colonIdx]
		key := line[colonIdx+1:]

		if !isValidSSHUser(user) {
			continue
		}

		pubKey, comment, _, _, err := ssh.ParseAuthorizedKey([]byte(key))
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARN: failed to parse public key in project metadata: %v\n%q\n", err, key)
			continue
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
	existingKeys, err := Infrastructure.GetUserAuthorizedKeys()
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
