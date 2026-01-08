// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vm

import (
	"fmt"
	"io"
	"text/template"

	"github.com/cockroachdb/errors"
)

const (
	// GrafanaEnterpriseVersion is the version of Grafana Enterprise installed
	// during the grafana-start command.
	GrafanaEnterpriseVersion = "9.2.3"
	// PrometheusVersion is the version of Prometheus installed during the
	// grafana-start command.
	PrometheusVersion = "2.27.1"
	// NodeExporterVersion is the version of NodeExporter installed on each node
	// in the startup script and during the grafana-start command.
	NodeExporterVersion = "1.2.2"
	// NodeExporterPort is the port that NodeExporter listens on.
	NodeExporterPort = 9100
	// NodeExporterMetricsPath is the path that NodeExporter serves metrics on.
	NodeExporterMetricsPath = "/metrics"
	// EbpfExporterVersion is the version of EbpfExporter installed on each node
	// in the startup script.
	EbpfExporterVersion = "2.4.2"
	// EbpfExporterPort is the port that EbpfExporter listens on.
	EbpfExporterPort = 9435
	// EbpfExporterMetricsPath is the path that EbpfExporter serves metrics on.
	EbpfExporterMetricsPath = "/metrics"

	// WorkloadMetricsPortMin and WorkloadMetricsPortMax are the range of ports
	// used by workload to expose metrics.
	WorkloadMetricsPortMin = 2112
	WorkloadMetricsPortMax = 2120
	// WorkloadMetricsPath is the path that workload serves metrics on.
	WorkloadMetricsPath = "/metrics"

	// DefaultSharedUser is the default user that is shared across all VMs.
	DefaultSharedUser = "ubuntu"
	// InitializedFile is the base name of the initialization paths defined below.
	InitializedFile = ".roachprod-initialized"
	// OSInitializedFile is a marker file that is created on a VM to indicate
	// that it has been initialized at least once by the VM start-up script. This
	// is used to avoid re-initializing a VM that has been stopped and restarted.
	OSInitializedFile = "/" + InitializedFile
	// DisksInitializedFile is a marker file that is created on a VM to indicate
	// that the disks have been initialized by the VM start-up script. This is
	// separate from OSInitializedFile, because the disks may be ephemeral and
	// need to be re-initialized on every start. The presence of this file
	// automatically implies the presence of OSInitializedFile.
	DisksInitializedFile = "/mnt/data1/" + InitializedFile
	// StartupLogs is a log file that is created on a VM to redirect startup script
	// output logs.
	StartupLogs = "/var/log/roachprod_startup.log"
)

type StartupArgs struct {
	VMName               string     // Name of the VM
	SharedUser           string     // The name of the shared user
	StartupLogs          string     // File to redirect startup script output logs
	OSInitializedFile    string     // File to touch when OS is initialized
	DisksInitializedFile string     // File to touch when disks are initialized
	BootDiskOnly         bool       // Whether to use only the boot disk
	UseMultipleDisks     bool       // Whether to use multiple disks
	ExtraMountOpts       string     // Extra mount options for disks (if filesystem is not ZFS)
	Filesystem           Filesystem // Filesystem to use for disks: ext4, zfs, xfs
	EnableFIPS           bool       // Enable FIPS mode
	EnableCron           bool       // Enable cron service
	ChronyServers        []string   // List of NTP servers to use
	NodeExporterPort     int        // Port that NodeExporter listens on
	EbpfExporterPort     int        // Port that EbpfExporter listens on
}

func DefaultStartupArgs(overrides ...IArgOverride) StartupArgs {
	startupArgs := StartupArgs{
		SharedUser:           DefaultSharedUser,
		StartupLogs:          StartupLogs,
		OSInitializedFile:    OSInitializedFile,
		DisksInitializedFile: DisksInitializedFile,
		ChronyServers: []string{
			"time1.google.com",
			"time2.google.com",
			"time3.google.com",
			"time4.google.com",
		},
		NodeExporterPort: NodeExporterPort,
		EbpfExporterPort: EbpfExporterPort,
	}
	for _, override := range overrides {
		override.apply(&startupArgs)
	}
	return startupArgs
}

var (
	// StartupScriptTemplateParts is the template for the startup script.
	StartupScriptTemplateParts = map[string]string{
		"apt_packages":          startupScriptAptPackages,
		"chrony_utils":          startupScriptChrony,
		"core_dumps_utils":      startupScriptCoreDumps,
		"cron_utils":            startupScriptCron,
		"fips_utils":            startupScriptFIPS,
		"head_utils":            startupScriptHead,
		"hostname_utils":        startupScriptHostname,
		"setup_disks_utils":     startupScriptMountDisks,
		"node_exporter":         startupScriptNodeExporter,
		"ebpf_exporter":         startupScriptEbpfExporter,
		"keepalives":            startupScriptKeepalives,
		"ssh_utils":             startupScriptSSH,
		"tcpdump":               startupScriptTcpdump,
		"timers_services_utils": startupScriptTimersAndServices,
		"ulimits":               startupScriptUlimits,
	}
)

const startupScriptAptPackages = `
sudo apt-get update -q
sudo apt-get install -qy --no-install-recommends mdadm
{{ if eq .Filesystem "zfs" }}
apt-get install -yq zfsutils-linux
{{ else if eq .Filesystem "f2fs" }}
apt-get install -yq f2fs-tools linux-modules-extra-$(uname -r)
{{ else if eq .Filesystem "btrfs" }}
apt-get install -yq btrfs-progs
{{ else if eq .Filesystem "xfs" }}
apt-get install -yq xfsprogs
{{ end }}`

const startupScriptChrony = `
sudo apt-get install -qy chrony

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
{{ range .ChronyServers -}}
server {{.}} prefer iburst
{{ end -}}
makestep 0.1 3
EOF

sudo /etc/init.d/chrony restart
sudo chronyc -a waitsync 30 0.01 | sudo tee -a /root/chrony.log`

const startupScriptCoreDumps = `
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
mkdir -p /mnt/data1/cores
chmod a+w /mnt/data1/cores
exec /bin/gzip -f - > /mnt/data1/cores/core.$1.$2.$3.$4.gz
EOF
chmod +x /bin/gzip_core.sh

CORE_PATTERN="|/bin/gzip_core.sh %e %p %h %t"
echo "$CORE_PATTERN" > /proc/sys/kernel/core_pattern
sed -i'~' 's/enabled=1/enabled=0/' /etc/default/apport
sed -i'~' '/.*kernel\\.core_pattern.*/c\\' /etc/sysctl.conf
echo "kernel.core_pattern=$CORE_PATTERN" >> /etc/sysctl.conf

sysctl --system  # reload sysctl settings`

const startupScriptCron = `
{{ if not .EnableCron }}
systemctl stop cron
systemctl mask cron
{{ end }}`

const startupScriptFIPS = `
{{ if .EnableFIPS }}
sudo apt-get install -yq ubuntu-advantage-tools jq
# Enable FIPS (in practice, it's often already enabled at this point).
fips_status=$(sudo pro status --format json | jq '.services[] | select(.name == "fips") | .status')
if [ "$fips_status" != '"enabled"' ]; then
	sudo ua enable fips --assume-yes
fi
{{ end }}`

const startupScriptHead = `
# ensure any failure fails the entire script
set -eux

# Redirect output to stdout/err and a log file
exec &> >(tee -a {{ .StartupLogs }})

# Log the startup of the script with a timestamp
echo "startup script starting: $(date -u)"

# If disks are already initialized, exit early.
# This happens upon VM restart if the disks are persistent.
if [ -e {{ .DisksInitializedFile }} ]; then
	echo "Already initialized, exiting."
	exit 0
fi

# Uninstall some packages to prevent them running cronjobs and similar jobs in
# parallel Check if the service exists before trying to stop it, because it may
# have been removed during a previous invocation of this script.
if systemctl list-unit-files | grep -q '^unattended-upgrades.service'; then
    echo "unattended-upgrades service exists. Proceeding with uninstallation."
    systemctl stop unattended-upgrades
    sudo rm -rf /var/log/unattended-upgrades
    apt-get purge -y unattended-upgrades
fi`

const startupScriptHostname = `
# set hostname according to the name used by roachprod. There's host
# validation logic that relies on this -- see comment on cluster_synced.go
{{ if .VMName }}
sudo hostnamectl set-hostname {{.VMName}}
{{ end }}`

const startupScriptKeepalives = `
# Send TCP keepalives every minute since GCE will terminate idle connections
# after 10m. Note that keepalives still need to be requested by the application
# with the SO_KEEPALIVE socket option.
cat <<EOF > /etc/sysctl.d/99-roachprod-tcp-keepalive.conf
net.ipv4.tcp_keepalive_time=60
net.ipv4.tcp_keepalive_intvl=60
net.ipv4.tcp_keepalive_probes=5
EOF

sysctl --system  # reload sysctl settings`

// This installs node_exporter and starts it.
// It also sets up firewall rules so that only localhost, the internal network
// and prometheus.testeng.crdb.io can scrape system metrics.
const startupScriptNodeExporter = `
# Add and start node_exporter, only authorize scrapping from internal network.
export ARCH=$(dpkg --print-architecture)
export DEFAULT_USER_HOME="/home/$(id -nu 1000)"
mkdir -p ${DEFAULT_USER_HOME}/node_exporter && curl -fsSL \
	https://storage.googleapis.com/cockroach-test-artifacts/prometheus/node_exporter-` + NodeExporterVersion + `.linux-${ARCH}.tar.gz |
	tar zxv --strip-components 1 -C ${DEFAULT_USER_HOME}/node_exporter \
	&& chown -R 1000:1000 ${DEFAULT_USER_HOME}/node_exporter

# Create iptables setup script for node_exporter
export SCRAPING_PUBLIC_IPS=$(dig +short prometheus.testeng.crdb.io | awk '{printf "%s%s",sep,$0; sep=","} END {print ""}')
cat <<FIREWALL_EOF | sudo tee /usr/local/bin/setup-node-exporter-firewall.sh
#!/bin/bash
# Remove existing rules for this port to avoid duplicates
/usr/sbin/iptables -D INPUT -p tcp -s 127.0.0.1,10.0.0.0/8,${SCRAPING_PUBLIC_IPS} --dport {{.NodeExporterPort}} -j ACCEPT 2>/dev/null || true
/usr/sbin/iptables -D INPUT -p tcp --dport {{.NodeExporterPort}} -j DROP 2>/dev/null || true
# Add firewall rules
/usr/sbin/iptables -A INPUT -p tcp -s 127.0.0.1,10.0.0.0/8,${SCRAPING_PUBLIC_IPS} --dport {{.NodeExporterPort}} -j ACCEPT
/usr/sbin/iptables -A INPUT -p tcp --dport {{.NodeExporterPort}} -j DROP
FIREWALL_EOF
sudo chmod +x /usr/local/bin/setup-node-exporter-firewall.sh

# Create systemd service file
cat <<EOF | sudo tee /etc/systemd/system/node_exporter.service
[Unit]
Description=Prometheus Node Exporter
After=network.target

[Service]
Type=simple
User=$(id -nu 1000)
PermissionsStartOnly=true
WorkingDirectory=${DEFAULT_USER_HOME}/node_exporter
ExecStartPre=/usr/local/bin/setup-node-exporter-firewall.sh
ExecStart=${DEFAULT_USER_HOME}/node_exporter/node_exporter \
	--collector.systemd \
	--collector.interrupts \
	--collector.processes \
	--web.listen-address=":{{.NodeExporterPort}}" \
	--web.telemetry-path="` + NodeExporterMetricsPath + `"
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
EOF

# Enable and start the service
sudo systemctl daemon-reload
sudo systemctl enable node_exporter
sudo systemctl start node_exporter`

const startupScriptEbpfExporter = `
# Add and start ebpf_exporter, only authorize scrapping from internal network.
export ARCH=$(dpkg --print-architecture)
export DEFAULT_USER_HOME="/home/$(id -nu 1000)"
mkdir -p ${DEFAULT_USER_HOME}/ebpf_exporter && curl -fsSL \
	https://storage.googleapis.com/cockroach-test-artifacts/prometheus/ebpf_exporter-` + EbpfExporterVersion + `.linux-${ARCH}.tar.gz |
	tar zxv --strip-components 1 -C ${DEFAULT_USER_HOME}/ebpf_exporter \
	&& chown -R 1000:1000 ${DEFAULT_USER_HOME}/ebpf_exporter

# Create iptables setup script for ebpf_exporter
export SCRAPING_PUBLIC_IPS=$(dig +short prometheus.testeng.crdb.io | awk '{printf "%s%s",sep,$0; sep=","} END {print ""}')
cat <<FIREWALL_EOF | sudo tee /usr/local/bin/setup-ebpf-exporter-firewall.sh
#!/bin/bash
# Remove existing rules for this port to avoid duplicates
/usr/sbin/iptables -D INPUT -p tcp -s 127.0.0.1,10.0.0.0/8,${SCRAPING_PUBLIC_IPS} --dport {{.EbpfExporterPort}} -j ACCEPT 2>/dev/null || true
/usr/sbin/iptables -D INPUT -p tcp --dport {{.EbpfExporterPort}} -j DROP 2>/dev/null || true
# Add firewall rules
/usr/sbin/iptables -A INPUT -p tcp -s 127.0.0.1,10.0.0.0/8,${SCRAPING_PUBLIC_IPS} --dport {{.EbpfExporterPort}} -j ACCEPT
/usr/sbin/iptables -A INPUT -p tcp --dport {{.EbpfExporterPort}} -j DROP
FIREWALL_EOF
sudo chmod +x /usr/local/bin/setup-ebpf-exporter-firewall.sh

# Create systemd service file
cat <<EOF | sudo tee /etc/systemd/system/ebpf_exporter.service
[Unit]
Description=Prometheus eBPF Exporter
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=${DEFAULT_USER_HOME}/ebpf_exporter
ExecStartPre=/usr/local/bin/setup-ebpf-exporter-firewall.sh
ExecStart=${DEFAULT_USER_HOME}/ebpf_exporter/ebpf_exporter \
	--config.dir=examples \
	--config.names=biolatency,timers,sched-trace,syscalls,uprobe \
	--web.listen-address=":{{.EbpfExporterPort}}" \
	--web.telemetry-path="` + EbpfExporterMetricsPath + `"
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
EOF

# Enable and start the service
sudo systemctl daemon-reload
sudo systemctl enable ebpf_exporter
sudo systemctl start ebpf_exporter`

const startupScriptSSH = `
# sshguard can prevent frequent ssh connections to the same host. Disable it.
if systemctl is-active --quiet sshguard; then
	systemctl stop sshguard
fi
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

sudo sed -i 's/#LoginGraceTime .*/LoginGraceTime 0/g' /etc/ssh/sshd_config
sudo sed -i 's/TCPKeepAlive no/TCPKeepAlive yes/g' /etc/ssh/sshd_config

sudo service sshd restart
sudo service ssh restart`

const startupScriptTcpdump = `
# N.B. Ubuntu 22.04 changed the location of tcpdump to /usr/bin. Since existing tooling, e.g.,
# jepsen uses /usr/sbin, we create a symlink.
# See https://ubuntu.pkgs.org/22.04/ubuntu-main-amd64/tcpdump_4.99.1-3build2_amd64.deb.html
# FIPS is still on Ubuntu 20.04 however, so don't create if using FIPS.
{{ if not .EnableFIPS }}
sudo ln -s /usr/bin/tcpdump /usr/sbin/tcpdump
{{ end }}`

const startupScriptTimersAndServices = `
for timer in apt-daily-upgrade.timer apt-daily.timer e2scrub_all.timer fstrim.timer man-db.timer e2scrub_all.timer ; do
	systemctl mask $timer
done

for service in apport.service atd.service; do
	if systemctl is-active --quiet $service; then
		systemctl stop $service
	fi
	systemctl mask $service
done`

const startupScriptUlimits = `
# increase the default maximum number of open file descriptors for
# root and non-root users. Load generators running a lot of concurrent
# workers bump into this often.
sudo sh -c 'echo "root - nofile 1048576\n* - nofile 1048576" > /etc/security/limits.d/10-roachprod-nofiles.conf'`

const startupScriptMountDisks = `
# Common disk setup logic shared across all cloud providers.
# This function formats, mounts, and configures disks (including RAID0/ZFS if needed).
# Args:
#   $1 = first_setup ("true" or "false") - controls whether to write fstab entries
#   $2 = filesystem (e.g., "ext4", "zfs", "btrfs")
#   $3 = mount_prefix (e.g., "/mnt/data")
#   $4 = mount_opts (e.g., "defaults,nofail")
#   $5 = use_multiple_disks ("true" or empty) - mount disks individually vs RAID0/ZFS
#   $6+ = disk paths to setup
# Usage: setup_disks "true" "ext4" "/mnt/data" "defaults,nofail" "" "${disks[@]}"
function setup_disks() {
	local first_setup=$1
	local filesystem=$2
	local mount_prefix=$3
	local mount_opts=$4
	local use_multiple_disks=$5
	shift 5
	local disks=("$@")

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
			if [ ${filesystem} == "zfs" ]; then
				zpool create -f $(basename $mountpoint) -m ${mountpoint} ${disk}
				# NOTE: we don't need an /etc/fstab entry for ZFS. It will handle this itself.
			elif [ ${filesystem} == "btrfs" ]; then
				mkfs.btrfs -f -L data${disknum} "${disk}"
				mount -t btrfs -L data${disknum} ${mountpoint}
				if [ "$first_setup" = "true" ]; then
					echo "LABEL=data${disknum} ${mountpoint} btrfs ${mount_opts} 0 0" | tee -a /etc/fstab
				fi
			else
				mkfsForceOpt="-f"
				if [ ${filesystem} == "ext4" ]; then
					mkfsForceOpt="-F"
				fi
	
				mkfs.{{ .Filesystem }} -q ${mkfsForceOpt} ${disk}
				mount -o ${mount_opts} ${disk} ${mountpoint}
				if [ "$first_setup" = "true" ]; then
					echo "${disk} ${mountpoint} {{ .Filesystem }} ${mount_opts} 0 2" | tee -a /etc/fstab
				fi
			fi
			chmod 777 ${mountpoint}
		done
	else
		mountpoint="${mount_prefix}1"
		echo "${#disks[@]} disks mounted, creating ${mountpoint} using RAID 0"
		mkdir -p ${mountpoint}

		if [ ${filesystem} == "zfs" ]; then
			zpool create -f $(basename $mountpoint) -m ${mountpoint} ${disks[@]}
			# NOTE: we don't need an /etc/fstab entry for ZFS. It will handle this itself.
		elif [ ${filesystem} == "btrfs" ]; then
			mkfs.btrfs -f -L data1 -d raid0 -m raid0 "${disks[@]}"
			mount -t btrfs -L data1 ${mountpoint}
			if [ "$first_setup" = "true" ]; then
				echo "LABEL=data1 ${mountpoint} btrfs ${mount_opts} 0 0" | tee -a /etc/fstab
			fi
		else
			mkfsForceOpt="-f"
			if [ ${filesystem} == "ext4" ]; then
				mkfsForceOpt="-F"
			fi

			raiddisk="/dev/md0"
			mdadm --create ${raiddisk} --level=0 --raid-devices=${#disks[@]} "${disks[@]}"
			mkfs.{{ .Filesystem }} -q ${mkfsForceOpt} ${raiddisk}
			mount -o ${mount_opts} ${raiddisk} ${mountpoint}
			if [ "$first_setup" = "true" ]; then
				echo "${raiddisk} ${mountpoint} {{ .Filesystem }} ${mount_opts} 0 2" | tee -a /etc/fstab
			fi

			if [ ${filesystem} == "ext4" ]; then
				tune2fs -m 0 ${raiddisk}
			fi
		fi

		chmod 777 ${mountpoint}
	fi

	# Print the block device and FS usage output. This is useful for debugging.
	lsblk
	df -h
	if [ ${filesystem} == "zfs" ]; then
		zpool list
	fi

	sudo touch {{ .DisksInitializedFile }}
}
	
function setup_disks_wrapper() {
	first_setup=$1

{{ if .BootDiskOnly }}
	mkdir -p /mnt/data1 && chmod 777 /mnt/data1
	echo "VM has no disk attached other than the boot disk."
	return 0
{{ end }}

	# Define various parameters
	mount_prefix="/mnt/data"
	use_multiple_disks='{{ if .UseMultipleDisks }}true{{ end }}'
	filesystem='{{ .Filesystem }}'

	mount_opts="defaults,nofail{{- if .ExtraMountOpts -}},{{- .ExtraMountOpts -}}{{- end -}}"
	if [ "${filesystem}" == "btrfs" ]; then
		mount_opts="${mount_opts},noatime,noautodefrag"
	fi
	
	# Detect disks
	mapfile -t disks < <(detect_disks)

	# Call shared setup logic
	setup_disks "${first_setup}" "${filesystem}" "${mount_prefix}" "${mount_opts}" "${use_multiple_disks}" "${disks[@]}"
}

# Start the process of detecting disks to mount and configure them.
if [ -e {{ .OSInitializedFile }} ]; then
  echo "OS already initialized, only initializing disks."
  # Initialize disks, but don't write fstab entries again.
  setup_disks_wrapper false
  exit 0
fi

# Initialize disks and write fstab entries.
setup_disks_wrapper true`

func GenerateStartupScript(w io.Writer, cloudSpecificTemplate string, args any) error {
	t := template.New("start")
	for name, tmpl := range StartupScriptTemplateParts {
		var err error

		t, err = t.Parse(
			fmt.Sprintf(`
{{define "%s"}}
%s
{{end}}`, name, tmpl),
		)
		if err != nil {
			return errors.Wrapf(err, "failed to parse generic template %s", name)
		}
	}
	t, err := t.Parse(cloudSpecificTemplate)
	if err != nil {
		return errors.Wrap(err, "failed to parse cloud-specific template")
	}

	err = t.Execute(w, args)
	if err != nil {
		return errors.Wrap(err, "failed to execute template")
	}

	return nil
}
