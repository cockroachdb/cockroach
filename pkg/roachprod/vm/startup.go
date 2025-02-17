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
	VMName               string   // Name of the VM
	SharedUser           string   // The name of the shared user
	StartupLogs          string   // File to redirect startup script output logs
	OSInitializedFile    string   // File to touch when OS is initialized
	DisksInitializedFile string   // File to touch when disks are initialized
	Zfs                  bool     // Use ZFS instead of ext4
	EnableFIPS           bool     // Enable FIPS mode
	EnableCron           bool     // Enable cron service
	ChronyServers        []string // List of NTP servers to use
	NodeExporterPort     int      // Port that NodeExporter listens on
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
		"node_exporter":         startupScriptNodeExporter,
		"keepalives":            startupScriptKeepalives,
		"ssh_utils":             startupScriptSSH,
		"tcpdump":               startupScriptTcpdump,
		"timers_services_utils": startupScriptTimersAndServices,
		"ulimits":               startupScriptUlimits,
	}
)

const startupScriptAptPackages = `
sudo apt-get update -q
sudo apt-get install -qy --no-install-recommends mdadm`

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
# Uninstall some packages to prevent them running cronjobs and similar jobs in parallel
systemctl stop unattended-upgrades
sudo rm -rf /var/log/unattended-upgrades
apt-get purge -y unattended-upgrades

{{ if not .EnableCron }}
systemctl stop cron
systemctl mask cron
{{ end }}`

const startupScriptFIPS = `
{{ if .EnableFIPS }}
sudo apt-get install -yq ubuntu-advantage-tools jq
# Enable FIPS (in practice, it's often already enabled at this point).
if [ $(sudo pro status --format json | jq '.services[] | select(.name == "fips") | .status') != '"enabled"' ]; then
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

if [ -e {{ .DisksInitializedFile }} ]; then
	echo "Already initialized, exiting."
	exit 0
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

sudo iptables -A INPUT -s 127.0.0.1,10.0.0.0/8,prometheus.testeng.crdb.io -p tcp --dport {{.NodeExporterPort}} -j ACCEPT
sudo iptables -A INPUT -p tcp --dport {{.NodeExporterPort}} -j DROP
(
	chown -R 1000:1000 ${DEFAULT_USER_HOME}/node_exporter && \
	cd ${DEFAULT_USER_HOME}/node_exporter && \
	sudo systemd-run --unit node_exporter --same-dir \
		./node_exporter --collector.systemd --collector.interrupts --collector.processes \
		--web.listen-address=":{{.NodeExporterPort}}" \
		--web.telemetry-path="` + NodeExporterMetricsPath + `"
)`

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
