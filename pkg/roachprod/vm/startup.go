// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vm

import (
	"crypto/sha256"
	"fmt"
	"io"
	"sort"
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

type StartupScriptMode int

const (
	StartupScriptPreBakingOnly StartupScriptMode = iota
	StartupScriptRuntimeOnly
	StartupScriptAll
)

type StartupArgs struct {
	VMName               string            // Name of the VM
	SharedUser           string            // The name of the shared user
	StartupLogs          string            // File to redirect startup script output logs
	OSInitializedFile    string            // File to touch when OS is initialized
	DisksInitializedFile string            // File to touch when disks are initialized
	Zfs                  bool              // Use ZFS instead of ext4
	EnableFIPS           bool              // Enable FIPS mode
	EnableCron           bool              // Enable cron service
	ChronyServers        []string          // List of NTP servers to use
	NodeExporterPort     int               // Port that NodeExporter listens on
	EbpfExporterPort     int               // Port that EbpfExporter listens on
	StartupScriptMode    StartupScriptMode // Whether we are generating the script for pre-baking an image or runtime
}

type StartupScriptModeGetter interface {
	getStartupScriptMode() StartupScriptMode
}

func (startupArgs StartupArgs) getStartupScriptMode() StartupScriptMode {
	return startupArgs.StartupScriptMode
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
		NodeExporterPort:  NodeExporterPort,
		EbpfExporterPort:  EbpfExporterPort,
		StartupScriptMode: StartupScriptAll,
	}
	for _, override := range overrides {
		override.apply(&startupArgs)
	}
	return startupArgs
}

// TemplateSnippet represents a piece of startup script that can have
// separate content for pre-baking (run during image creation) and
// runtime (run when the VM boots).
type TemplateSnippet struct {
	// PreBaking is the script content to run during image baking.
	// This should include package installation, downloads, configuration, etc.
	PreBaking string
	// Runtime is the script content to run at VM boot time.
	// This should include instance-specific configuration like firewall rules,
	// hostname setting, etc. that cannot be pre-baked into the image.
	Runtime string
}

var (
	// StartupScriptTemplateParts is the template for the startup script.
	StartupScriptTemplateParts = map[string]TemplateSnippet{
		"apt_packages": {
			PreBaking: startupScriptAptPackages,
		},
		"chrony_utils": {
			PreBaking: startupScriptChrony,
		},
		"core_dumps_utils": {
			PreBaking: startupScriptCoreDumps,
		},
		"cron_utils": {
			Runtime: startupScriptCron,
		},
		"fips_utils": {
			PreBaking: startupScriptFIPS,
		},
		"head_utils": {
			PreBaking: startupScriptHead,
			Runtime:   startupScriptHead,
		},
		"tail_utils": {
			PreBaking: startupScriptTail,
			Runtime:   startupScriptTail,
		},
		"hostname_utils": {
			Runtime: startupScriptHostname,
		},
		"node_exporter": {
			PreBaking: startupScriptNodeExporter,
			Runtime:   startupScriptNodeExporterRuntime,
		},
		"ebpf_exporter": {
			PreBaking: startupScriptEbpfExporter,
			Runtime:   startupScriptEbpfExporterRuntime,
		},
		"keepalives": {
			PreBaking: startupScriptKeepalives,
		},
		"ssh_utils": {
			PreBaking: startupScriptSSH,
		},
		"tcpdump": {
			PreBaking: startupScriptTcpdump,
		},
		"timers_services_utils": {
			PreBaking: startupScriptTimersAndServices,
		},
		"ulimits": {
			PreBaking: startupScriptUlimits,
		},
		"touch_initialized_file": {
			Runtime: startupScriptTouchInitializedFile,
		},
	}
)

const startupScriptAptPackages = `
# If enabled, disable unattended-upgrades to prevent it from interfering with
# apt operations performed by the startup script.
if systemctl list-unit-files | grep -q '^unattended-upgrades.service'; then

    echo "unattended-upgrades service exists, disabling it and waiting for apt locks to clear."
    systemctl stop unattended-upgrades

	for i in {1..60}; do
		if ! sudo fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock >/dev/null 2>&1; then
			break
		fi
		echo "Attempt $i/60: apt lock is held, waiting..."
		sleep 1
	done

	# Clean up unattended-upgrades logs
    sudo rm -rf /var/log/unattended-upgrades
fi

{{ if .EnableFIPS }}
# Wait for cloud-init to finish in case it's enabling FIPS/FIPS-Updates
# because this will update the package lists.
sudo cloud-init status --wait || true
{{ end }}

# Update package lists
sudo apt-get update -q

# Uninstall unattended-upgrades if it's installed.
sudo apt-get remove -yq unattended-upgrades

# Install required packages.
sudo apt-get install -qy --no-install-recommends mdadm`

const startupScriptChrony = `
# If chrony is not already installed, install it.
if ! dpkg -s chrony >/dev/null 2>&1; then
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
	sudo chronyc -a waitsync 30 0.01 | sudo tee -a /root/chrony.log
fi`

const startupScriptCoreDumps = `
# Enable core dumps, do this last, something above resets /proc/sys/kernel/core_pattern
# to just "core".
if [ ! -e /bin/gzip_core.sh ]; then
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
fi`

const startupScriptCron = `
{{ if not .EnableCron }}
systemctl stop cron
systemctl mask cron
{{ else }}
systemctl unmask cron
systemctl enable cron
systemctl start cron
{{ end }}`

const startupScriptFIPS = `
{{ if .EnableFIPS }}
# Install ubuntu-advantage-tools if not already installed.
sudo apt-get install -yq ubuntu-advantage-tools jq
# Enable FIPS (in practice, it's often already enabled at this point).
# Check both "fips" and "fips-updates" services
fips_status=$(sudo pro status --format json | jq '.services[] | select(.name == "fips" or .name == "fips-updates") | .status')
if [ "$fips_status" != '"enabled"' ]; then
	# Enable fips (ubuntu < 22.04) or fips-updates (ubuntu 22.04+)
	# depending on which service is available.
	sudo ua enable $(sudo pro status --format json | jq -r '.services[] | select(.name == "fips" or .name == "fips-updates") | .name') --assume-yes
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

const startupScriptTail = `
# Log the ending time of the script with a timestamp
echo "startup script ending: $(date -u)"
`

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
if [ ! -e /etc/sysctl.d/99-roachprod-tcp-keepalive.conf ]; then
	cat <<EOF > /etc/sysctl.d/99-roachprod-tcp-keepalive.conf
net.ipv4.tcp_keepalive_time=60
net.ipv4.tcp_keepalive_intvl=60
net.ipv4.tcp_keepalive_probes=5
EOF

	sysctl --system  # reload sysctl settings
fi`

// This installs node_exporter (PreBaking) and sets up firewall rules (Runtime).
const startupScriptNodeExporter = `
# If node_exporter is not already installed, install it.
export DEFAULT_USER_HOME="/home/$(id -nu 1000)"
if [ ! -e "${DEFAULT_USER_HOME}/node_exporter/node_exporter" ]; then
	export ARCH=$(dpkg --print-architecture)
	mkdir -p ${DEFAULT_USER_HOME}/node_exporter && curl -fsSL \
		https://storage.googleapis.com/cockroach-test-artifacts/prometheus/node_exporter-` + NodeExporterVersion + `.linux-${ARCH}.tar.gz |
		tar zxv --strip-components 1 -C ${DEFAULT_USER_HOME}/node_exporter \
		&& chown -R 1000:1000 ${DEFAULT_USER_HOME}/node_exporter
fi`

// Runtime portion: start node_exporter and configure firewall rules.
// These must run every boot since they're instance-specific.
const startupScriptNodeExporterRuntime = `
export DEFAULT_USER_HOME="/home/$(id -nu 1000)"
(
	cd ${DEFAULT_USER_HOME}/node_exporter && \
	sudo systemd-run --unit node_exporter --same-dir \
		./node_exporter --collector.systemd --collector.interrupts --collector.processes \
		--web.listen-address=":{{.NodeExporterPort}}" \
		--web.telemetry-path="` + NodeExporterMetricsPath + `"
)

export SCRAPING_PUBLIC_IPS=$(dig +short prometheus.testeng.crdb.io | awk '{printf "%s%s",sep,$0; sep=","} END {print ""}')
sudo iptables -A INPUT -s 127.0.0.1,10.0.0.0/8,${SCRAPING_PUBLIC_IPS} -p tcp --dport {{.NodeExporterPort}} -j ACCEPT
sudo iptables -A INPUT -p tcp --dport {{.NodeExporterPort}} -j DROP
`

const startupScriptEbpfExporter = `
# If ebpf_exporter is not already installed, install it.
export DEFAULT_USER_HOME="/home/$(id -nu 1000)"
if [ ! -e "${DEFAULT_USER_HOME}/ebpf_exporter/ebpf_exporter" ]; then
	export ARCH=$(dpkg --print-architecture)
	mkdir -p ${DEFAULT_USER_HOME}/ebpf_exporter && curl -fsSL \
		https://storage.googleapis.com/cockroach-test-artifacts/prometheus/ebpf_exporter-` + EbpfExporterVersion + `.linux-${ARCH}.tar.gz |
		tar zxv --strip-components 1 -C ${DEFAULT_USER_HOME}/ebpf_exporter \
		&& chown -R 1000:1000 ${DEFAULT_USER_HOME}/ebpf_exporter
fi`

// Runtime portion: start ebpf_exporter and configure firewall rules.
const startupScriptEbpfExporterRuntime = `
export DEFAULT_USER_HOME="/home/$(id -nu 1000)"
(
	cd ${DEFAULT_USER_HOME}/ebpf_exporter && \
	sudo systemd-run --unit ebpf_exporter --same-dir \
		./ebpf_exporter \
		--config.dir=examples \
		--config.names=biolatency,timers,sched-trace,syscalls,uprobe \
		--web.listen-address=":{{.EbpfExporterPort}}" \
		--web.telemetry-path="` + EbpfExporterMetricsPath + `"
)

export SCRAPING_PUBLIC_IPS=$(dig +short prometheus.testeng.crdb.io | awk '{printf "%s%s",sep,$0; sep=","} END {print ""}')
sudo iptables -A INPUT -s 127.0.0.1,10.0.0.0/8,${SCRAPING_PUBLIC_IPS} -p tcp --dport {{.EbpfExporterPort}} -j ACCEPT
sudo iptables -A INPUT -p tcp --dport {{.EbpfExporterPort}} -j DROP
`

const startupScriptSSH = `
# sshguard can prevent frequent ssh connections to the same host. Disable it.
if systemctl is-active --quiet sshguard; then
	systemctl stop sshguard
fi
systemctl mask sshguard

ssh_updated_config=0
# increase the number of concurrent unauthenticated connections to the sshd
# daemon. See https://en.wikibooks.org/wiki/OpenSSH/Cookbook/Load_Balancing.
# By default, only 10 unauthenticated connections are permitted before sshd
# starts randomly dropping connections.
if ! grep -q '^MaxStartups' /etc/ssh/sshd_config; then
	sudo sh -c 'echo "MaxStartups 64:30:128" >> /etc/ssh/sshd_config'
	ssh_updated_config=1
fi

# Crank up the logging for issues such as:
# https://github.com/cockroachdb/cockroach/issues/36929
if ! grep -q '^LogLevel' /etc/ssh/sshd_config; then
	sudo sed -i'' 's/LogLevel.*$/LogLevel DEBUG3/' /etc/ssh/sshd_config
	ssh_updated_config=1
fi

# FIPS is still on Ubuntu 20.04 however, so don't enable if using FIPS.
{{ if not .EnableFIPS }}
if ! grep -q '^PubkeyAcceptedAlgorithms' /etc/ssh/sshd_config; then
	sudo sh -c 'echo "PubkeyAcceptedAlgorithms +ssh-rsa" >> /etc/ssh/sshd_config'
	ssh_updated_config=1
fi
{{ end }}

if ! grep -q '^LoginGraceTime' /etc/ssh/sshd_config; then
	sudo sed -i 's/#LoginGraceTime .*/LoginGraceTime 0/g' /etc/ssh/sshd_config
	ssh_updated_config=1
fi
if ! grep -q '^TCPKeepAlive' /etc/ssh/sshd_config; then
	sudo sed -i 's/TCPKeepAlive no/TCPKeepAlive yes/g' /etc/ssh/sshd_config
	ssh_updated_config=1
fi

if [ "$ssh_updated_config" -eq 1 ]; then
	echo "Restarting sshd to apply updated configuration"
	sudo service ssh restart
fi`

const startupScriptTcpdump = `
# N.B. Ubuntu 22.04 changed the location of tcpdump to /usr/bin. Since existing tooling, e.g.,
# jepsen uses /usr/sbin, we create a symlink.
# See https://ubuntu.pkgs.org/22.04/ubuntu-main-amd64/tcpdump_4.99.1-3build2_amd64.deb.html
if [ ! -e /usr/sbin/tcpdump ]; then
	sudo ln -s /usr/bin/tcpdump /usr/sbin/tcpdump
fi`

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
if [ ! -e /etc/security/limits.d/10-roachprod-nofiles.conf ]; then
	sudo sh -c 'echo "root - nofile 1048576\n* - nofile 1048576" > /etc/security/limits.d/10-roachprod-nofiles.conf'
fi`

const startupScriptTouchInitializedFile = `
# Touch the disks initialized file to mark that initialization is complete.
sudo touch {{ .OSInitializedFile }}
`

func GenerateStartupScript(w io.Writer, cloudSpecificTemplate string, args any) error {
	t := template.New("start")

	// Extract PreBaking flag from args to determine which snippets to include
	var startupScriptMode StartupScriptMode
	// Try to extract PreBaking from the args
	// We use reflection-like approach by checking the concrete type
	switch v := args.(type) {
	case StartupArgs:
		startupScriptMode = v.StartupScriptMode
	default:
		// For template params that embed StartupArgs, try to get the field
		// This handles the tmplParams struct used in cloud-specific code
		if getter, ok := args.(StartupScriptModeGetter); ok {
			startupScriptMode = getter.getStartupScriptMode()
		}
	}

	for name, snippet := range StartupScriptTemplateParts {
		var err error
		var content string

		switch startupScriptMode {
		case StartupScriptPreBakingOnly:
			// When pre-baking, only include PreBaking parts
			content = snippet.PreBaking
		case StartupScriptRuntimeOnly:
			// When generating runtime script, only include Runtime parts
			content = snippet.Runtime
		case StartupScriptAll:
			// At runtime, include both PreBaking (for fallback when using base image)
			// and Runtime parts (for instance-specific config)
			if snippet.PreBaking != "" && snippet.Runtime != "" {
				content = snippet.PreBaking + "\n\n" + snippet.Runtime
			} else if snippet.PreBaking != "" {
				content = snippet.PreBaking
			} else {
				content = snippet.Runtime
			}
		default:
			return errors.Errorf("unknown StartupScriptMode: %d", startupScriptMode)
		}

		t, err = t.Parse(
			fmt.Sprintf(`
{{define "%s"}}
%s
{{end}}`, name, content),
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

// ComputeStartupTemplateChecksum generates a deterministic hash of all
// startup template content. This is used to version pre-baked images.
// The checksum includes all shared template parts to ensure that any
// change to the startup configuration triggers a new image build.
func ComputeStartupTemplateChecksum(providerTemplate string) string {
	h := sha256.New()

	// Hash the provider template
	h.Write([]byte(providerTemplate))

	// Get all template part names and sort them for determinism
	names := make([]string, 0, len(StartupScriptTemplateParts))
	for name := range StartupScriptTemplateParts {
		names = append(names, name)
	}
	sort.Strings(names)

	// Hash each template in sorted order (both PreBaking and Runtime parts)
	for _, name := range names {
		snippet := StartupScriptTemplateParts[name]
		h.Write([]byte(snippet.PreBaking))
		h.Write([]byte(snippet.Runtime))
	}

	// Return first 8 characters of hex hash
	return fmt.Sprintf("%x", h.Sum(nil))[:8]
}
