// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vm

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
		"keepalives":            startupScriptKeepalives,
		"ssh_utils":             startupScriptSSH,
		"tcpdump":               startupScriptTcpdump,
		"timers_services_utils": startupScriptTimersAndServices,
		"ulimits":               startupScriptUlimits,
	}
)

const startupScriptAptPackages = `
{{define "apt_packages"}}
sudo apt-get update -q
sudo apt-get install -qy --no-install-recommends mdadm
{{end}}`

const startupScriptChrony = `
{{define "chrony_utils"}}
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
{{ range .ChronyServers }}
server {{.}} prefer iburst
{{ end }}
makestep 0.1 3
EOF

sudo /etc/init.d/chrony restart
sudo chronyc -a waitsync 30 0.01 | sudo tee -a /root/chrony.log
{{end}}`

const startupScriptCoreDumps = `
{{define "core_dumps_utils"}}
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
{{end}}`

const startupScriptCron = `
{{define "cron_utils"}}
# Uninstall some packages to prevent them running cronjobs and similar jobs in parallel
systemctl stop unattended-upgrades
sudo rm -rf /var/log/unattended-upgrades
apt-get purge -y unattended-upgrades

{{ if not .EnableCron }}
systemctl stop cron
systemctl mask cron
{{ end }}
{{end}}`

const startupScriptFIPS = `
{{define "fips_utils"}}
{{ if .EnableFIPS }}
sudo apt-get install -yq ubuntu-advantage-tools jq
# Enable FIPS (in practice, it's often already enabled at this point).
if [ $(sudo pro status --format json | jq '.services[] | select(.name == "fips") | .status') != '"enabled"' ]; then
  sudo ua enable fips --assume-yes
fi
{{ end }}
{{end}}`

const startupScriptHead = `
{{define "head_utils"}}
# ensure any failure fails the entire script
set -eux

# Redirect output to stdout/err and a log file
exec &> >(tee -a {{ .StartupLogs }})

# Log the startup of the script with a timestamp
echo "startup script starting: $(date -u)"

if [ -e {{ .DisksInitializedFile }} ]; then
  echo "Already initialized, exiting."
  exit 0
fi
{{end}}`

const startupScriptHostname = `
{{define "hostname_utils"}}
# set hostname according to the name used by roachprod. There's host
# validation logic that relies on this -- see comment on cluster_synced.go
{{ if .VMName }}
sudo hostnamectl set-hostname {{.VMName}}
{{ end }}
{{end}}`

const startupScriptKeepalives = `
{{define "keepalives"}}
# Send TCP keepalives every minute since GCE will terminate idle connections
# after 10m. Note that keepalives still need to be requested by the application
# with the SO_KEEPALIVE socket option.
cat <<EOF > /etc/sysctl.d/99-roachprod-tcp-keepalive.conf
net.ipv4.tcp_keepalive_time=60
net.ipv4.tcp_keepalive_intvl=60
net.ipv4.tcp_keepalive_probes=5
EOF

sysctl --system  # reload sysctl settings
{{end}}`

const startupScriptSSH = `
{{define "ssh_utils"}}
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
sudo service ssh restart
{{end}}`

const startupScriptTcpdump = `
{{define "tcpdump"}}
# N.B. Ubuntu 22.04 changed the location of tcpdump to /usr/bin. Since existing tooling, e.g.,
# jepsen uses /usr/sbin, we create a symlink.
# See https://ubuntu.pkgs.org/22.04/ubuntu-main-amd64/tcpdump_4.99.1-3build2_amd64.deb.html
# FIPS is still on Ubuntu 20.04 however, so don't create if using FIPS.
{{ if not .EnableFIPS }}
sudo ln -s /usr/bin/tcpdump /usr/sbin/tcpdump
{{ end }}
{{end}}`

const startupScriptTimersAndServices = `
{{define "timers_services_utils"}}
for timer in apt-daily-upgrade.timer apt-daily.timer e2scrub_all.timer fstrim.timer man-db.timer e2scrub_all.timer ; do
  systemctl mask $timer
done

for service in apport.service atd.service; do
	if systemctl is-active --quiet $service; then
    	systemctl stop $service
	fi
  systemctl mask $service
done
{{end}}`

const startupScriptUlimits = `
{{define "ulimits"}}
# increase the default maximum number of open file descriptors for
# root and non-root users. Load generators running a lot of concurrent
# workers bump into this often.
sudo sh -c 'echo "root - nofile 1048576\n* - nofile 1048576" > /etc/security/limits.d/10-roachprod-nofiles.conf'
{{end}}
`
