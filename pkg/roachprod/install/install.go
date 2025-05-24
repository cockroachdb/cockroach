// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package install

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

var installCmds = map[string]string{
	"docker": `
# Add Docker's official GPG key:
sudo apt-get update;
sudo apt-get install  -y \
    ca-certificates \
    curl \
    gnupg;
sudo install -m 0755 -d /etc/apt/keyrings;
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --no-tty --batch --yes --dearmor -o /etc/apt/keyrings/docker.gpg;
sudo chmod a+r /etc/apt/keyrings/docker.gpg;

# Add the repository to Apt sources:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null;
sudo apt-get update;

# Install
sudo apt-get install -y docker-ce;
sudo usermod -aG docker ubuntu;

# Verify
sudo docker run hello-world
`,

	"gcc": `
sudo apt-get update;
sudo apt-get install -y gcc;
`,

	"go": `
sudo apt-get update;
sudo apt-get install -y golang-go;`,

	"haproxy": `
sudo apt-get update;
sudo apt-get install -y haproxy;
`,

	"ntp": `
sudo apt-get update;
sudo apt-get install -y \
  ntp \
  ntpdate;
`,

	"sysbench": `
sudo apt-get update;
sudo apt-get install -y sysbench;
`,

	"zfs": `
sudo apt-get update;
sudo apt-get install -y \
  zfsutils-linux;
`,

	"postgresql": `
sudo apt-get update;
sudo apt-get install -y postgresql;
`,

	"fluent-bit": `
curl -fsSL https://packages.fluentbit.io/fluentbit.key | sudo gpg --no-tty --batch --yes --dearmor -o /etc/apt/keyrings/fluent-bit.gpg;
code_name="$(. /etc/os-release && echo "${VERSION_CODENAME}")";
echo "deb [signed-by=/etc/apt/keyrings/fluent-bit.gpg] https://packages.fluentbit.io/ubuntu/${code_name} ${code_name} main" | \
  sudo tee /etc/apt/sources.list.d/fluent-bit.list > /dev/null;
sudo apt-get update;
sudo apt-get install -y fluent-bit;
`,

	"opentelemetry": `
sudo apt-get update;
sudo apt-get install -y curl;
curl -L -o /tmp/otelcol-contrib.deb https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v0.101.0/otelcol-contrib_0.101.0_linux_amd64.deb;
sudo apt-get install -y /tmp/otelcol-contrib.deb;
rm /tmp/otelcol-contrib.deb;
`,

	"bzip2": `
sudo apt-get update;
sudo apt-get install -y bzip2;
`,

	"nmap": `
sudo apt-get update;
sudo apt-get install -y nmap;
`,

	"vmtouch": `
sudo apt-get update;
sudo apt-get install -y vmtouch;
`,
}

// SortedCmds TODO(peter): document
func SortedCmds() []string {
	cmds := make([]string, 0, len(installCmds))
	for cmd := range installCmds {
		cmds = append(cmds, cmd)
	}
	sort.Strings(cmds)
	return cmds
}

// Install TODO(peter): document
func Install(ctx context.Context, l *logger.Logger, c *SyncedCluster, args []string) error {
	for _, arg := range args {
		var buf bytes.Buffer
		if err := InstallTool(ctx, l, c, c.Nodes, arg, &buf, &buf); err != nil {
			l.Printf(buf.String())
			return err
		}
	}
	return nil
}

func InstallTool(
	ctx context.Context,
	l *logger.Logger,
	c *SyncedCluster,
	nodes Nodes,
	softwareName string,
	stdout, stderr io.Writer,
) error {
	cmd, ok := installCmds[softwareName]
	if !ok {
		return fmt.Errorf("unknown tool %q", softwareName)
	}
	cmd = strings.ReplaceAll(cmd, "%ROACHPROD_CLUSTER_NAME%", c.Name)

	// Ensure that we early exit if any of the shell statements fail.
	cmd = "set -exuo pipefail;" + cmd
	if err := c.Run(ctx, l, stdout, stderr, WithNodes(nodes), "installing "+softwareName, cmd); err != nil {
		return rperrors.TransientFailure(err, "install_flake")
	}

	return nil
}

// Mapping of binary name to sha256 checksum for the Go tarballs. Used as optional validation in
// InstallGoVersion, but must be manually entered below.
var goBinarySha = map[string]string{
	"go1.21.3.linux-amd64.tar.gz": "1241381b2843fae5a9707eec1f8fb2ef94d827990582c7c7c32f5bdfbfd420c8",
	"go1.21.3.linux-arm64.tar.gz": "fc90fa48ae97ba6368eecb914343590bbb61b388089510d0c56c2dde52987ef3",
	"go1.21.3.linux-s390x.tar.gz": "4c78e2e6f4c684a3d5a9bdc97202729053f44eb7be188206f0627ef3e18716b6",
	"go1.22.2.linux-amd64.tar.gz": "5901c52b7a78002aeff14a21f93e0f064f74ce1360fce51c6ee68cd471216a17",
	"go1.22.2.linux-arm64.tar.gz": "36e720b2d564980c162a48c7e97da2e407dfcc4239e1e58d98082dfa2486a0c1",
	"go1.22.2.linux-s390x.tar.gz": "2b39019481c28c560d65e9811a478ae10e3ef765e0f59af362031d386a71bfef",
	"go1.23.7.linux-amd64.tar.gz": "4741525e69841f2e22f9992af25df0c1112b07501f61f741c12c6389fcb119f3",
	"go1.23.7.linux-arm64.tar.gz": "597acbd0505250d4d98c4c83adf201562a8c812cbcd7b341689a07087a87a541",
	"go1.23.7.linux-s390x.tar.gz": "af1d4c5d01e32c2cf6e3cc00e44cb240e1a6cef539b28a64389b2b9ca284ac6c",
}

func InstallGoVersion(ctx context.Context, l *logger.Logger, c *SyncedCluster, v string) error {
	retryRunOpts := WithNodes(c.Nodes).WithRetryOpts(*DefaultRetryOpt)

	if err := c.Run(
		ctx, l, l.Stdout, l.Stderr, retryRunOpts, "update apt-get", `sudo apt-get -qq update`,
	); err != nil {
		return err
	}

	if err := c.Run(
		ctx, l, l.Stdout, l.Stderr, retryRunOpts, "install dependencies (go uses C bindings)", `sudo apt-get -qq install build-essential`,
	); err != nil {
		return err
	}

	arch := c.VMs[0].CPUArch
	binary := fmt.Sprintf("go%s.linux-%s.tar.gz", v, arch)

	if err := c.Run(
		ctx, l, l.Stdout, l.Stderr, retryRunOpts, fmt.Sprintf("installing go version: %s", v), fmt.Sprintf(`curl -fsSL https://go.dev/dl/%s > /tmp/go.tgz`, binary),
	); err != nil {
		return errors.Wrapf(err, "failed to download binary %s", binary)
	}

	if sha, ok := goBinarySha[binary]; !ok {
		// Encourage users to add the sha256 to the map above, but don't error because of it.
		// We log a warning with the sha instead so manual verification can be done as a fallback.
		l.Printf("WARN: no sha found for %s, skipping tarball verification of %s", binary, sha)
	} else {
		if err := c.Run(
			ctx, l, l.Stdout, l.Stderr, retryRunOpts, "verify tarball", fmt.Sprintf(`sha256sum -c - <<EOF
%s /tmp/go.tgz
EOF`, sha),
		); err != nil {
			return err
		}
	}

	if err := c.Run(
		ctx, l, l.Stdout, l.Stderr, retryRunOpts, "extract go", `sudo tar -C /usr/local -zxf /tmp/go.tgz && rm /tmp/go.tgz`,
	); err != nil {
		return err
	}

	if err := c.Run(
		ctx, l, l.Stdout, l.Stderr, retryRunOpts, "force symlink go", "sudo ln -sf /usr/local/go/bin/go /usr/bin",
	); err != nil {
		return err
	}
	return nil
}
