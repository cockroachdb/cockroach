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
	"os"
	"os/exec"
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

	"side-eye": `
	curl https://sh.side-eye.io/ | SIDE_EYE_API_TOKEN=%API_KEY% SIDE_EYE_ENVIRONMENT="%ROACHPROD_CLUSTER_NAME%" sh
	`,

	"bzip2": `
sudo apt-get update;
sudo apt-get install -y bzip2;
`,

	"nmap": `
sudo apt-get update;
sudo apt-get install -y nmap;
`,
}

// installLocalCmds is a map from software name to a map of strings that
// are replaced in the installCmd for that software with the stdout of executing
// a command locally.
var installLocalCmds = map[string]map[string]*exec.Cmd{
	"side-eye": {
		"%API_KEY%": sideEyeSecretCmd,
	},
}

var sideEyeSecretCmd = exec.Command("gcloud",
	"--project", "cockroach-ephemeral",
	"secrets", "versions", "access", "latest", "--secret", "side-eye-key")

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

	for replace, localCmd := range installLocalCmds[softwareName] {
		copy := *localCmd
		copy.Stderr = os.Stderr
		out, err := copy.Output()
		if err != nil {
			return errors.Wrapf(err, "running local command to derive install argument %s, command %s, failed", replace, copy.String())
		}
		cmd = strings.ReplaceAll(cmd, replace, string(out))
	}

	// Ensure that we early exit if any of the shell statements fail.
	cmd = "set -exuo pipefail;" + cmd
	if err := c.Run(ctx, l, stdout, stderr, WithNodes(nodes), "installing "+softwareName, cmd); err != nil {
		return rperrors.TransientFailure(err, "install_flake")
	}

	return nil
}

func GetGcloudSideEyeSecret() string {
	c := *sideEyeSecretCmd
	c.Stderr = os.Stderr
	out, err := c.Output()
	if err != nil {
		return ""
	}
	return string(out)
}
