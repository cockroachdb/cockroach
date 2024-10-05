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

	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
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
	// Ensure that we early exit if any of the shell statements fail.
	cmd = "set -exuo pipefail;" + cmd
	if err := c.Run(ctx, l, stdout, stderr, OnNodes(nodes), "installing "+softwareName, cmd); err != nil {
		return rperrors.TransientFailure(err, "install_flake")
	}

	return nil
}
