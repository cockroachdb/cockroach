// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parca

import (
	"context"
	_ "embed"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

//go:embed files/parca-agent.service
var parcaAgentSystemdUnit string

type Config struct {
	Token string
}

// Install installs the Parca Agent on the cluster and starts it as a systemd service.
func Install(ctx context.Context, l *logger.Logger, c *install.SyncedCluster, config Config) error {

	if err := c.PutString(ctx, l, c.Nodes, config.Token, "/tmp/parca.token", 0644); err != nil {
		return errors.Wrapf(err, "failed writing token")
	}
	if err := c.PutString(ctx, l, c.Nodes, parcaAgentSystemdUnit, "/tmp/parca-agent.service", 0644); err != nil {
		return errors.Wrapf(err, "failed writing parca agent systemd unit file")
	}

	if err := c.Run(ctx, l, l.Stdout, l.Stderr, install.WithNodes(c.Nodes),
		"download and set up parca-agent", `sudo mkdir -p /opt/parca
curl -sL https://github.com/parca-dev/parca-agent/releases/download/v0.42.0/parca-agent_0.42.0_$(uname -s)_$(uname -m) -o /tmp/parca-agent
sudo mv /tmp/parca-agent /opt/parca/parca-agent && chmod +x /opt/parca/parca-agent
sudo mv /tmp/parca.token /opt/parca/parca.token
sudo mv /tmp/parca-agent.service /etc/systemd/system/parca-agent.service
sudo systemctl daemon-reload && sudo systemctl enable parca-agent && sudo systemctl restart parca-agent
`); err != nil {
		return err
	}

	return nil
}

// Stop stops the Parca Agent on the cluster and removes it from systemd.
func Stop(ctx context.Context, l *logger.Logger, c *install.SyncedCluster) error {
	return c.Run(ctx, l, l.Stdout, l.Stderr,
		install.WithNodes(c.Nodes).WithShouldRetryFn(install.AlwaysTrue), "parca-agent-stop",
		"sudo systemctl disable parca-agent && sudo systemctl stop parca-agent")
}
