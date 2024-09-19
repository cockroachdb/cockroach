// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fluentbit

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
)

//go:embed files/fluent-bit.yaml.tmpl
var fluentBitTemplate string

//go:embed files/fluent-bit.service
var fluentBitSystemdUnit string

// Config represents the information needed to configure and run Fluent Bit on
// a CockroachDB cluster.
type Config struct {
	// Datadog site to send telemetry data to (e.g, us5.datadoghq.com).
	DatadogSite string

	// Datadog API key to authenticate to Datadog.
	DatadogAPIKey string

	// Datadog service for emitted logs.
	DatadogService string

	// Datadog tags as a comma-separated list in the format KEY1:VAL1,KEY2:VAL2.
	DatadogTags []string
}

// Install installs, configures, and starts Fluent Bit on the given CockroachDB
// cluster c.
func Install(ctx context.Context, l *logger.Logger, c *install.SyncedCluster, config Config) error {
	if err := c.Parallel(ctx, l, install.WithNodes(c.Nodes), func(ctx context.Context, node install.Node) (*install.RunResultDetails, error) {
		res := &install.RunResultDetails{Node: node}

		if err := install.InstallTool(ctx, l, c, install.Nodes{node}, "fluent-bit", l.Stdout, l.Stderr); err != nil {
			res.Err = errors.Wrap(err, "failed installing fluent bit")
			return res, res.Err
		}

		tags := []string{
			// Reserved Datadog tags.
			"env:development",

			// Custom tags.
			fmt.Sprintf("cluster:%s", c.Name),
		}
		tags = append(tags, config.DatadogTags...)

		data := templateData{
			DatadogSite:    config.DatadogSite,
			DatadogAPIKey:  config.DatadogAPIKey,
			DatadogService: config.DatadogService,
			Hostname:       vm.Name(c.Name, int(node)),
			Tags:           tags,
		}

		fluentBitConfig, err := executeTemplate(data)
		if err != nil {
			res.Err = errors.Wrapf(err, "failed rendering fluent bit configuration for node %d", node)
			return res, res.Err
		}

		if err := c.PutString(ctx, l, install.Nodes{node}, fluentBitConfig, "/tmp/fluent-bit.yaml", 0644); err != nil {
			res.Err = errors.Wrapf(err, "failed writing fluent bit configuration to node %d", node)
			return res, res.Err
		}

		if err := c.PutString(ctx, l, install.Nodes{node}, fluentBitSystemdUnit, "/tmp/fluent-bit.service", 0644); err != nil {
			res.Err = errors.Wrap(err, "failed writing fluent bit systemd unit file")
			return res, res.Err
		}

		// The `/etc/fluent-bit/config-override.yaml` file is created with no
		// content so that the fluent-bit service can successfully start. Operators
		// can add additional configuration in there to suit their needs.
		if err := c.Run(ctx, l, l.Stdout, l.Stderr, install.WithNodes(install.Nodes{node}), "fluent-bit", `
		sudo cp /tmp/fluent-bit.yaml /etc/fluent-bit/fluent-bit.yaml && rm /tmp/fluent-bit.yaml
		sudo touch /etc/fluent-bit/config-override.yaml
		sudo cp /tmp/fluent-bit.service /etc/systemd/system/fluent-bit.service && rm /tmp/fluent-bit.service
		sudo systemctl daemon-reload && sudo systemctl enable fluent-bit && sudo systemctl restart fluent-bit
		`); err != nil {
			res.Err = errors.Wrap(err, "failed enabling and starting fluent bit service")
			return res, res.Err
		}

		return res, nil
	}); err != nil {
		return errors.Wrap(err, "failed starting fluent bit")
	}

	return nil
}

// Stop stops a running Fluent Bit service on the given CockroachDB cluster c.
func Stop(ctx context.Context, l *logger.Logger, c *install.SyncedCluster) error {
	if err := c.Run(ctx, l, l.Stdout, l.Stderr, install.WithNodes(c.Nodes).WithShouldRetryFn(install.AlwaysTrue), "fluent-bit-stop", `
sudo systemctl disable fluent-bit && sudo systemctl stop fluent-bit
`); err != nil {
		return errors.Wrap(err, "failed stopping fluent bit")
	}

	return nil
}

type templateData struct {
	DatadogSite    string
	DatadogAPIKey  string
	DatadogService string
	Hostname       string
	Tags           []string
}

func executeTemplate(data templateData) (string, error) {
	tpl, err := template.New("fluent-bit-config").
		Funcs(template.FuncMap{
			"join": strings.Join,
		}).
		Parse(fluentBitTemplate)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}
