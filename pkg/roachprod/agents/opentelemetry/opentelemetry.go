// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opentelemetry

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

//go:embed files/opentelemetry-config.yaml.tmpl
var opentelemetryConfigTemplate string

//go:embed files/opentelemetry-env.conf.tmpl
var opentelemetryEnvTemplate string

// Config represents the information needed to configure and run the
// OpenTelemetry Collector on a CockroachDB cluster.
type Config struct {
	// Datadog site to send telemetry data to (e.g, us5.datadoghq.com).
	DatadogSite string

	// Datadog API key to authenticate to Datadog.
	DatadogAPIKey string

	// Datadog tags as a comma-separated list in the format KEY1:VAL1,KEY2:VAL2.
	DatadogTags []string
}

// Install installs, configures, and starts the OpenTelemetry Collector on the
// given CockroachDB cluster c.
func Install(ctx context.Context, l *logger.Logger, c *install.SyncedCluster, config Config) error {
	if err := c.Parallel(ctx, l, install.WithNodes(c.Nodes), func(ctx context.Context, node install.Node) (*install.RunResultDetails, error) {
		res := &install.RunResultDetails{Node: node}

		if err := install.InstallTool(ctx, l, c, install.Nodes{node}, "opentelemetry", l.Stdout, l.Stderr); err != nil {
			res.Err = errors.Wrap(err, "failed installing opentelemetry")
			return res, res.Err
		}

		tags := []string{
			// Reserved Datadog tags.
			"env:development",
			fmt.Sprintf("host:%s", vm.Name(c.Name, int(node))),

			// Custom tags.
			fmt.Sprintf("cluster:%s", c.Name),
		}
		tags = append(tags, config.DatadogTags...)

		data := templateData{
			DatadogSite:        config.DatadogSite,
			DatadogAPIKey:      config.DatadogAPIKey,
			DatadogTags:        makeDatadogTags(tags),
			CockroachDBMetrics: cockroachdbMetrics,
		}

		opentelemetryConfig, err := executeTemplate(opentelemetryConfigTemplate, data)
		if err != nil {
			res.Err = errors.Wrapf(err, "failed rendering opentelemetry configuration for node %d", node)
			return res, res.Err
		}

		if err := c.PutString(ctx, l, install.Nodes{node}, opentelemetryConfig, "/tmp/opentelemetry-config.yaml", 0644); err != nil {
			res.Err = errors.Wrapf(err, "failed writing opentelemetry configuration to node %d", node)
			return res, res.Err
		}

		opentelemetryEnvConfig, err := executeTemplate(opentelemetryEnvTemplate, data)
		if err != nil {
			res.Err = errors.Wrapf(err, "failed rendering opentelemetry environment file for node %d", node)
			return res, res.Err
		}

		if err := c.PutString(ctx, l, install.Nodes{node}, opentelemetryEnvConfig, "/tmp/opentelemetry-env.conf", 0644); err != nil {
			res.Err = errors.Wrapf(err, "failed writing opentelemetry environment file to node %d", node)
			return res, res.Err
		}

		// The `/etc/otelcol-contrib/config-override.yaml` file is created with no
		// content so that the otelcol-contrib service can successfully start.
		// Operators can add additional configuration in there to suit their needs.
		if err := c.Run(ctx, l, l.Stdout, l.Stderr, install.WithNodes(install.Nodes{node}), "opentelemetry", `
		sudo cp /tmp/opentelemetry-config.yaml /etc/otelcol-contrib/config.yaml && rm /tmp/opentelemetry-config.yaml
		sudo touch /etc/otelcol-contrib/config-override.yaml
		sudo cp /tmp/opentelemetry-env.conf /etc/otelcol-contrib/otelcol-contrib.conf && rm /tmp/opentelemetry-env.conf
		sudo systemctl enable otelcol-contrib && sudo systemctl restart otelcol-contrib
		`); err != nil {
			res.Err = errors.Wrap(err, "failed enabling and starting opentelemetry service")
			return res, res.Err
		}

		return res, nil
	}); err != nil {
		return errors.Wrap(err, "failed starting opentelemetry")
	}

	return nil
}

// Stop stops a running OpenTelemetry Collector service on the given CockroachDB cluster c.
func Stop(ctx context.Context, l *logger.Logger, c *install.SyncedCluster) error {
	if err := c.Run(ctx, l, l.Stdout, l.Stderr, install.WithNodes(c.Nodes).WithShouldRetryFn(install.AlwaysTrue), "opentelemetry-stop", `
	sudo systemctl disable otelcol-contrib && sudo systemctl stop otelcol-contrib
	`); err != nil {
		return errors.Wrap(err, "failed stopping opentelemetry")
	}

	return nil
}

type templateData struct {
	DatadogSite   string
	DatadogAPIKey string
	DatadogTags   map[string]string

	// CockroachDBMetrics is a mapping of CockroachDB metric names to cockroachdb
	// Datadog integration metric names.
	CockroachDBMetrics map[string]string
}

func executeTemplate(tmpl string, data templateData) (string, error) {
	tpl, err := template.New("opentelemetry").
		Funcs(template.FuncMap{
			"join": strings.Join,
		}).
		Parse(tmpl)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// makeDatadogTags parses tags for the format VALUE or KEY:VALUE and builds a
// mapping of tag keys to tag values. It does not validate that the tags
// contain valid characters.
func makeDatadogTags(tags []string) map[string]string {
	ddTags := make(map[string]string)

	for _, tag := range tags {
		fields := strings.SplitN(tag, ":", 2)

		// Assume a tag format of VALUE.
		key := fields[0]
		value := ""

		// This tag is in the format KEY:VALUE. Pull out its value.
		if len(fields) > 1 {
			value = fields[1]
		}

		ddTags[key] = value
	}

	return ddTags
}
