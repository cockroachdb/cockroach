package fluentbit

import (
	"bytes"
	"context"
	_ "embed"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

//go:embed files/fluent-bit.yaml.tmpl
var fluentBitTemplate string

//go:embed files/fluent-bit.service
var fluentBitSystemdUnit string

type Config struct {
	DatadogSite   string
	DatadogAPIKey string
}

func Install(ctx context.Context, l *logger.Logger, c *install.SyncedCluster, config Config) error {
	if err := install.InstallTool(ctx, l, c, c.Nodes, "fluent-bit", l.Stdout, l.Stderr); err != nil {
		return err
	}

	var buf bytes.Buffer
	var combinedErr error

	// The Fluent Bit configuration needs the hostname of each node.
	for _, node := range c.Nodes {
		buf.Reset()

		if err := c.Run(ctx, l, &buf, l.Stderr, install.WithNodes(install.Nodes{node}), "hostname", "hostname -f"); err != nil {
			combinedErr = errors.CombineErrors(combinedErr, err)
			continue
		}

		data := templateData{
			DatadogSite:   config.DatadogSite,
			DatadogAPIKey: config.DatadogAPIKey,
			Hostname:      strings.TrimSpace(buf.String()),
			Cluster:       c.Name,
		}

		fluentBitConfig, err := executeTemplate(data)
		if err != nil {
			combinedErr = errors.CombineErrors(combinedErr, err)
			continue
		}

		if err := c.PutString(ctx, l, install.Nodes{node}, fluentBitConfig, "/tmp/fluent-bit.yaml", 0644); err != nil {
			combinedErr = errors.CombineErrors(combinedErr, err)
			continue
		}
	}

	if err := c.PutString(ctx, l, c.Nodes, fluentBitSystemdUnit, "/tmp/fluent-bit.service", 0644); err != nil {
		combinedErr = errors.CombineErrors(combinedErr, err)
	}

	if err := c.Run(ctx, l, l.Stdout, l.Stderr, install.WithNodes(c.Nodes), "fluent-bit", `
sudo cp /tmp/fluent-bit.yaml /etc/fluent-bit/fluent-bit.yaml && rm /tmp/fluent-bit.yaml
sudo cp /tmp/fluent-bit.service /etc/systemd/system/fluent-bit.service && rm /tmp/fluent-bit.service
sudo systemctl daemon-reload && sudo systemctl enable fluent-bit && sudo systemctl restart fluent-bit
`); err != nil {
		combinedErr = errors.CombineErrors(combinedErr, err)
	}

	return combinedErr
}

func Stop(ctx context.Context, l *logger.Logger, c *install.SyncedCluster) error {
	if err := c.Run(ctx, l, l.Stdout, l.Stderr, install.WithNodes(c.Nodes).WithShouldRetryFn(install.AlwaysTrue), "fluent-bit-stop", `
sudo systemctl disable fluent-bit && sudo systemctl stop fluent-bit
`); err != nil {
		return err
	}

	return nil
}

type templateData struct {
	DatadogSite   string
	DatadogAPIKey string
	Hostname      string
	Cluster       string
}

func executeTemplate(data templateData) (string, error) {
	tpl, err := template.New("fluent-bit-config").Parse(fluentBitTemplate)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}
