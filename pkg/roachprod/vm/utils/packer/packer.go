// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package packer

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

const (
	RoachprodPreBakedImageIdentifier = "rp"
)

// Build orchestrates a Packer build across multiple cloud providers and architectures.
// It generates a single Packer HCL configuration with all sources and runs the build,
// allowing Packer to build images for multiple providers in parallel.
func Build(
	l *logger.Logger,
	sources []SourceConfig,
	provisioners []ProvisionerConfig,
	plugins []PluginConfig,
	dryRun bool,
) error {
	if len(sources) == 0 {
		return errors.New("no sources provided for Packer build")
	}

	// Check if Packer is installed
	if _, err := exec.LookPath("packer"); err != nil {
		return errors.New("Packer is not installed or not found in PATH")
	}

	l.Printf("Building %d image(s) across %d provider(s)...", len(sources), countProviders(sources))

	// Build Packer HCL configuration
	var packerConfig strings.Builder

	// Write required plugins block
	packerConfig.WriteString("packer {\n  required_plugins {\n")
	pluginsSeen := make(map[string]bool)
	for _, plugin := range plugins {
		if pluginsSeen[plugin.Name] {
			continue
		}
		pluginsSeen[plugin.Name] = true
		packerConfig.WriteString(fmt.Sprintf(`    %s = {
      version = "%s"
      source  = "%s"
    }
`, plugin.Name, plugin.Version, plugin.Source))
	}
	packerConfig.WriteString("  }\n}\n\n")

	// Write source blocks
	for _, src := range sources {
		packerConfig.WriteString(fmt.Sprintf(`source "%s" "%s" {
`, src.SourceType, src.SourceName))

		// Write simple key-value config
		for key, value := range src.Config {
			packerConfig.WriteString(fmt.Sprintf("  %s = \"%s\"\n", key, value))
		}

		// Write multi-line config (arrays, objects, etc.)
		for key, value := range src.ConfigMultiline {
			packerConfig.WriteString(fmt.Sprintf("  %s = %s\n", key, value))
		}

		packerConfig.WriteString("}\n\n")
	}

	// Write build block
	var sourceRefs []string
	for _, src := range sources {
		sourceRefs = append(sourceRefs, fmt.Sprintf(`"source.%s.%s"`, src.SourceType, src.SourceName))
	}

	packerConfig.WriteString(fmt.Sprintf(`build {
  sources = [%s]

`, strings.Join(sourceRefs, ", ")))

	// Write provisioner blocks
	for _, prov := range provisioners {
		executeCmd := prov.ExecuteCommand
		if executeCmd == "" {
			executeCmd = "chmod +x {{ .Path }}; sudo sh -c '{{ .Vars }} {{ .Path }}'"
		}

		packerConfig.WriteString(fmt.Sprintf(`  provisioner "shell" {
    only   = [%s]
    script = "%s"
    execute_command = "%s"
  }

`, strings.Join(quoteSlice(prov.OnlySources), ", "), prov.ScriptPath, executeCmd))
	}

	packerConfig.WriteString("}\n")

	// Write Packer config to temp file
	packerFile, err := os.CreateTemp("", "packer-multi-*.pkr.hcl")
	if err != nil {
		return errors.Wrap(err, "failed to create temporary packer file")
	}
	defer func() { _ = os.Remove(packerFile.Name()) }()

	if _, err := packerFile.WriteString(packerConfig.String()); err != nil {
		_ = packerFile.Close()
		return errors.Wrap(err, "failed to write packer config")
	}
	if err := packerFile.Close(); err != nil {
		return errors.Wrap(err, "failed to close packer config file")
	}

	l.Printf("Created Packer configuration at %s", packerFile.Name())

	if dryRun {
		l.Printf("Dry run enabled; skipping Packer build.")
		return nil
	}

	// Initialize Packer
	l.Printf("Initializing Packer...")
	initCmd := exec.Command("packer", "init", packerFile.Name())
	initCmd.Stdout = l.Stdout
	initCmd.Stderr = l.Stderr
	if err := initCmd.Run(); err != nil {
		return errors.Wrap(err, "packer init failed")
	}

	// Run Packer build
	l.Printf("Running Packer build (this may take 15-20 minutes)...")
	buildCmd := exec.Command("packer", "build", packerFile.Name())
	buildCmd.Stdout = l.Stdout
	buildCmd.Stderr = l.Stderr

	if err := buildCmd.Run(); err != nil {
		return errors.Wrap(err, "packer build failed")
	}

	l.Printf("Successfully created all images")
	return nil
}

// countProviders counts unique provider names in the sources.
func countProviders(sources []SourceConfig) int {
	providers := make(map[string]bool)
	for _, src := range sources {
		providers[src.ProviderName] = true
	}
	return len(providers)
}

// quoteSlice wraps each string in quotes.
func quoteSlice(strs []string) []string {
	result := make([]string, len(strs))
	for i, s := range strs {
		result[i] = fmt.Sprintf(`"%s"`, s)
	}
	return result
}
