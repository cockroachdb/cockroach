// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package packer

import "github.com/cockroachdb/cockroach/pkg/roachprod/logger"

// SourceConfig defines a Packer source block for building an image.
type SourceConfig struct {
	// ProviderName is the cloud provider name (e.g., "gce", "aws")
	ProviderName string

	// SourceType is the Packer builder type (e.g., "googlecompute", "amazon-ebs")
	SourceType string

	// SourceName is the unique name for this source (e.g., "roachprod_gce_amd64")
	SourceName string

	// Config contains provider-specific configuration as key-value pairs
	// These will be written directly into the Packer HCL source block
	Config map[string]string

	// ConfigMultiline contains provider-specific multi-line configuration
	// (e.g., arrays, nested objects) that need special HCL formatting
	ConfigMultiline map[string]string
}

// ProvisionerConfig defines a Packer provisioner block for running scripts.
type ProvisionerConfig struct {
	// OnlySources lists which sources this provisioner should run on
	// Format: ["googlecompute.roachprod_gce_amd64", "amazon-ebs.roachprod_aws_amd64"]
	OnlySources []string

	// ScriptPath is the path to the shell script to execute
	ScriptPath string

	// ExecuteCommand is the command template for running the script
	// Defaults to sudo execution if empty
	ExecuteCommand string
}

// PluginConfig defines a required Packer plugin.
type PluginConfig struct {
	// Name is the plugin name (e.g., "googlecompute", "amazon")
	Name string

	// Version is the plugin version constraint (e.g., "1.1.1", "~> 1")
	Version string

	// Source is the plugin source (e.g., "github.com/hashicorp/googlecompute")
	Source string
}

// ImageBakingProvider defines the interface for cloud providers that support
// pre-baking VM images with roachprod configuration.
type ImageBakingProvider interface {
	// GetPackerSources returns the Packer source and provisioner configurations
	// for building pre-baked images for the requested architectures.
	//
	// Parameters:
	//   - l: logger for output
	//   - architectures: list of architectures to build (e.g., ["amd64", "arm64", "fips"])
	//   - providerOpts: provider-specific options as a map
	//
	// Returns:
	//   - sources: list of Packer source configurations
	//   - provisioners: list of Packer provisioner configurations
	//   - plugins: list of required Packer plugins
	//   - error: any error encountered during configuration
	GetPackerSources(
		l *logger.Logger,
		architectures []string,
		providerOpts map[string]interface{},
	) (sources []SourceConfig, provisioners []ProvisionerConfig, plugins []PluginConfig, err error)
}
