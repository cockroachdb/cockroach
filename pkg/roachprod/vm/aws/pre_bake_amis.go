// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aws

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/utils/packer"
	"github.com/cockroachdb/errors"
)

// archConfig holds architecture-specific configuration for AWS image building.
type archConfig struct {
	arch        string // "amd64", "arm64", "fips"
	baseAmiID   string // Base AMI ID
	baseAmiName string // Base AMI name (for naming pre-baked AMI)
	machineType string // EC2 instance type for building
	enableFIPS  bool   // Whether to run the FIPS setup
}

// GetPackerSources returns Packer source configurations for AWS AMIs.
func (p *Provider) GetPackerSources(
	l *logger.Logger, architectures []string, providerOpts map[string]interface{},
) ([]packer.SourceConfig, []packer.ProvisionerConfig, []packer.PluginConfig, error) {
	checksum := vm.ComputeStartupTemplateChecksum(awsStartupScriptTemplate)

	// Get all regions from the default config
	cfg := DefaultConfig
	var allRegions []string
	for _, region := range cfg.Regions {
		allRegions = append(allRegions, region.Name)
	}
	if len(allRegions) == 0 {
		return nil, nil, nil, errors.New("no regions found in AWS configuration")
	}

	// Use us-east-1 as the primary region for building
	primaryRegion := "us-east-1"

	l.Printf(
		"Building in primary region %s, then copying to %d other region(s) in parallel...",
		primaryRegion,
		len(allRegions)-1,
	)

	// Check which images need to be built
	var imagesToBuild []archConfig
	for _, arch := range architectures {

		amiID, amiName, preBaked, err := p.resolveAMI(l, primaryRegion, vm.CPUArch(arch))
		if err != nil {
			return nil, nil, nil, errors.Wrapf(err, "failed to resolve AMI for arch %s", arch)
		}

		if preBaked {
			l.Printf("Pre-baked AMI %s (%s) already exists for %s, skipping build", amiName, amiID, arch)
			continue
		}

		machineType := "t3.medium"
		enableFIPS := false
		switch arch {
		case "amd64":
			// Defaults are already set for amd64.
			// Used to ensure that the switch is exhaustive.
		case "arm64":
			machineType = "t4g.medium"
		case "fips":
			enableFIPS = true
		default:
			return nil, nil, nil, errors.Newf("unsupported architecture: %s (supported: amd64, arm64, fips)", arch)
		}

		l.Printf("Pre-baked AMI for arch %s (checksum %s) needs to be built from AMI %s (%s)", arch, checksum, amiName, amiID)

		imagesToBuild = append(imagesToBuild, archConfig{
			arch:        arch,
			baseAmiID:   amiID,
			baseAmiName: amiName,
			machineType: machineType,
			enableFIPS:  enableFIPS,
		})
	}

	if len(imagesToBuild) == 0 {
		l.Printf("All requested AWS AMIs already exist, nothing to build")
		return nil, nil, nil, nil
	}

	l.Printf("Preparing to build %d AWS AMI(s)", len(imagesToBuild))

	// Generate startup scripts for each architecture
	scriptPaths := make(map[string]string)
	for _, cfg := range imagesToBuild {
		scriptPath, err := writeStartupScript(
			"",      // no name for pre-baking script
			"",      // no extra mount opts
			vm.Ext4, // default filesystem
			false,   // useMultiple
			cfg.enableFIPS,
			"",   // remoteUserName (AWS uses ubuntu by default)
			true, // bootDiskOnly - Packer VM has no extra disks
			vm.StartupScriptPreBakingOnly,
		)
		if err != nil {
			return nil, nil, nil, errors.Wrapf(err, "failed to generate startup script for %s", cfg.arch)
		}
		scriptPaths[cfg.arch] = scriptPath
	}

	// Get list of regions to copy to (all except primary)
	var copyToRegions []string
	for _, region := range allRegions {
		if region != primaryRegion {
			copyToRegions = append(copyToRegions, fmt.Sprintf(`"%s"`, region))
		}
	}

	// Build Packer source configurations
	var sources []packer.SourceConfig
	var provisioners []packer.ProvisionerConfig

	for _, cfg := range imagesToBuild {

		imageName := fmt.Sprintf(
			"%s-%s-%s",
			cfg.baseAmiName,
			packer.RoachprodPreBakedImageIdentifier,
			checksum,
		)

		sourceName := fmt.Sprintf("roachprod_aws_%s", cfg.arch)

		sources = append(sources, packer.SourceConfig{
			ProviderName: "aws",
			SourceType:   "amazon-ebs",
			SourceName:   sourceName,
			Config: map[string]string{
				"region":        primaryRegion,
				"source_ami":    cfg.baseAmiID,
				"instance_type": cfg.machineType,
				"ssh_username":  "ubuntu",
				"ami_name":      imageName,
				"ami_description": fmt.Sprintf(
					"Pre-baked roachprod AMI (%s, checksum: %s)",
					cfg.arch,
					checksum,
				),
			},
			ConfigMultiline: map[string]string{
				"ami_regions": fmt.Sprintf("[%s]", strings.Join(copyToRegions, ", ")),
				"tags": fmt.Sprintf(`{
    roachprod = "true"
    checksum  = "%s"
    arch      = "%s"
    base      = "%s"
  }`, checksum, cfg.arch, strings.ReplaceAll(cfg.baseAmiName, ".", "-")),
			},
		})

		provisioners = append(provisioners, packer.ProvisionerConfig{
			OnlySources: []string{fmt.Sprintf("amazon-ebs.%s", sourceName)},
			ScriptPath:  scriptPaths[cfg.arch],
		})
	}

	plugins := []packer.PluginConfig{
		{
			Name:    "amazon",
			Version: "~> 1",
			Source:  "github.com/hashicorp/amazon",
		},
	}

	return sources, provisioners, plugins, nil
}
