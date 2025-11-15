// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/utils/packer"
	"github.com/cockroachdb/errors"
)

// archConfig holds architecture-specific configuration for image building.
type archConfig struct {
	arch         string // "amd64", "arm64", "fips"
	baseImage    string
	imageProject string
	machineType  string
	enableFIPS   bool
}

// GetPackerSources returns Packer source configurations for GCE images.
func (p *Provider) GetPackerSources(
	l *logger.Logger, architectures []string, providerOpts map[string]interface{},
) ([]packer.SourceConfig, []packer.ProvisionerConfig, []packer.PluginConfig, error) {
	// Extract GCE-specific options
	project, _ := providerOpts["project"].(string)
	if project == "" {
		project = DefaultProject()
	}

	zones, _ := providerOpts["zones"].([]string)
	if len(zones) == 0 {
		zones = []string{"us-central1-a"}
	}
	zone := zones[0]

	checksum := vm.ComputeStartupTemplateChecksum(gceDiskStartupScriptTemplate)

	// Configure each architecture
	var imagesToBuild []archConfig
	for _, arch := range architectures {

		baseImage, baseImageProject, preBaked, err := p.resolveImage(l, project, vm.ParseArch(arch))
		if err != nil {
			return nil, nil, nil, errors.Wrapf(err, "failed to resolve image for arch %s", arch)
		}

		if preBaked {
			l.Printf("Pre-baked image found for arch %s: %s (project: %s)", arch, baseImage, baseImageProject)
			continue
		}

		machineType := "n2-standard-4"
		enableFIPS := false

		switch arch {
		case "amd64":
			// Defaults are already set.
			// Used to ensure that the switch is exhaustive.
		case "arm64":
			machineType = "t2a-standard-4"
		case "fips":
			enableFIPS = true
		default:
			return nil, nil, nil, errors.Newf("unsupported architecture: %s (supported: amd64, arm64, fips)", arch)
		}

		l.Printf(
			"Pre-baked image for arch %s (checksum %s) needs to be built from image %s in project %s",
			arch,
			checksum,
			baseImage,
			baseImageProject,
		)

		imagesToBuild = append(imagesToBuild, archConfig{
			arch:         arch,
			baseImage:    baseImage,
			imageProject: baseImageProject,
			machineType:  machineType,
			enableFIPS:   enableFIPS,
		})
	}

	if len(imagesToBuild) == 0 {
		l.Printf("All requested GCE images already exist, nothing to build")
		return nil, nil, nil, nil
	}

	l.Printf("Preparing to build %d GCE image(s) in zone %s", len(imagesToBuild), zone)

	// Generate startup scripts for each architecture
	scriptPaths := make(map[string]string)
	for _, cfg := range imagesToBuild {
		scriptPath, err := writeStartupScript(
			"",      // no extra mount opts
			vm.Ext4, // default filesystem
			false,   // useMultiple
			cfg.enableFIPS,
			false, // enableCron: cron config happens at runtime
			true,  // bootDiskOnly - Packer VM has no extra disks
			vm.StartupScriptPreBakingOnly,
		)
		if err != nil {
			return nil, nil, nil, errors.Wrapf(err, "failed to generate startup script for %s", cfg.arch)
		}
		scriptPaths[cfg.arch] = scriptPath
	}

	// Build Packer source configurations
	var sources []packer.SourceConfig
	var provisioners []packer.ProvisionerConfig

	for _, cfg := range imagesToBuild {
		imageName := fmt.Sprintf("%s-%s-%s", cfg.baseImage, packer.RoachprodPreBakedImageIdentifier, checksum)
		sourceName := fmt.Sprintf("roachprod_gce_%s", cfg.arch)

		sources = append(sources, packer.SourceConfig{
			ProviderName: "gce",
			SourceType:   "googlecompute",
			SourceName:   sourceName,
			Config: map[string]string{
				// Source image info
				"source_image": cfg.baseImage,

				// Resulting image info
				"project_id":   project,
				"image_name":   imageName,
				"image_family": "crl-roachprod",
				"image_description": fmt.Sprintf(
					"Pre-baked roachprod image (%s, checksum: %s)",
					cfg.arch,
					checksum,
				),

				// Build VM config
				"zone":         zone,
				"machine_type": cfg.machineType,
				"ssh_username": "ubuntu",
				"disk_size":    "10",
			},
			ConfigMultiline: map[string]string{
				// Source image project must be an array in HCL
				"source_image_project_id": fmt.Sprintf(`["%s"]`, cfg.imageProject),
				"image_labels": fmt.Sprintf(`{
    roachprod = "true"
    checksum  = "%s"
    arch      = "%s"
    base      = "%s"
  }`, checksum, cfg.arch, strings.ReplaceAll(cfg.baseImage, ".", "-")),
			},
		})

		provisioners = append(provisioners, packer.ProvisionerConfig{
			OnlySources: []string{fmt.Sprintf("googlecompute.%s", sourceName)},
			ScriptPath:  scriptPaths[cfg.arch],
		})
	}

	plugins := []packer.PluginConfig{
		{
			Name:    "googlecompute",
			Version: "1.1.1",
			Source:  "github.com/hashicorp/googlecompute",
		},
	}

	return sources, provisioners, plugins, nil
}
