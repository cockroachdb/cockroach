// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"context"
	"fmt"
	"strings"

	compute "cloud.google.com/go/compute/apiv1"
	computepb "cloud.google.com/go/compute/apiv1/computepb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ui"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"
)

// buildInstanceProperties creates a computepb.InstanceProperties struct for the
// GCP Compute SDK. This is the SDK equivalent of computeInstanceArgs().
func (p *Provider) buildInstanceProperties(
	l *logger.Logger,
	opts vm.CreateOpts,
	providerOpts *ProviderOpts,
	startupScriptContent string,
	zone string,
	labels map[string]string,
) (*computepb.InstanceProperties, error) {
	// Determine which image to use.
	image := providerOpts.Image
	imageProject := defaultImageProject

	if providerOpts.useArmAMI() {
		image = ARM64Image
	}

	if opts.Arch == string(vm.ArchFIPS) {
		image = FIPSImage
		imageProject = FIPSImageProject
	}

	// Build the boot disk configuration.
	disks := []*computepb.AttachedDisk{
		{
			AutoDelete: proto.Bool(true),
			Boot:       proto.Bool(true),
			InitializeParams: &computepb.AttachedDiskInitializeParams{
				DiskSizeGb:  proto.Int64(int64(opts.OsVolumeSize)),
				DiskType:    proto.String(providerOpts.BootDiskType),
				SourceImage: proto.String(fmt.Sprintf("projects/%s/global/images/%s", imageProject, image)),
			},
		},
	}

	// Add additional disks (SSDs or persistent disks).
	if !providerOpts.BootDiskOnly {
		if opts.SSDOpts.UseLocalSSD {
			// Local SSDs are physically attached to the host machine.
			for i := 0; i < providerOpts.SSDCount; i++ {
				disks = append(disks, &computepb.AttachedDisk{
					AutoDelete: proto.Bool(true),
					Type:       proto.String(computepb.AttachedDisk_SCRATCH.String()),
					Mode:       proto.String(computepb.AttachedDisk_READ_WRITE.String()),
					InitializeParams: &computepb.AttachedDiskInitializeParams{
						DiskType: proto.String("local-ssd"),
					},
					Interface: proto.String(computepb.AttachedDisk_NVME.String()),
				})
			}
		} else {
			// Persistent disks are network-attached storage.
			for i := 0; i < providerOpts.PDVolumeCount; i++ {
				disks = append(disks, &computepb.AttachedDisk{
					AutoDelete: proto.Bool(true),
					InitializeParams: &computepb.AttachedDiskInitializeParams{
						DiskSizeGb: proto.Int64(int64(providerOpts.PDVolumeSize)),
						DiskType:   proto.String(providerOpts.PDVolumeType),
					},
				})
			}
		}
	}

	// Configure networking.
	if len(zone) < 3 {
		return nil, errors.Newf("invalid zone %q: must be at least 3 characters", zone)
	}
	region := zone[:len(zone)-2]
	project := p.GetProject()
	networkInterfaces := []*computepb.NetworkInterface{
		{
			Subnetwork: proto.String(fmt.Sprintf("projects/%s/regions/%s/subnetworks/default", project, region)),
			AccessConfigs: []*computepb.AccessConfig{
				{
					Name: proto.String("External NAT"),
					Type: proto.String(computepb.AccessConfig_ONE_TO_ONE_NAT.String()),
				},
			},
		},
	}

	// Configure scheduling.
	scheduling := &computepb.Scheduling{}
	if providerOpts.preemptible {
		scheduling.Preemptible = proto.Bool(true)
		scheduling.OnHostMaintenance = proto.String(computepb.Scheduling_TERMINATE.String())
		scheduling.AutomaticRestart = proto.Bool(false)
	} else if providerOpts.UseSpot {
		scheduling.ProvisioningModel = proto.String(computepb.Scheduling_SPOT.String())
	} else if providerOpts.TerminateOnMigration {
		scheduling.OnHostMaintenance = proto.String(computepb.Scheduling_TERMINATE.String())
	} else {
		scheduling.OnHostMaintenance = proto.String(computepb.Scheduling_MIGRATE.String())
	}

	// Configure the service account.
	var serviceAccounts []*computepb.ServiceAccount
	sa := providerOpts.ServiceAccount
	if sa == "" && p.GetProject() == p.defaultProject {
		sa = providerOpts.defaultServiceAccount
	}
	if sa != "" {
		serviceAccounts = []*computepb.ServiceAccount{
			{
				Email:  proto.String(sa),
				Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"},
			},
		}
	}

	// Configure metadata (including the startup script).
	metadata := &computepb.Metadata{
		Items: []*computepb.Items{
			{
				Key:   proto.String("startup-script"),
				Value: proto.String(startupScriptContent),
			},
		},
	}

	// Build the final InstanceProperties.
	props := &computepb.InstanceProperties{
		MachineType:       proto.String(providerOpts.MachineType),
		Disks:             disks,
		NetworkInterfaces: networkInterfaces,
		Scheduling:        scheduling,
		ServiceAccounts:   serviceAccounts,
		Metadata:          metadata,
	}

	if providerOpts.MinCPUPlatform != "" {
		props.MinCpuPlatform = proto.String(providerOpts.MinCPUPlatform)
	}

	// Set the labels on the instance properties.
	props.Labels = labels

	return props, nil
}

// parseLabelsString converts a "key=value,key2=value2" string into a map.
func parseLabelsString(labels string) (map[string]string, error) {
	result := make(map[string]string)
	if labels == "" {
		return result, nil
	}

	pairs := strings.Split(labels, ",")
	for _, pair := range pairs {
		kv := strings.Split(pair, "=")
		if len(kv) != 2 {
			return nil, errors.Newf("invalid label format: %q (expected key=value)", pair)
		}
		result[kv[0]] = kv[1]
	}
	return result, nil
}

// bulkInsertInstances uses the GCP Compute SDK to create multiple VMs in a
// single zone using the BulkInsert API.
//
// Benefits over CLI-based approach:
// - CLI: 100 VMs = ~4 concurrent gcloud CLI calls with batching
// - BulkInsert: 100 VMs = 1 API call per zone
func (p *Provider) bulkInsertInstances(
	ctx context.Context,
	l *logger.Logger,
	zone string,
	hostNames []string,
	instanceProps *computepb.InstanceProperties,
) error {
	project := p.GetProject()

	client, err := compute.NewInstancesRESTClient(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create GCE compute client")
	}
	defer func() { _ = client.Close() }()

	// Build the per-instance properties map for exact VM naming.
	perInstanceProps := make(map[string]*computepb.BulkInsertInstanceResourcePerInstanceProperties)
	for _, name := range hostNames {
		perInstanceProps[name] = &computepb.BulkInsertInstanceResourcePerInstanceProperties{
			Name: proto.String(name),
		}
	}

	// Build the BulkInsert request.
	req := &computepb.BulkInsertInstanceRequest{
		Project: project,
		Zone:    zone,
		BulkInsertInstanceResourceResource: &computepb.BulkInsertInstanceResource{
			Count:                 proto.Int64(int64(len(hostNames))),
			MinCount:              proto.Int64(int64(len(hostNames))),
			InstanceProperties:    instanceProps,
			PerInstanceProperties: perInstanceProps,
		},
	}

	// Execute the BulkInsert request (async operation).
	op, err := client.BulkInsert(ctx, req)
	if err != nil {
		return errors.Wrapf(err, "BulkInsert request failed for zone %s", zone)
	}

	// Wait for the operation to complete with a spinner for progress feedback.
	err = func() error {
		defer ui.NewDefaultSpinner(l, fmt.Sprintf("BulkInsert: creating %d instances in %s", len(hostNames), zone)).Start()()
		return op.Wait(ctx)
	}()
	if err != nil {
		return errors.Wrapf(err, "BulkInsert operation failed for zone %s", zone)
	}
	l.Printf("BulkInsert: successfully created %d instances in zone %s", len(hostNames), zone)
	return nil
}

// createInstancesSDK creates VMs using the GCP Compute SDK's BulkInsert API.
// This is an alternative to the CLI-based approach that is more efficient
// for creating large numbers of VMs.
func (p *Provider) createInstancesSDK(
	l *logger.Logger,
	opts vm.CreateOpts,
	providerOpts *ProviderOpts,
	labels string,
	zoneToHostNames map[string][]string,
	usedZones []string,
) (vm.List, error) {
	project := p.GetProject()

	// Compute the extraMountOpts for the startup script.
	extraMountOpts := ""
	if !providerOpts.BootDiskOnly {
		if opts.SSDOpts.UseLocalSSD {
			extraMountOpts = "discard"
			if opts.SSDOpts.NoExt4Barrier {
				extraMountOpts = fmt.Sprintf("%s,nobarrier", extraMountOpts)
			}
		} else {
			extraMountOpts = "discard"
		}
	}

	// Generate the startup script content as a string.
	startupScriptContent, err := generateStartupScriptContent(
		extraMountOpts,
		opts.SSDOpts.FileSystem,
		providerOpts.UseMultipleDisks,
		opts.Arch == string(vm.ArchFIPS),
		providerOpts.EnableCron,
		providerOpts.BootDiskOnly,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to generate startup script content")
	}

	// Parse the labels string into a map.
	labelsMap, err := parseLabelsString(labels)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse labels")
	}

	// Create VMs using BulkInsert - one API call per zone.
	l.Printf("Creating instances using BulkInsert API, distributed across [%s]",
		strings.Join(usedZones, ", "))

	g := newLimitedErrorGroup()
	for zone, zoneHosts := range zoneToHostNames {
		zone := zone
		zoneHosts := zoneHosts

		g.Go(func() error {
			// Build the InstanceProperties for this zone (includes labels).
			instanceProps, err := p.buildInstanceProperties(
				l, opts, providerOpts, startupScriptContent, zone, labelsMap,
			)
			if err != nil {
				return errors.Wrapf(err, "failed to build instance properties for zone %s", zone)
			}

			// Call BulkInsert for this zone.
			err = p.bulkInsertInstances(
				context.Background(),
				l,
				zone,
				zoneHosts,
				instanceProps,
			)
			if err != nil {
				return errors.Wrapf(err, "BulkInsert failed for zone %s", zone)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Fetch the created VMs to populate vmList using parallel list calls per zone.
	var vmList vm.List
	var vmListMutex syncutil.Mutex

	g = newLimitedErrorGroup()
	for zone, zoneHosts := range zoneToHostNames {
		zone := zone
		zoneHosts := zoneHosts

		g.Go(func() error {
			// Build a set of hostnames for quick lookup.
			hostNameSet := make(map[string]struct{}, len(zoneHosts))
			for _, name := range zoneHosts {
				hostNameSet[name] = struct{}{}
			}

			// Build name filter for server-side filtering.
			nameFilter := fmt.Sprintf("zone:%s AND name:(%s)", zone, strings.Join(zoneHosts, " "))

			listArgs := []string{
				"compute", "instances", "list",
				"--filter", nameFilter,
				"--project", project,
				"--format", "json",
			}

			var instances []jsonVM
			if err := runJSONCommand(listArgs, &instances); err != nil {
				return errors.Wrapf(err, "failed to list VMs in zone %s", zone)
			}

			// Filter to only include our created VMs (safety check).
			vmListMutex.Lock()
			defer vmListMutex.Unlock()
			for _, instance := range instances {
				if _, ok := hostNameSet[instance.Name]; ok {
					v := instance.toVM(project, p.dnsProvider.PublicDomain())
					vmList = append(vmList, *v)
				}
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return vmList, nil
}
