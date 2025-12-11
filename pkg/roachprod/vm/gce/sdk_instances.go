// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"
)

// CreateWithContext implements instance creation using the GCP SDK.
// This is the SDK equivalent of the gcloud-based Create() function.
func (p *Provider) CreateWithContext(
	ctx context.Context,
	l *logger.Logger,
	names []string,
	opts vm.CreateOpts,
	vmProviderOpts vm.ProviderOpts,
) (vm.List, error) {
	providerOpts := vmProviderOpts.(*ProviderOpts)

	project := p.GetProject()
	var gcJob bool
	for _, prj := range projectsWithGC {
		if prj == p.GetProject() {
			gcJob = true
			break
		}
	}
	if !gcJob {
		l.Printf(
			"WARNING: --lifetime functionality requires `roachprod gc --gce-project=%s` cronjob",
			project,
		)
	}

	// ============================================================
	// COMPUTE ZONES AND LABELS
	// ============================================================
	zones, err := computeZones(opts, providerOpts)
	if err != nil {
		return nil, err
	}

	labels, err := computeLabelsMap(opts, providerOpts)
	if err != nil {
		return nil, err
	}

	// ============================================================
	// ZONE PLACEMENT
	// ============================================================
	nodeZones := vm.ZonePlacement(len(zones), len(names))
	zoneToHostNames := make(map[string][]string, min(len(zones), len(names)))
	for i, name := range names {
		zone := zones[nodeZones[i]]
		zoneToHostNames[zone] = append(zoneToHostNames[zone], name)
	}
	usedZones := maps.Keys(zoneToHostNames)

	// ============================================================
	// CREATE INSTANCES (MANAGED vs UNMANAGED)
	// ============================================================
	if providerOpts.Managed {
		// Managed instance group creation - delegate to sdk_groups.go
		return p.createManagedInstances(ctx, l, names, opts, providerOpts, labels, usedZones, zoneToHostNames)
	}

	// Unmanaged instance creation
	return p.createUnmanagedInstances(ctx, l, opts, providerOpts, labels, usedZones, zoneToHostNames)
}

// createUnmanagedInstancesWithSDK creates individual (non-managed) VM instances.
// This is extracted from createWithSDK to improve testability and code organization.
func (p *Provider) createUnmanagedInstances(
	ctx context.Context,
	l *logger.Logger,
	opts vm.CreateOpts,
	providerOpts *ProviderOpts,
	labels map[string]string,
	usedZones []string,
	zoneToHostNames map[string][]string,
) (vm.List, error) {
	project := p.GetProject()
	var vmList vm.List
	var vmListMutex syncutil.Mutex

	g := newLimitedErrorGroupWithContext(ctx)

	l.Printf("Creating %d instances, distributed across [%s]",
		len(zoneToHostNames), strings.Join(usedZones, ", "))

	for zone, hostnames := range zoneToHostNames {
		for _, hostname := range hostnames {
			g.GoCtx(func(ctx context.Context) error {
				// Build instance configuration
				instance, err := p.buildInstance(ctx, l, zone, hostname, opts, providerOpts, labels)
				if err != nil {
					return errors.Wrapf(err, "failed to build instance config for %s", hostname)
				}

				// Create instance
				req := &computepb.InsertInstanceRequest{
					Project:          project,
					Zone:             zone,
					InstanceResource: instance,
				}

				op, err := p.computeInstancesClient.Insert(ctx, req)
				if err != nil {
					return errors.Wrapf(err, "failed to start instance creation for %s", hostname)
				}

				// Wait for operation completion
				if err := op.Wait(ctx); err != nil {
					return errors.Wrapf(err, "instance creation operation failed for %s", hostname)
				}

				// Check for operation errors
				if opErr := op.Proto().GetError(); opErr != nil {
					return errors.Errorf("instance %s creation failed: %v", hostname, opErr)
				}

				// Fetch created instance to get runtime fields (IPs, timestamps, etc.)
				getInstance, err := p.computeInstancesClient.Get(ctx, &computepb.GetInstanceRequest{
					Project:  project,
					Zone:     zone,
					Instance: hostname,
				})
				if err != nil {
					return errors.Wrapf(err, "failed to fetch created instance %s", hostname)
				}

				// Convert to vm.VM using existing conversion logic
				vm := (&sdkInstance{getInstance}).toVM(project, p.dnsProvider.PublicDomain())

				vmListMutex.Lock()
				defer vmListMutex.Unlock()
				vmList = append(vmList, *vm)

				return nil
			})
		}
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return vmList, nil
}

// buildInstance creates a computepb.Instance for SDK-based instance creation.
// This function orchestrates the various helper functions to build a complete
// instance configuration.
func (p *Provider) buildInstance(
	ctx context.Context,
	l *logger.Logger,
	zone string,
	name string,
	opts vm.CreateOpts,
	providerOpts *ProviderOpts,
	labels map[string]string,
) (*computepb.Instance, error) {
	project := p.GetProject()

	// Select and validate image
	image, imageProject, err := selectImage(opts, providerOpts)
	if err != nil {
		return nil, err
	}

	// Build disks configuration
	disks, err := buildDisks(l, zone, opts, providerOpts, labels, imageProject, image)
	if err != nil {
		return nil, err
	}

	// Build scheduling configuration
	scheduling, err := buildScheduling(opts, providerOpts)
	if err != nil {
		return nil, err
	}

	// Build metadata (startup script)
	metadata, err := buildMetadata(opts, providerOpts)
	if err != nil {
		return nil, err
	}

	// Assemble instance
	instance := &computepb.Instance{
		Name:                    proto.String(name),
		Zone:                    proto.String(zone),
		MachineType:             proto.String(machineTypeURL(zone, providerOpts.MachineType)),
		MinCpuPlatform:          buildMinCPUPlatform(l, providerOpts),
		Disks:                   disks,
		NetworkInterfaces:       buildNetworkInterfaces(project, zone),
		Scheduling:              scheduling,
		Metadata:                metadata,
		ServiceAccounts:         buildServiceAccounts(project, providerOpts, p.defaultProject),
		Labels:                  labels,
		AdvancedMachineFeatures: buildAdvancedFeatures(providerOpts),
	}

	return instance, nil
}

// ============================================================
// INSTANCE CONFIGURATION HELPERS
// ============================================================

// selectImage determines the image and image project to use based on architecture.
// It validates architecture compatibility with the machine type.
func selectImage(
	opts vm.CreateOpts, providerOpts *ProviderOpts,
) (image, imageProject string, err error) {
	image = providerOpts.Image
	imageProject = defaultImageProject

	// FIPS override
	if opts.Arch == string(vm.ArchFIPS) {
		image = FIPSImage
		imageProject = FIPSImageProject
	} else if providerOpts.useArmAMI() {
		// ARM64 override
		image = ARM64Image
	}

	// Validations
	if opts.Arch == string(vm.ArchARM64) && !providerOpts.useArmAMI() {
		return "", "", errors.Errorf("requested arch is arm64, but machine type is %s. Use an ARM64 VM", providerOpts.MachineType)
	}
	if providerOpts.useArmAMI() && (opts.Arch != "" && opts.Arch != string(vm.ArchARM64)) {
		return "", "", errors.Errorf("machine type %s is arm64, but requested arch is %s", providerOpts.MachineType, opts.Arch)
	}
	if opts.SSDOpts.UseLocalSSD && !providerOpts.machineTypeSupportsLocalSSD() {
		return "", "", errors.Errorf("local SSDs are not supported with %s instance types, use --local-ssd=false", providerOpts.MachineType)
	}

	return image, imageProject, nil
}

// buildBootDisk creates the boot disk configuration.
func buildBootDisk(
	zone string,
	opts vm.CreateOpts,
	providerOpts *ProviderOpts,
	labels map[string]string,
	imageProject, image string,
) *computepb.AttachedDisk {
	return &computepb.AttachedDisk{
		Boot:       proto.Bool(true),
		AutoDelete: proto.Bool(true),
		InitializeParams: &computepb.AttachedDiskInitializeParams{
			DiskSizeGb:  proto.Int64(int64(opts.OsVolumeSize)),
			DiskType:    proto.String(diskTypeURL(zone, providerOpts.BootDiskType)),
			SourceImage: proto.String(imageURL(imageProject, image)),
			Labels:      labels,
		},
		DeviceName: proto.String("persistent-disk-0"),
	}
}

// buildAdditionalDisks creates local SSD or persistent disk configurations.
// Returns nil if boot-disk-only is enabled.
func buildAdditionalDisks(
	l *logger.Logger,
	zone string,
	opts vm.CreateOpts,
	providerOpts *ProviderOpts,
	labels map[string]string,
) ([]*computepb.AttachedDisk, error) {
	if providerOpts.BootDiskOnly {
		return nil, nil
	}

	var disks []*computepb.AttachedDisk

	if opts.SSDOpts.UseLocalSSD {
		// Validate SSD count for machine type
		allowedCounts, err := AllowedLocalSSDCount(providerOpts.MachineType)
		if err != nil {
			return nil, err
		}

		// If only one count is allowed, the VM will be automatically
		// configured with that count of local SSDs.
		if len(allowedCounts) == 1 {
			// If only one count is allowed, the VM will be automatically configured
			// with that count of local SSDs. In case the user specified a different count,
			// warn it will be overridden.
			if providerOpts.SSDCount != allowedCounts[0] {
				l.Printf(
					"WARNING: %[1]q only supports %[2]d local SSDs. Setting --gce-local-ssd-count to %[2]d",
					providerOpts.MachineType,
					allowedCounts[0],
				)
			}
		} else {
			// Make sure the minimum number of local SSDs is met.
			minCount := allowedCounts[0]
			if providerOpts.SSDCount < minCount {
				// Adjust to minimum (matches gcloud behavior)
				l.Printf("WARNING: SSD count must be at least %d for %q. Setting --gce-local-ssd-count to %d", minCount, providerOpts.MachineType, minCount)
				providerOpts.SSDCount = minCount
			}

			// Make sure the requested number of local SSDs is allowed.
			if !slices.Contains(allowedCounts, providerOpts.SSDCount) {
				return nil, errors.Errorf(
					"requested %d local SSDs is not supported for %q. Allowed counts: %v",
					providerOpts.SSDCount,
					providerOpts.MachineType,
					allowedCounts,
				)
			}
		}

		// Create local SSDs
		for i := 0; i < providerOpts.SSDCount; i++ {
			disks = append(disks, &computepb.AttachedDisk{
				Type:       proto.String(computepb.AttachedDisk_SCRATCH.String()),
				AutoDelete: proto.Bool(true),
				Interface:  proto.String(computepb.AttachedDisk_NVME.String()),
				InitializeParams: &computepb.AttachedDiskInitializeParams{
					DiskType: proto.String(diskTypeURL(zone, "local-ssd")),
				},
			})
		}
	} else {
		// Create persistent disks
		for i := 1; i <= providerOpts.PDVolumeCount; i++ {
			initializeParams := &computepb.AttachedDiskInitializeParams{
				DiskSizeGb: proto.Int64(int64(providerOpts.PDVolumeSize)),
				DiskType:   proto.String(diskTypeURL(zone, providerOpts.PDVolumeType)),
				Labels:     labels,
			}

			// Add provisioned IOPS if specified (required for hyperdisk-balanced, optional for pd-extreme).
			if providerOpts.PDVolumeProvisionedIOPS > 0 {
				initializeParams.ProvisionedIops = proto.Int64(int64(providerOpts.PDVolumeProvisionedIOPS))
			}
			// Add provisioned throughput if specified (required for hyperdisk-balanced).
			if providerOpts.PDVolumeProvisionedThroughput > 0 {
				initializeParams.ProvisionedThroughput = proto.Int64(int64(providerOpts.PDVolumeProvisionedThroughput))
			}

			disks = append(disks, &computepb.AttachedDisk{
				AutoDelete:       proto.Bool(true),
				Boot:             proto.Bool(false),
				InitializeParams: initializeParams,
				DeviceName:       proto.String(fmt.Sprintf("persistent-disk-%d", i)),
			})
		}
	}

	return disks, nil
}

// buildDisks creates all disk configurations (boot + additional).
func buildDisks(
	l *logger.Logger,
	zone string,
	opts vm.CreateOpts,
	providerOpts *ProviderOpts,
	labels map[string]string,
	imageProject, image string,
) ([]*computepb.AttachedDisk, error) {
	// Boot disk (always present)
	disks := []*computepb.AttachedDisk{
		buildBootDisk(zone, opts, providerOpts, labels, imageProject, image),
	}

	// Additional disks (optional)
	additionalDisks, err := buildAdditionalDisks(l, zone, opts, providerOpts, labels)
	if err != nil {
		return nil, err
	}
	disks = append(disks, additionalDisks...)

	return disks, nil
}

// buildMinCPUPlatform returns the minimum CPU platform if configured.
// Returns nil for ARM instances or if not specified.
func buildMinCPUPlatform(l *logger.Logger, providerOpts *ProviderOpts) *string {
	if providerOpts.MinCPUPlatform == "" {
		return nil
	}

	if providerOpts.useArmAMI() {
		l.Printf("WARNING: --gce-min-cpu-platform is ignored for ARM64 instances")
		return nil
	}

	cpuPlatform := providerOpts.MinCPUPlatform
	// Adjust for n2d machines (AMD)
	if strings.HasPrefix(providerOpts.MachineType, "n2d-") && strings.HasPrefix(cpuPlatform, "Intel") {
		cpuPlatform = "AMD Milan"
	}

	return proto.String(cpuPlatform)
}

// buildNetworkInterfaces creates the network interface configuration.
func buildNetworkInterfaces(project, zone string) []*computepb.NetworkInterface {
	region := regionFromZone(zone)

	return []*computepb.NetworkInterface{
		{
			Network:    proto.String(fmt.Sprintf("projects/%s/global/networks/default", project)),
			Subnetwork: proto.String(fmt.Sprintf("projects/%s/regions/%s/subnetworks/default", project, region)),
			AccessConfigs: []*computepb.AccessConfig{
				{
					Name:        proto.String("External NAT"),
					Type:        proto.String(computepb.AccessConfig_ONE_TO_ONE_NAT.String()),
					NetworkTier: proto.String(computepb.AccessConfig_PREMIUM.String()),
				},
			},
		},
	}
}

// buildScheduling creates the scheduling configuration based on instance type
// (preemptible, spot, or regular).
func buildScheduling(
	opts vm.CreateOpts, providerOpts *ProviderOpts,
) (*computepb.Scheduling, error) {
	scheduling := &computepb.Scheduling{}

	if providerOpts.preemptible {
		// Validations
		if opts.Lifetime > 24*time.Hour {
			return nil, errors.New("lifetime cannot be longer than 24 hours for preemptible instances")
		}
		if !providerOpts.TerminateOnMigration {
			return nil, errors.New("preemptible instances require 'TERMINATE' maintenance policy; use --gce-terminateOnMigration")
		}

		scheduling.Preemptible = proto.Bool(true)
		scheduling.OnHostMaintenance = proto.String(computepb.Scheduling_TERMINATE.String())
		scheduling.AutomaticRestart = proto.Bool(false)

	} else if providerOpts.UseSpot {
		scheduling.ProvisioningModel = proto.String(computepb.Scheduling_SPOT.String())
		scheduling.OnHostMaintenance = proto.String(computepb.Scheduling_TERMINATE.String())

	} else {
		// Regular instance
		if providerOpts.TerminateOnMigration {
			scheduling.OnHostMaintenance = proto.String(computepb.Scheduling_TERMINATE.String())
		} else {
			scheduling.OnHostMaintenance = proto.String(computepb.Scheduling_MIGRATE.String())
		}
		scheduling.AutomaticRestart = proto.Bool(true)
	}

	return scheduling, nil
}

// buildMetadata creates the metadata configuration with startup script.
func buildMetadata(opts vm.CreateOpts, providerOpts *ProviderOpts) (*computepb.Metadata, error) {
	extraMountOpts := ""
	if !providerOpts.BootDiskOnly {
		if opts.SSDOpts.UseLocalSSD {
			extraMountOpts = "discard"
			if opts.SSDOpts.NoExt4Barrier {
				extraMountOpts = "discard,nobarrier"
			}
		} else {
			extraMountOpts = "discard"
		}
	}

	startupScriptContent, err := generateStartupScriptContent(
		extraMountOpts,
		opts.SSDOpts.FileSystem,
		providerOpts.UseMultipleDisks,
		opts.Arch == string(vm.ArchFIPS),
		providerOpts.EnableCron,
		providerOpts.BootDiskOnly,
	)
	if err != nil {
		return nil, err
	}

	return &computepb.Metadata{
		Items: []*computepb.Items{
			{
				Key:   proto.String("startup-script"),
				Value: proto.String(startupScriptContent),
			},
		},
	}, nil
}

// buildServiceAccounts creates the service account configuration.
func buildServiceAccounts(
	project string, providerOpts *ProviderOpts, defaultProject string,
) []*computepb.ServiceAccount {
	serviceAccount := providerOpts.ServiceAccount
	if project == defaultProject && serviceAccount == "" {
		serviceAccount = providerOpts.defaultServiceAccount
	}

	if serviceAccount == "" {
		return nil
	}

	return []*computepb.ServiceAccount{
		{
			Email:  proto.String(serviceAccount),
			Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"},
		},
	}
}

// buildAdvancedFeatures creates advanced machine features configuration
// (turbo mode, threads per core). Returns nil if no features are configured.
func buildAdvancedFeatures(providerOpts *ProviderOpts) *computepb.AdvancedMachineFeatures {
	if providerOpts.TurboMode == "" && providerOpts.ThreadsPerCore == 0 {
		return nil
	}

	features := &computepb.AdvancedMachineFeatures{}
	if providerOpts.TurboMode != "" {
		features.TurboMode = proto.String(providerOpts.TurboMode)
	}
	if providerOpts.ThreadsPerCore > 0 {
		features.ThreadsPerCore = proto.Int32(int32(providerOpts.ThreadsPerCore))
	}

	return features
}

// ============================================================
// URL GENERATION HELPERS
// ============================================================

// regionFromZone extracts the region from a zone name.
// Example: "us-east1-b" -> "us-east1"
func regionFromZone(zone string) string {
	lastDash := strings.LastIndex(zone, "-")
	if lastDash == -1 {
		return zone
	}
	return zone[:lastDash]
}

// machineTypeURL generates the full machine type URL for GCE API.
// Example: machineTypeURL("us-east1-b", "n2-standard-4") ->
//
//	"zones/us-east1-b/machineTypes/n2-standard-4"
func machineTypeURL(zone, machineType string) string {
	return fmt.Sprintf("zones/%s/machineTypes/%s", zone, machineType)
}

// diskTypeURL generates the full disk type URL for GCE API.
// Example: diskTypeURL("us-east1-b", "pd-ssd") ->
//
//	"zones/us-east1-b/diskTypes/pd-ssd"
func diskTypeURL(zone, diskType string) string {
	return fmt.Sprintf("zones/%s/diskTypes/%s", zone, diskType)
}

// imageURL generates the full image URL for GCE API.
// Example: imageURL("ubuntu-os-cloud", "ubuntu-2204-jammy-v20240319") ->
//
//	"projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v20240319"
func imageURL(project, image string) string {
	return fmt.Sprintf("projects/%s/global/images/%s", project, image)
}

// buildProjectZoneMap organizes a list of VMs into a map structure for batch operations.
// Returns a map from project -> zone -> list of VM names.
// This helper is used by reset, delete, and other operations that work on VMs grouped by project/zone.
func buildProjectZoneMap(vms vm.List) (map[string]map[string][]string, error) {
	projectZoneMap := make(map[string]map[string][]string)
	for _, v := range vms {
		if v.Provider != ProviderName {
			return nil, errors.Errorf("%s received VM instance from %s", ProviderName, v.Provider)
		}
		if projectZoneMap[v.Project] == nil {
			projectZoneMap[v.Project] = make(map[string][]string)
		}
		projectZoneMap[v.Project][v.Zone] = append(projectZoneMap[v.Project][v.Zone], v.Name)
	}
	return projectZoneMap, nil
}

// AddLabelsWithContext adds labels to VM instances using the SDK.
func (p *Provider) AddLabelsWithContext(
	ctx context.Context, l *logger.Logger, vms vm.List, labels map[string]string,
) error {
	return p.editLabels(ctx, l, vms, labels, false /* remove */)
}

// RemoveLabelsWithContext removes labels from VM instances using the SDK.
func (p *Provider) RemoveLabelsWithContext(
	ctx context.Context, l *logger.Logger, vms vm.List, labels []string,
) error {
	labelsMap := make(map[string]string, len(labels))
	for _, label := range labels {
		labelsMap[label] = ""
	}
	return p.editLabels(ctx, l, vms, labelsMap, true /* remove */)
}

// editLabelsWithSDK edits labels on VM instances using the SDK.
// This requires a Get + SetLabels pattern due to label fingerprinting.
// The remove parameter controls whether labels are added/updated or removed.
func (p *Provider) editLabels(
	ctx context.Context, l *logger.Logger, vms vm.List, labels map[string]string, remove bool,
) error {
	g := newLimitedErrorGroupWithContext(ctx)

	for _, v := range vms {
		g.GoCtx(func(ctx context.Context) error {
			// Get instance to retrieve label fingerprint
			getReq := &computepb.GetInstanceRequest{
				Project:  v.Project,
				Zone:     v.Zone,
				Instance: v.Name,
			}

			instance, err := p.computeInstancesClient.Get(ctx, getReq)
			if err != nil {
				return errors.Wrapf(err, "failed to get instance %s", v.Name)
			}

			// Merge or filter labels
			mergedLabels := make(map[string]string)
			if remove {
				// Copy existing labels, excluding the ones to remove
				for k, val := range instance.GetLabels() {
					if _, shouldRemove := labels[k]; !shouldRemove {
						mergedLabels[k] = val
					}
				}
			} else {
				// Copy existing labels
				for k, val := range instance.GetLabels() {
					mergedLabels[k] = val
				}
				// Add/update new labels
				for k, val := range labels {
					mergedLabels[k] = val
				}
			}

			instanceLabelsFingerprint := instance.GetLabelFingerprint()

			// Set labels with fingerprint
			setLabelsReq := &computepb.SetLabelsInstanceRequest{
				Project:  v.Project,
				Zone:     v.Zone,
				Instance: v.Name,
				InstancesSetLabelsRequestResource: &computepb.InstancesSetLabelsRequest{
					Labels:           mergedLabels,
					LabelFingerprint: &instanceLabelsFingerprint,
				},
			}

			op, err := p.computeInstancesClient.SetLabels(ctx, setLabelsReq)
			if err != nil {
				return errors.Wrapf(err, "failed to set labels on instance %s", v.Name)
			}

			// Wait for operation to complete
			if err := op.Wait(ctx); err != nil {
				return errors.Wrapf(err, "set labels operation failed for instance %s", v.Name)
			}

			// Check for operation errors
			if opErr := op.Proto().GetError(); opErr != nil {
				return errors.Errorf("set labels failed for instance %s: %v", v.Name, opErr)
			}

			return nil
		})
	}

	return g.Wait()
}

// computeLabelsMap generates labels for instance creation.
func computeLabelsMap(opts vm.CreateOpts, providerOpts *ProviderOpts) (map[string]string, error) {
	m := vm.GetDefaultLabelMap(opts)

	// Format timestamp for GCE label requirements
	timestamp := timeutil.Now().Format(time.RFC3339)
	timestamp = strings.ToLower(strings.ReplaceAll(timestamp, ":", "_"))
	m[vm.TagCreated] = timestamp

	// Add managed label
	if providerOpts.Managed {
		m[ManagedLabel] = "true"
	}

	// Add spot instance label
	if providerOpts.UseSpot {
		m[vm.TagSpotInstance] = "true"
	}

	// Add custom labels with validation
	for key, value := range opts.CustomLabels {
		if _, exists := m[strings.ToLower(key)]; exists {
			return nil, fmt.Errorf("duplicate label name defined: %s", key)
		}
		m[key] = value
	}

	return m, nil
}
