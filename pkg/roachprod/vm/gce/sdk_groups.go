// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

// createManagedInstancesWithSDK creates managed instance groups and their instances using the SDK.
func (p *Provider) createManagedInstances(
	ctx context.Context,
	l *logger.Logger,
	names []string,
	opts vm.CreateOpts,
	providerOpts *ProviderOpts,
	labels map[string]string,
	usedZones []string,
	zoneToHostNames map[string][]string,
) (vm.List, error) {
	project := p.GetProject()

	// Build instance configurations for each zone
	zoneToInstanceConfig := make(map[string]*computepb.Instance)
	for _, zone := range usedZones {
		// Use a dummy name for template - actual instance names will be set during creation
		instance, err := p.buildInstance(ctx, l, zone, opts.ClusterName, opts, providerOpts, labels)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to build instance config for zone %s", zone)
		}
		zoneToInstanceConfig[zone] = instance
	}

	// Handle spot instance zones if specified
	if len(providerOpts.ManagedSpotZones) > 0 {
		if providerOpts.UseSpot {
			return nil, errors.Newf("Use either --gce-use-spot or --gce-managed-spot-zones, not both")
		}
		spotProviderOpts := *providerOpts
		spotProviderOpts.UseSpot = true
		for _, zone := range providerOpts.ManagedSpotZones {
			if _, ok := zoneToInstanceConfig[zone]; !ok {
				return nil, errors.Newf("the managed spot zone %q is not in the list of zones for the cluster", zone)
			}
			spotInstance, err := p.buildInstance(ctx, l, zone, opts.ClusterName, opts, &spotProviderOpts, labels)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to build spot instance config for zone %s", zone)
			}
			zoneToInstanceConfig[zone] = spotInstance
		}
	}

	// Create instance templates
	templates, err := p.createInstanceTemplates(ctx, l, opts.ClusterName, zoneToInstanceConfig, labels)
	if err != nil {
		return nil, err
	}

	// Create instance groups
	err = p.createInstanceGroups(ctx, l, project, opts.ClusterName, usedZones, templates, opts, providerOpts)
	if err != nil {
		return nil, err
	}

	// Create instances within the groups
	groupName := instanceGroupName(opts.ClusterName)
	g := newLimitedErrorGroupWithContext(ctx)

	l.Printf("Creating %d managed instances, distributed across [%s]", len(names), strings.Join(usedZones, ", "))

	for zone, hostnames := range zoneToHostNames {
		hostnames := hostnames

		for _, hostname := range hostnames {
			g.GoCtx(func(ctx context.Context) error {
				req := &computepb.CreateInstancesInstanceGroupManagerRequest{
					Project:              project,
					Zone:                 zone,
					InstanceGroupManager: groupName,
					InstanceGroupManagersCreateInstancesRequestResource: &computepb.InstanceGroupManagersCreateInstancesRequest{
						Instances: []*computepb.PerInstanceConfig{
							{
								Name: proto.String(hostname),
							},
						},
					},
				}

				op, err := p.computeInstanceGroupManagersClient.CreateInstances(ctx, req)
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

				return nil
			})
		}
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Wait for groups to become stable
	err = p.waitForGroupStability(ctx, l, project, groupName, usedZones)
	if err != nil {
		return nil, err
	}

	// Fetch the created VMs from the managed instance groups
	return p.getManagedInstanceGroupVMs(ctx, l, project, groupName, templates, p.dnsProvider.PublicDomain())
}

// createInstanceTemplatesWithSDK creates instance templates for a cluster using the SDK.
// One template is created per zone with the given instance configuration.
// This is used for managed instance groups.
func (p *Provider) createInstanceTemplates(
	ctx context.Context,
	l *logger.Logger,
	clusterName string,
	zoneToInstanceConfig map[string]*computepb.Instance,
	labels map[string]string,
) (map[string]*computepb.InstanceTemplate, error) {
	project := p.GetProject()
	templates := make(map[string]*computepb.InstanceTemplate)
	var templatesMu syncutil.Mutex
	g := newLimitedErrorGroupWithContext(ctx)

	l.Printf("Creating instance templates across %d zones", len(zoneToInstanceConfig))

	for zone, instanceConfig := range zoneToInstanceConfig {

		templateName := instanceTemplateName(clusterName, zone)

		g.GoCtx(func(ctx context.Context) error {
			// Build instance template from instance configuration
			// Note: Instance templates require just resource names (e.g., "n2-standard-4", "pd-ssd"),
			// not the full URLs that instances use (e.g., "zones/us-east1-d/machineTypes/n2-standard-4").
			// This applies to machine type, disk type, and other resource references.
			machineTypeName := instanceConfig.MachineType
			if machineTypeName != nil {
				// Extract just the machine type name from the URL
				machineTypeName = proto.String(lastComponent(*instanceConfig.MachineType))
			}

			// Transform disks to extract just the disk type names from URLs
			templateDisks := make([]*computepb.AttachedDisk, len(instanceConfig.Disks))
			for i, disk := range instanceConfig.Disks {
				templateDisk := &computepb.AttachedDisk{
					Boot:       disk.Boot,
					AutoDelete: disk.AutoDelete,
					Type:       disk.Type,
					Mode:       disk.Mode,
				}

				// Transform InitializeParams if present
				if disk.InitializeParams != nil {
					templateDisk.InitializeParams = &computepb.AttachedDiskInitializeParams{
						DiskSizeGb:  disk.InitializeParams.DiskSizeGb,
						SourceImage: disk.InitializeParams.SourceImage,
						Labels:      disk.InitializeParams.Labels,
					}

					// Extract just the disk type name from the URL
					if disk.InitializeParams.DiskType != nil {
						diskTypeName := lastComponent(*disk.InitializeParams.DiskType)
						templateDisk.InitializeParams.DiskType = proto.String(diskTypeName)
					}
				}

				templateDisks[i] = templateDisk
			}

			template := &computepb.InstanceTemplate{
				Name:        proto.String(templateName),
				Description: proto.String(fmt.Sprintf("Instance template for %s in %s", clusterName, zone)),
				Properties: &computepb.InstanceProperties{
					MachineType:             machineTypeName,
					MinCpuPlatform:          instanceConfig.MinCpuPlatform,
					Disks:                   templateDisks,
					NetworkInterfaces:       instanceConfig.NetworkInterfaces,
					Scheduling:              instanceConfig.Scheduling,
					Metadata:                instanceConfig.Metadata,
					ServiceAccounts:         instanceConfig.ServiceAccounts,
					Labels:                  labels,
					AdvancedMachineFeatures: instanceConfig.AdvancedMachineFeatures,
				},
			}

			// Create instance template
			req := &computepb.InsertInstanceTemplateRequest{
				Project:                  project,
				InstanceTemplateResource: template,
			}

			op, err := p.computeInstanceTemplatesClient.Insert(ctx, req)
			if err != nil {
				return errors.Wrapf(err, "failed to start instance template creation for %s", templateName)
			}

			// Wait for operation completion
			if err := op.Wait(ctx); err != nil {
				return errors.Wrapf(err, "instance template creation operation failed for %s", templateName)
			}

			// Check for operation errors
			if opErr := op.Proto().GetError(); opErr != nil {
				return errors.Errorf("instance template %s creation failed: %v", templateName, opErr)
			}

			// Fetch created template to get full details
			getTemplate, err := p.computeInstanceTemplatesClient.Get(ctx, &computepb.GetInstanceTemplateRequest{
				Project:          project,
				InstanceTemplate: templateName,
			})
			if err != nil {
				return errors.Wrapf(err, "failed to fetch created instance template %s", templateName)
			}

			templatesMu.Lock()
			defer templatesMu.Unlock()
			templates[zone] = getTemplate

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return templates, nil
}

// createInstanceGroups creates managed instance groups in each zone using the SDK.
// Each group is created with stateful IPs and disks configuration.
func (p *Provider) createInstanceGroups(
	ctx context.Context,
	l *logger.Logger,
	project, clusterName string,
	zones []string,
	templates map[string]*computepb.InstanceTemplate,
	opts vm.CreateOpts,
	providerOpts *ProviderOpts,
) error {
	groupName := instanceGroupName(clusterName)
	g := newLimitedErrorGroupWithContext(ctx)

	// Determine the number of stateful disks
	numStatefulDisks := 1
	if !opts.SSDOpts.UseLocalSSD && !providerOpts.BootDiskOnly {
		numStatefulDisks += providerOpts.PDVolumeCount
	}

	// Build stateful policy for disks
	statefulPolicy := &computepb.StatefulPolicy{
		PreservedState: &computepb.StatefulPolicyPreservedState{
			Disks: make(map[string]*computepb.StatefulPolicyPreservedStateDiskDevice),
		},
	}
	for i := 0; i < numStatefulDisks; i++ {
		diskName := fmt.Sprintf("persistent-disk-%d", i)
		statefulPolicy.PreservedState.Disks[diskName] = &computepb.StatefulPolicyPreservedStateDiskDevice{
			AutoDelete: proto.String(computepb.StatefulPolicyPreservedStateDiskDevice_ON_PERMANENT_INSTANCE_DELETION.String()),
		}
	}

	l.Printf("Creating instance groups across %d zones", len(zones))

	for _, zone := range zones {
		template := templates[zone]

		g.GoCtx(func(ctx context.Context) error {
			// Build instance group manager configuration
			instanceGroupManager := &computepb.InstanceGroupManager{
				Name:             proto.String(groupName),
				BaseInstanceName: proto.String(clusterName),
				InstanceTemplate: template.SelfLink,
				TargetSize:       proto.Int32(0), // Start with size 0
				StatefulPolicy:   statefulPolicy,
			}

			// Create instance group manager
			req := &computepb.InsertInstanceGroupManagerRequest{
				Project:                      project,
				Zone:                         zone,
				InstanceGroupManagerResource: instanceGroupManager,
			}

			op, err := p.computeInstanceGroupManagersClient.Insert(ctx, req)
			if err != nil {
				return errors.Wrapf(err, "failed to start instance group creation for %s in %s", groupName, zone)
			}

			// Wait for operation completion
			if err := op.Wait(ctx); err != nil {
				return errors.Wrapf(err, "instance group creation operation failed for %s in %s", groupName, zone)
			}

			// Check for operation errors
			if opErr := op.Proto().GetError(); opErr != nil {
				return errors.Errorf("instance group %s creation failed in %s: %v", groupName, zone, opErr)
			}

			return nil
		})
	}

	return g.Wait()
}

// waitForGroupStability waits for instance groups to become stable using the SDK.
// This polls the instance group manager status until it's stable or times out.
func (p *Provider) waitForGroupStability(
	ctx context.Context, l *logger.Logger, project, groupName string, zones []string,
) error {
	g := newLimitedErrorGroupWithContext(ctx)

	l.Printf("Waiting for instance groups to become stable across %d zones", len(zones))

	for _, zone := range zones {
		g.GoCtx(func(ctx context.Context) error {
			// Poll until stable or timeout
			maxAttempts := 120 // 10 minutes with 5-second intervals
			for attempt := 0; attempt < maxAttempts; attempt++ {
				req := &computepb.GetInstanceGroupManagerRequest{
					Project:              project,
					Zone:                 zone,
					InstanceGroupManager: groupName,
				}

				igm, err := p.computeInstanceGroupManagersClient.Get(ctx, req)
				if err != nil {
					return errors.Wrapf(err, "failed to get instance group manager %s in %s", groupName, zone)
				}

				// Check if group is stable (no pending actions)
				status := igm.GetStatus()
				if status != nil && status.GetIsStable() {
					return nil
				}

				// Wait before next poll
				time.Sleep(5 * time.Second)
			}

			return errors.Errorf("instance group %s in %s did not become stable within timeout", groupName, zone)
		})
	}

	return g.Wait()
}

// getManagedInstanceGroupVMs fetches VMs from managed instance groups using the SDK.
// It lists managed instances in each zone and converts them to vm.VM structs.
func (p *Provider) getManagedInstanceGroupVMs(
	ctx context.Context,
	l *logger.Logger,
	project, groupName string,
	templates map[string]*computepb.InstanceTemplate,
	dnsDomain string,
) (vm.List, error) {
	var vmList vm.List
	var vmListMutex syncutil.Mutex
	g := newLimitedErrorGroupWithContext(ctx)

	zones := maps.Keys(templates)
	l.Printf("Fetching instances from managed instance groups across %d zones", len(zones))

	for _, zone := range zones {
		g.GoCtx(func(ctx context.Context) error {
			// List managed instances in this zone
			req := &computepb.ListManagedInstancesInstanceGroupManagersRequest{
				Project:              project,
				Zone:                 zone,
				InstanceGroupManager: groupName,
			}

			it := p.computeInstanceGroupManagersClient.ListManagedInstances(ctx, req)

			// Collect VMs for this zone
			var zoneVMs vm.List
			for {
				managedInstance, err := it.Next()
				if errors.Is(err, iterator.Done) {
					break
				}
				if err != nil {
					return errors.Wrapf(err, "failed to list managed instances in %s", zone)
				}

				// Get full instance details
				instanceName := lastComponent(managedInstance.GetInstance())
				getInstance, err := p.computeInstancesClient.Get(ctx, &computepb.GetInstanceRequest{
					Project:  project,
					Zone:     zone,
					Instance: instanceName,
				})
				if err != nil {
					return errors.Wrapf(err, "failed to get instance %s", instanceName)
				}

				// Convert to vm.VM using existing conversion logic
				vm := (&sdkInstance{getInstance}).toVM(project, dnsDomain)
				zoneVMs = append(zoneVMs, *vm)
			}

			// Append all zone VMs to the shared list with proper locking
			vmListMutex.Lock()
			defer vmListMutex.Unlock()
			vmList = append(vmList, zoneVMs...)

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return vmList, nil
}

// ============================================================
// GROW/SHRINK OPERATIONS
// ============================================================

// ShrinkWithContext shrinks a managed instance group by deleting the specified VMs using the SDK.
func (p *Provider) ShrinkWithContext(
	ctx context.Context, l *logger.Logger, vmsToDelete vm.List, clusterName string,
) error {
	if !isManaged(vmsToDelete) {
		return errors.New("shrinking is only supported for managed instance groups")
	}

	project := vmsToDelete[0].Project
	groupName := instanceGroupName(clusterName)
	vmZones := make(map[string]vm.List)
	for _, cVM := range vmsToDelete {
		vmZones[cVM.Zone] = append(vmZones[cVM.Zone], cVM)
	}

	g := newLimitedErrorGroupWithContext(ctx)
	for zone, vms := range vmZones {
		// Build instance URLs required by the API
		// Format: zones/{zone}/instances/{name}
		instanceURLs := make([]string, len(vms))
		for i, v := range vms {
			instanceURLs[i] = fmt.Sprintf("zones/%s/instances/%s", zone, v.Name)
		}

		g.GoCtx(func(ctx context.Context) error {
			req := &computepb.DeleteInstancesInstanceGroupManagerRequest{
				Project:              project,
				Zone:                 zone,
				InstanceGroupManager: groupName,
				InstanceGroupManagersDeleteInstancesRequestResource: &computepb.InstanceGroupManagersDeleteInstancesRequest{
					Instances: instanceURLs,
				},
			}

			op, err := p.computeInstanceGroupManagersClient.DeleteInstances(ctx, req)
			if err != nil {
				return errors.Wrapf(err, "failed to start instance deletion in %s", zone)
			}

			// Wait for operation completion
			if err := op.Wait(ctx); err != nil {
				return errors.Wrapf(err, "instance deletion operation failed in %s", zone)
			}

			// Check for operation errors
			if opErr := op.Proto().GetError(); opErr != nil {
				return errors.Errorf("instance deletion failed in %s: %v", zone, opErr)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return p.waitForGroupStability(ctx, l, project, groupName, maps.Keys(vmZones))
}

// GrowWithContext grows a managed instance group by creating new instances using the SDK.
func (p *Provider) GrowWithContext(
	ctx context.Context, l *logger.Logger, vms vm.List, clusterName string, names []string,
) (vm.List, error) {
	if !isManaged(vms) {
		return nil, errors.New("growing is only supported for managed instance groups")
	}

	project := vms[0].Project
	groupName := instanceGroupName(clusterName)

	// List instance groups to get their current sizes
	groups, err := p.listManagedInstanceGroups(ctx, project, groupName)
	if err != nil {
		return nil, err
	}

	// Sort groups by size (smallest first)
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].GetTargetSize() < groups[j].GetTargetSize()
	})

	// Compute distribution of new VMs across zones
	newNodeCount := len(names)
	zoneToHostNames := p.computeHostNamesPerZone(groups, names, newNodeCount)

	// Track which VMs we're adding
	addedVms := make(map[string]bool)
	g := newLimitedErrorGroupWithContext(ctx)

	l.Printf("Growing cluster by %d instances across zones", newNodeCount)

	for _, group := range groups {
		zone := lastComponent(group.GetZone())
		hostnames, ok := zoneToHostNames[zone]
		if !ok || len(hostnames) == 0 {
			continue
		}

		for _, hostname := range hostnames {
			addedVms[hostname] = true

			g.GoCtx(func(ctx context.Context) error {
				req := &computepb.CreateInstancesInstanceGroupManagerRequest{
					Project:              project,
					Zone:                 zone,
					InstanceGroupManager: groupName,
					InstanceGroupManagersCreateInstancesRequestResource: &computepb.InstanceGroupManagersCreateInstancesRequest{
						Instances: []*computepb.PerInstanceConfig{
							{
								Name: proto.String(hostname),
							},
						},
					},
				}

				op, err := p.computeInstanceGroupManagersClient.CreateInstances(ctx, req)
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

				return nil
			})
		}
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Wait for group stability
	err = p.waitForGroupStability(ctx, l, project, groupName, maps.Keys(zoneToHostNames))
	if err != nil {
		return nil, err
	}

	// Fetch instance templates for the cluster
	templates, err := p.listInstanceTemplates(ctx, l, project, clusterName)
	if err != nil {
		return nil, err
	}

	zoneToInstanceTemplates := make(map[string]*computepb.InstanceTemplate)
	for _, t := range templates {
		zone := p.getZoneFromTemplate(t)
		zoneToInstanceTemplates[zone] = t
	}

	// Fetch all VMs from the managed instance groups
	vmList, err := p.getManagedInstanceGroupVMs(ctx, l, project, groupName, zoneToInstanceTemplates, p.dnsProvider.PublicDomain())
	if err != nil {
		return nil, err
	}

	// Filter to return only newly added VMs
	var addedVmList vm.List
	for _, v := range vmList {
		if _, ok := addedVms[v.Name]; ok {
			addedVmList = append(addedVmList, v)
		}
	}

	return addedVmList, nil
}

// listManagedInstanceGroupsWithSDK lists managed instance groups for a given group name using the SDK.
func (p *Provider) listManagedInstanceGroups(
	ctx context.Context, project, groupName string,
) ([]*computepb.InstanceGroupManager, error) {
	req := &computepb.AggregatedListInstanceGroupManagersRequest{
		Project: project,
		Filter:  proto.String(fmt.Sprintf("name=%s", groupName)),
	}

	it := p.computeInstanceGroupManagersClient.AggregatedList(ctx, req)

	var groups []*computepb.InstanceGroupManager
	for {
		pair, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "failed to list instance group managers")
		}

		if pair.Value != nil && len(pair.Value.InstanceGroupManagers) > 0 {
			groups = append(groups, pair.Value.InstanceGroupManagers...)
		}
	}

	return groups, nil
}

// computeHostNamesPerZoneWithSDK distributes VM hostnames across zones based on growth distribution.
func (p *Provider) computeHostNamesPerZone(
	groups []*computepb.InstanceGroupManager, vmNames []string, newNodeCount int,
) map[string][]string {
	addCounts := p.computeGrowDistribution(groups, newNodeCount)
	zoneToHostNames := make(map[string][]string)
	nameIndex := 0

	for idx, group := range groups {
		addCount := addCounts[idx]
		if addCount == 0 {
			continue
		}

		zone := lastComponent(group.GetZone())
		vmNamesForZone := make([]string, addCount)
		for i := 0; i < addCount; i++ {
			vmNamesForZone[i] = vmNames[nameIndex]
			nameIndex++
		}

		zoneToHostNames[zone] = vmNamesForZone
	}

	return zoneToHostNames
}

// computeGrowDistributionWithSDK computes the distribution of new nodes across instance groups.
// Groups must be sorted by size from smallest to largest before passing to this function.
func (p *Provider) computeGrowDistribution(
	groups []*computepb.InstanceGroupManager, newNodeCount int,
) []int {
	addCount := make([]int, len(groups))
	curIndex := 0

	for i := 0; i < newNodeCount; i++ {
		nextIndex := (curIndex + 1) % len(groups)
		currentSize := int(groups[curIndex].GetTargetSize()) + addCount[curIndex]
		nextSize := int(groups[nextIndex].GetTargetSize()) + addCount[nextIndex]

		if currentSize > nextSize {
			curIndex = nextIndex
		} else {
			curIndex = 0
		}
		addCount[curIndex]++
	}

	return addCount
}

// getZoneFromTemplate extracts the zone from an instance template name.
func (p *Provider) getZoneFromTemplate(template *computepb.InstanceTemplate) string {
	clusterName := template.Properties.Labels[vm.TagCluster]
	namePrefix := fmt.Sprintf("%s-", instanceTemplateNamePrefix(clusterName))
	return strings.TrimPrefix(template.GetName(), namePrefix)
}
