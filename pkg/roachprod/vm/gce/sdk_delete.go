// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"context"
	"time"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
)

// deleteWithSDK deletes VMs using the GCP SDK, routing to the appropriate
// deletion method based on whether the VMs are managed or unmanaged.
func (p *Provider) DeleteWithContext(ctx context.Context, l *logger.Logger, vms vm.List) error {
	switch {
	case isManaged(vms):
		return p.deleteManaged(ctx, l, vms)
	default:
		return p.deleteUnmanaged(ctx, l, vms)
	}
}

// deleteManagedWithSDK deletes managed instance groups, their instance templates,
// and associated load balancer resources using the GCP SDK.
func (p *Provider) deleteManaged(ctx context.Context, l *logger.Logger, vms vm.List) error {
	// Build map of cluster name to project
	clusterProjectMap := make(map[string]string)
	for _, v := range vms {
		clusterName, err := v.ClusterName()
		if err != nil {
			return err
		}
		clusterProjectMap[clusterName] = v.Project
	}

	// Delete load balancer resources first (prevents dependency errors when deleting instance groups)
	for cluster, project := range clusterProjectMap {
		err := p.deleteLoadBalancerResources(ctx, project, cluster, "" /* portFilter */)
		if err != nil {
			return err
		}
	}

	// Delete all instance groups in parallel
	g := newLimitedErrorGroupWithContext(ctx)
	for cluster, project := range clusterProjectMap {
		// Multiple instance groups can exist for a single cluster, one for each zone
		projectGroups, err := p.listManagedInstanceGroups(ctx, project, instanceGroupName(cluster))
		if err != nil {
			return err
		}

		for _, group := range projectGroups {

			groupName := *group.Name
			groupZone := lastComponent(*group.Zone)

			g.GoCtx(func(ctx context.Context) error {
				req := &computepb.DeleteInstanceGroupManagerRequest{
					Project:              project,
					Zone:                 groupZone,
					InstanceGroupManager: groupName,
				}

				op, err := p.computeInstanceGroupManagersClient.Delete(ctx, req)
				if err != nil {
					return errors.Wrapf(err, "failed to delete instance group %s in zone %s", groupName, groupZone)
				}

				// Wait for the operation to complete
				if err := op.Wait(ctx); err != nil {
					return errors.Wrapf(err, "failed to wait for instance group deletion %s in zone %s", groupName, groupZone)
				}

				// Check for operation errors
				if opErr := op.Proto().GetError(); opErr != nil {
					return errors.Newf("instance group deletion failed for %s in zone %s: %s", groupName, groupZone, opErr.String())
				}

				return nil
			})
		}
	}

	// Wait for all instance groups to be deleted
	if err := g.Wait(); err != nil {
		return err
	}

	// All instance groups must be deleted before templates can be deleted
	g = newLimitedErrorGroupWithContext(ctx)
	for cluster, project := range clusterProjectMap {
		templates, err := p.listInstanceTemplates(ctx, l, project, cluster)
		if err != nil {
			return err
		}

		for _, template := range templates {

			templateName := *template.Name

			g.GoCtx(func(ctx context.Context) error {
				req := &computepb.DeleteInstanceTemplateRequest{
					Project:          project,
					InstanceTemplate: templateName,
				}

				op, err := p.computeInstanceTemplatesClient.Delete(ctx, req)
				if err != nil {
					return errors.Wrapf(err, "failed to delete instance template %s", templateName)
				}

				// Wait for the operation to complete
				if err := op.Wait(ctx); err != nil {
					return errors.Wrapf(err, "failed to wait for template deletion %s", templateName)
				}

				// Check for operation errors
				if opErr := op.Proto().GetError(); opErr != nil {
					return errors.Newf("template deletion failed for %s: %s", templateName, opErr.String())
				}

				return nil
			})
		}
	}

	return g.Wait()
}

// deleteUnmanagedWithSDK deletes unmanaged VMs using the GCP SDK.
// This matches gcloud's "--delete-disks all" behavior by:
// 1. Fetching all attached disks for each instance
// 2. Setting AutoDelete=true on all disks (matching gcloud's flag modification)
// 3. Deleting the instance (GCP automatically deletes all disks)
func (p *Provider) deleteUnmanaged(ctx context.Context, l *logger.Logger, vms vm.List) error {
	// Organize VMs by project and zone for batch operations
	projectZoneMap, err := buildProjectZoneMap(vms)
	if err != nil {
		return err
	}

	// Set timeout for deletion operations
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	g := newLimitedErrorGroupWithContext(ctx)

	for project, zoneMap := range projectZoneMap {
		for zone, names := range zoneMap {

			// Delete each instance in parallel, up to MaxConcurrentCommands concurrent deletions
			// The error group's SetLimit already handles concurrency control
			for _, name := range names {
				g.GoCtx(func(ctx context.Context) error {

					// First, get the instance to find all attached disks
					// This matches gcloud's "--delete-disks all" behavior
					getReq := &computepb.GetInstanceRequest{
						Project:  project,
						Zone:     zone,
						Instance: name,
					}
					instance, err := p.computeInstancesClient.Get(ctx, getReq)
					if err != nil {
						return errors.Wrapf(err, "failed to get instance %s in zone %s", name, zone)
					}

					// Set AutoDelete=true on all disks that don't have it
					// This matches gcloud's "--delete-disks all" behavior which modifies
					// the auto-delete bits before issuing the deletion request
					for _, disk := range instance.GetDisks() {
						if !disk.GetAutoDelete() && disk.GetSource() != "" && disk.GetDeviceName() != "" {
							// Extract disk name from source URL
							diskName := lastComponent(disk.GetSource())

							setAutoDeleteReq := &computepb.SetDiskAutoDeleteInstanceRequest{
								Project:    project,
								Zone:       zone,
								Instance:   name,
								AutoDelete: true,
								DeviceName: disk.GetDeviceName(),
							}

							op, err := p.computeInstancesClient.SetDiskAutoDelete(ctx, setAutoDeleteReq)
							if err != nil {
								return errors.Wrapf(err, "failed to set auto-delete for disk %s on instance %s", diskName, name)
							}

							// Wait for the operation to complete
							if err := op.Wait(ctx); err != nil {
								return errors.Wrapf(err, "failed to wait for auto-delete setting on disk %s", diskName)
							}

							// Check for operation errors
							if opErr := op.Proto().GetError(); opErr != nil {
								return errors.Newf("setting auto-delete failed for disk %s: %s", diskName, opErr.String())
							}
						}
					}

					// Now delete the instance - all disks will be automatically deleted by GCP
					deleteReq := &computepb.DeleteInstanceRequest{
						Project:  project,
						Zone:     zone,
						Instance: name,
					}

					op, err := p.computeInstancesClient.Delete(ctx, deleteReq)
					if err != nil {
						return errors.Wrapf(err, "failed to delete instance %s in zone %s", name, zone)
					}

					// Wait for the instance deletion to complete
					if err := op.Wait(ctx); err != nil {
						return errors.Wrapf(err, "failed to wait for instance deletion %s in zone %s", name, zone)
					}

					// Check for operation errors
					if opErr := op.Proto().GetError(); opErr != nil {
						return errors.Newf("instance deletion failed for %s in zone %s: %s", name, zone, opErr.String())
					}

					return nil
				})
			}
		}
	}

	return g.Wait()
}
