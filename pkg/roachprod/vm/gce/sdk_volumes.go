// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

// ListVolumesWithContext lists all non-boot persistent disks attached to a VM using the GCP SDK.
// This matches the gcloud implementation which fetches instance disks and filters out boot volumes.
func (p *Provider) ListVolumesWithContext(
	ctx context.Context, l *logger.Logger, v *vm.VM,
) ([]vm.Volume, error) {
	// Get the instance to access its attached disks
	getReq := &computepb.GetInstanceRequest{
		Project:  v.Project,
		Zone:     v.Zone,
		Instance: v.Name,
	}

	instance, err := p.computeInstancesClient.Get(ctx, getReq)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get instance %s", v.Name)
	}

	// Collect disk sources for detailed information
	// We need to fetch disk details separately to get labels and accurate type information
	var volumes []vm.Volume
	var bootDiskCount, localSSDCount int

	for _, attachedDisk := range instance.GetDisks() {
		// Skip boot disks
		if attachedDisk.GetBoot() {
			bootDiskCount++
			continue
		}

		// Handle local SSDs (scratch disks)
		if attachedDisk.GetType() == computepb.AttachedDisk_SCRATCH.String() {
			localSSDCount++
			volumes = append(volumes, vm.Volume{
				Size:               int(attachedDisk.GetDiskSizeGb()),
				ProviderVolumeType: "local-ssd",
				// Local SSDs don't have persistent names/zones/labels
			})
			continue
		}

		// For persistent disks, we need to fetch detailed information
		if attachedDisk.GetSource() == "" {
			continue
		}

		diskName := lastComponent(attachedDisk.GetSource())
		diskZone := zoneFromSelfLink(attachedDisk.GetSource())

		// Get detailed disk information including labels and accurate type
		getDiskReq := &computepb.GetDiskRequest{
			Project: v.Project,
			Zone:    diskZone,
			Disk:    diskName,
		}

		disk, err := p.computeDisksClient.Get(ctx, getDiskReq)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get disk details for %s", diskName)
		}

		volumes = append(volumes, diskToVolume(disk))
	}

	return volumes, nil
}

// validateVolumeCreateOpts validates the options for creating a volume.
// Extracted for testability.
func validateVolumeCreateOpts(vco vm.VolumeCreateOpts) error {
	if vco.IOPS != 0 && vco.Type != "hyperdisk-balanced" && vco.Type != "pd-extreme" {
		return errors.New("Creating a volume with IOPS is not supported at this time.")
	}
	if vco.Size == 0 {
		return errors.New("Cannot create a volume of size 0")
	}
	if vco.Encrypted {
		return errors.New("Volume encryption is not implemented for GCP")
	}

	// Validate architecture if specified
	if vco.Architecture != "" {
		if err := validateArchitecture(vco.Architecture); err != nil {
			return err
		}
	}

	// Validate disk type if specified
	if vco.Type != "" {
		if err := validateDiskType(vco.Type); err != nil {
			return err
		}
	}

	return nil
}

// validateArchitecture validates that the architecture is supported.
// Extracted for testability.
func validateArchitecture(arch string) error {
	if arch != "ARM64" && arch != "X86_64" {
		return errors.Newf("Expected architecture to be one of ARM64, X86_64 got %s", arch)
	}
	return nil
}

// validateDiskType validates that the disk type is supported.
// Extracted for testability.
func validateDiskType(diskType string) error {
	validTypes := map[string]bool{
		"local-ssd":          true,
		"pd-balanced":        true,
		"pd-extreme":         true,
		"pd-ssd":             true,
		"pd-standard":        true,
		"hyperdisk-balanced": true,
	}
	if !validTypes[diskType] {
		return errors.Newf("Expected type to be one of %s got %s",
			strings.Join(slices.Collect(maps.Keys(validTypes)), ", "),
			diskType,
		)
	}
	return nil
}

// buildDiskSourceURL creates the GCP URL for a disk source.
// Extracted for testability.
func buildDiskSourceURL(project, zone, diskName string) string {
	return fmt.Sprintf("projects/%s/zones/%s/disks/%s", project, zone, diskName)
}

// buildDiskTypeURL creates the GCP URL for a disk type.
// Extracted for testability (note: this duplicates diskTypeURL in sdk_instances.go, consider consolidating).
func buildDiskTypeURL(project, zone, diskType string) string {
	return fmt.Sprintf("projects/%s/zones/%s/diskTypes/%s", project, zone, diskType)
}

// buildDevicePath creates the device path for an attached disk.
// Extracted for testability.
func buildDevicePath(diskName string) string {
	return "/dev/disk/by-id/google-" + diskName
}

// buildSnapshotFilters creates filter strings for snapshot listing.
// Extracted for testability.
func buildSnapshotFilters(vslo vm.VolumeSnapshotListOpts) []string {
	var filters []string
	if vslo.NamePrefix != "" {
		filters = append(filters, fmt.Sprintf("name:%s*", vslo.NamePrefix))
	}
	if !vslo.CreatedBefore.IsZero() {
		filters = append(filters, fmt.Sprintf("creationTimestamp<'%s'", vslo.CreatedBefore.Format("2006-01-02")))
	}
	for k, v := range vslo.Labels {
		filters = append(filters, fmt.Sprintf("labels.%s=%s", k, v))
	}
	return filters
}

// buildDiskResource creates a computepb.Disk for volume creation.
// This is extracted for testability and follows the pattern used in sdk_instances.go.
func buildDiskResource(vco vm.VolumeCreateOpts, project string) *computepb.Disk {
	disk := &computepb.Disk{
		Name:   &vco.Name,
		SizeGb: proto.Int64(int64(vco.Size)),
	}

	// Set disk type if specified
	if vco.Type != "" {
		diskTypeURL := buildDiskTypeURL(project, vco.Zone, vco.Type)
		disk.Type = &diskTypeURL
	}

	// Set IOPS if specified and supported
	if vco.IOPS != 0 && (vco.Type == "hyperdisk-balanced" || vco.Type == "pd-extreme") {
		disk.ProvisionedIops = proto.Int64(int64(vco.IOPS))
	}

	// Set architecture if specified
	if vco.Architecture != "" {
		disk.Architecture = &vco.Architecture
	}

	// Set source snapshot if specified
	if vco.SourceSnapshotID != "" {
		disk.SourceSnapshot = &vco.SourceSnapshotID
	}

	// Set labels if provided
	if len(vco.Labels) > 0 {
		disk.Labels = make(map[string]string)
		for k, v := range vco.Labels {
			disk.Labels[serializeLabel(k)] = serializeLabel(v)
		}
	}

	return disk
}

// diskToVolume converts a computepb.Disk to a vm.Volume.
// This is extracted for testability and ensures consistent conversion logic.
func diskToVolume(disk *computepb.Disk) vm.Volume {
	return vm.Volume{
		ProviderResourceID: disk.GetName(),
		ProviderVolumeType: lastComponent(disk.GetType()),
		Zone:               lastComponent(disk.GetZone()),
		Encrypted:          false, // Only used for AWS
		Name:               disk.GetName(),
		Labels:             disk.GetLabels(),
		Size:               int(disk.GetSizeGb()),
	}
}

// CreateVolumeWithContext creates a persistent disk volume using the GCP SDK.
// This matches the gcloud implementation which creates a disk with specified
// configuration and optionally adds labels after creation.
func (p *Provider) CreateVolumeWithContext(
	ctx context.Context, l *logger.Logger, vco vm.VolumeCreateOpts,
) (vm.Volume, error) {
	// Validate inputs (matching gcloud implementation)
	if err := validateVolumeCreateOpts(vco); err != nil {
		return vm.Volume{}, err
	}

	// Build the disk resource using extracted helper
	disk := buildDiskResource(vco, p.GetProject())

	// Create the disk
	req := &computepb.InsertDiskRequest{
		Project:      p.GetProject(),
		Zone:         vco.Zone,
		DiskResource: disk,
	}

	op, err := p.computeDisksClient.Insert(ctx, req)
	if err != nil {
		return vm.Volume{}, errors.Wrapf(err, "failed to create disk %s", vco.Name)
	}

	// Wait for the operation to complete
	if err := op.Wait(ctx); err != nil {
		return vm.Volume{}, errors.Wrapf(err, "failed to wait for disk creation %s", vco.Name)
	}

	// Check for operation errors
	if opErr := op.Proto().GetError(); opErr != nil {
		return vm.Volume{}, errors.Newf("disk creation failed for %s: %s", vco.Name, opErr.String())
	}

	// Get the created disk to return full details
	getReq := &computepb.GetDiskRequest{
		Project: p.GetProject(),
		Zone:    vco.Zone,
		Disk:    vco.Name,
	}

	createdDisk, err := p.computeDisksClient.Get(ctx, getReq)
	if err != nil {
		return vm.Volume{}, errors.Wrapf(err, "failed to get created disk %s", vco.Name)
	}

	// Convert using extracted helper
	return diskToVolume(createdDisk), nil
}

// DeleteVolumeWithContext deletes a persistent disk volume using the GCP SDK.
// This matches the gcloud implementation which first detaches the disk from
// the instance, then deletes it.
func (p *Provider) DeleteVolumeWithContext(
	ctx context.Context, l *logger.Logger, volume vm.Volume, v *vm.VM,
) error {
	// First, detach the disk from the instance
	detachReq := &computepb.DetachDiskInstanceRequest{
		Project:    v.Project,
		Zone:       v.Zone,
		Instance:   v.Name,
		DeviceName: volume.ProviderResourceID,
	}

	op, err := p.computeInstancesClient.DetachDisk(ctx, detachReq)
	if err != nil {
		return errors.Wrapf(err, "failed to detach disk %s from instance %s", volume.ProviderResourceID, v.Name)
	}

	// Wait for the detach operation to complete
	if err := op.Wait(ctx); err != nil {
		return errors.Wrapf(err, "failed to wait for disk detach %s", volume.ProviderResourceID)
	}

	// Check for operation errors
	if opErr := op.Proto().GetError(); opErr != nil {
		return errors.Newf("disk detach failed for %s: %s", volume.ProviderResourceID, opErr.String())
	}

	// Now delete the disk
	deleteReq := &computepb.DeleteDiskRequest{
		Project: p.GetProject(),
		Zone:    volume.Zone,
		Disk:    volume.ProviderResourceID,
	}

	op, err = p.computeDisksClient.Delete(ctx, deleteReq)
	if err != nil {
		return errors.Wrapf(err, "failed to delete disk %s", volume.ProviderResourceID)
	}

	// Wait for the delete operation to complete
	if err := op.Wait(ctx); err != nil {
		return errors.Wrapf(err, "failed to wait for disk deletion %s", volume.ProviderResourceID)
	}

	// Check for operation errors
	if opErr := op.Proto().GetError(); opErr != nil {
		return errors.Newf("disk deletion failed for %s: %s", volume.ProviderResourceID, opErr.String())
	}

	return nil
}

// AttachVolumeWithContext attaches a persistent disk to a VM instance using the GCP SDK.
// This matches the gcloud implementation which attaches the disk and then sets
// the auto-delete flag to true.
func (p *Provider) AttachVolumeWithContext(
	ctx context.Context, l *logger.Logger, volume vm.Volume, v *vm.VM,
) (string, error) {
	// Build the attached disk configuration
	attachedDisk := &computepb.AttachedDisk{
		Source:     proto.String(buildDiskSourceURL(p.GetProject(), volume.Zone, volume.ProviderResourceID)),
		DeviceName: proto.String(volume.ProviderResourceID),
		AutoDelete: proto.Bool(false), // Will be set to true in a separate operation
	}

	// Attach the disk to the instance
	attachReq := &computepb.AttachDiskInstanceRequest{
		Project:              v.Project,
		Zone:                 v.Zone,
		Instance:             v.ProviderID,
		AttachedDiskResource: attachedDisk,
	}

	op, err := p.computeInstancesClient.AttachDisk(ctx, attachReq)
	if err != nil {
		return "", errors.Wrapf(err, "failed to attach disk %s to instance %s", volume.ProviderResourceID, v.ProviderID)
	}

	// Wait for the attach operation to complete
	if err := op.Wait(ctx); err != nil {
		return "", errors.Wrapf(err, "failed to wait for disk attach %s", volume.ProviderResourceID)
	}

	// Check for operation errors
	if opErr := op.Proto().GetError(); opErr != nil {
		return "", errors.Newf("disk attach failed for %s: %s", volume.ProviderResourceID, opErr.String())
	}

	// Verify the disk was attached by getting the instance
	getReq := &computepb.GetInstanceRequest{
		Project:  v.Project,
		Zone:     v.Zone,
		Instance: v.Name,
	}

	instance, err := p.computeInstancesClient.Get(ctx, getReq)
	if err != nil {
		return "", errors.Wrapf(err, "failed to verify disk attachment for %s", volume.ProviderResourceID)
	}

	// Check if the disk is in the list of attached disks
	found := false
	for _, disk := range instance.GetDisks() {
		if disk.GetSource() != "" && lastComponent(disk.GetSource()) == volume.ProviderResourceID {
			found = true
			break
		}
	}

	if !found {
		return "", errors.Newf("Could not find attached disk '%s' in list of disks for %s", volume.ProviderResourceID, v.ProviderID)
	}

	// Set auto-delete to true
	setAutoDeleteReq := &computepb.SetDiskAutoDeleteInstanceRequest{
		Project:    v.Project,
		Zone:       v.Zone,
		Instance:   v.ProviderID,
		AutoDelete: true,
		DeviceName: volume.ProviderResourceID,
	}

	op, err = p.computeInstancesClient.SetDiskAutoDelete(ctx, setAutoDeleteReq)
	if err != nil {
		return "", errors.Wrapf(err, "failed to set auto-delete for disk %s", volume.ProviderResourceID)
	}

	// Wait for the auto-delete operation to complete
	if err := op.Wait(ctx); err != nil {
		return "", errors.Wrapf(err, "failed to wait for auto-delete setting %s", volume.ProviderResourceID)
	}

	// Check for operation errors
	if opErr := op.Proto().GetError(); opErr != nil {
		return "", errors.Newf("setting auto-delete failed for %s: %s", volume.ProviderResourceID, opErr.String())
	}

	// Return the device path (matching gcloud behavior)
	return buildDevicePath(volume.ProviderResourceID), nil
}

// CreateVolumeSnapshotWithContext creates a snapshot of a persistent disk using the GCP SDK.
// This matches the gcloud implementation which creates a snapshot and then adds labels.
func (p *Provider) CreateVolumeSnapshotWithContext(
	ctx context.Context, l *logger.Logger, volume vm.Volume, vsco vm.VolumeSnapshotCreateOpts,
) (vm.VolumeSnapshot, error) {
	// Build the snapshot configuration
	snapshot := &computepb.Snapshot{
		Name:        proto.String(vsco.Name),
		Description: proto.String(vsco.Description),
	}

	// Set labels if provided
	if len(vsco.Labels) > 0 {
		snapshot.Labels = make(map[string]string)
		for k, v := range vsco.Labels {
			snapshot.Labels[serializeLabel(k)] = serializeLabel(v)
		}
	}

	// Create the snapshot request
	sourceDisk := buildDiskSourceURL(p.GetProject(), volume.Zone, volume.ProviderResourceID)
	req := &computepb.InsertSnapshotRequest{
		Project:          p.GetProject(),
		SnapshotResource: snapshot,
	}

	// Note: Source disk must be set as a query parameter, not in the snapshot resource
	// We use the disk's self-link format
	req.SnapshotResource.SourceDisk = &sourceDisk

	op, err := p.computeSnapshotsClient.Insert(ctx, req)
	if err != nil {
		return vm.VolumeSnapshot{}, errors.Wrapf(err, "failed to create snapshot %s", vsco.Name)
	}

	// Wait for the operation to complete
	if err := op.Wait(ctx); err != nil {
		return vm.VolumeSnapshot{}, errors.Wrapf(err, "failed to wait for snapshot creation %s", vsco.Name)
	}

	// Check for operation errors
	if opErr := op.Proto().GetError(); opErr != nil {
		return vm.VolumeSnapshot{}, errors.Newf("snapshot creation failed for %s: %s", vsco.Name, opErr.String())
	}

	// Get the created snapshot to return full details
	getReq := &computepb.GetSnapshotRequest{
		Project:  p.GetProject(),
		Snapshot: vsco.Name,
	}

	createdSnapshot, err := p.computeSnapshotsClient.Get(ctx, getReq)
	if err != nil {
		return vm.VolumeSnapshot{}, errors.Wrapf(err, "failed to get created snapshot %s", vsco.Name)
	}

	return vm.VolumeSnapshot{
		ID:   fmt.Sprintf("%d", createdSnapshot.GetId()),
		Name: createdSnapshot.GetName(),
	}, nil
}

// ListVolumeSnapshotsWithContext lists snapshots matching the given criteria using the GCP SDK.
// This matches the gcloud implementation which filters snapshots by name prefix, creation time, and labels.
func (p *Provider) ListVolumeSnapshotsWithContext(
	ctx context.Context, l *logger.Logger, vslo vm.VolumeSnapshotListOpts,
) ([]vm.VolumeSnapshot, error) {
	// Build the filter string (matching gcloud's filter format)
	filters := buildSnapshotFilters(vslo)

	req := &computepb.ListSnapshotsRequest{
		Project: p.GetProject(),
	}

	if len(filters) > 0 {
		filterStr := strings.Join(filters, " AND ")
		req.Filter = &filterStr
	}

	var snapshots []vm.VolumeSnapshot
	it := p.computeSnapshotsClient.List(ctx, req)

	for {
		snapshot, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "failed to list snapshots")
		}

		// Additional name prefix check (matching gcloud behavior)
		if vslo.NamePrefix != "" && !strings.HasPrefix(snapshot.GetName(), vslo.NamePrefix) {
			continue
		}

		snapshots = append(snapshots, vm.VolumeSnapshot{
			ID:   fmt.Sprintf("%d", snapshot.GetId()),
			Name: snapshot.GetName(),
		})
	}

	sort.Sort(vm.VolumeSnapshots(snapshots))
	return snapshots, nil
}

// DeleteVolumeSnapshotsWithContext deletes the given snapshots using the GCP SDK.
// This matches the gcloud implementation which deletes multiple snapshots in parallel.
func (p *Provider) DeleteVolumeSnapshotsWithContext(
	ctx context.Context, l *logger.Logger, snapshots ...vm.VolumeSnapshot,
) error {
	if len(snapshots) == 0 {
		return nil
	}

	g := newLimitedErrorGroupWithContext(ctx)

	for _, snapshot := range snapshots {
		snapshotName := snapshot.Name

		g.GoCtx(func(ctx context.Context) error {
			req := &computepb.DeleteSnapshotRequest{
				Project:  p.GetProject(),
				Snapshot: snapshotName,
			}

			op, err := p.computeSnapshotsClient.Delete(ctx, req)
			if err != nil {
				return errors.Wrapf(err, "failed to delete snapshot %s", snapshotName)
			}

			// Wait for the operation to complete
			if err := op.Wait(ctx); err != nil {
				return errors.Wrapf(err, "failed to wait for snapshot deletion %s", snapshotName)
			}

			// Check for operation errors
			if opErr := op.Proto().GetError(); opErr != nil {
				return errors.Newf("snapshot deletion failed for %s: %s", snapshotName, opErr.String())
			}

			return nil
		})
	}

	return g.Wait()
}
