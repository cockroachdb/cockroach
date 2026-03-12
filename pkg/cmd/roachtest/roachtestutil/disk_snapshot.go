// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
)

// CopySnapshotDataToNodes copies CockroachDB data from GCE disk snapshots to
// each CRDB node's primary disk. This avoids using cluster.ApplySnapshots
// directly because GCE snapshot-backed disks have a hydration problem: reads
// are very slow (~50 MB/s with 250ms latency) until the disk is fully hydrated
// in the background.
//
// For each CRDB node, the function:
//  1. Finds the matching snapshot by node number suffix
//  2. Creates a temporary pd-ssd disk from the snapshot
//  3. Attaches and mounts it read-only
//  4. Copies the data to the node's primary disk via rsync
//  5. Cleans up (unmount, detach, delete the temporary disk)
//
// All nodes are processed in parallel.
func CopySnapshotDataToNodes(
	ctx context.Context, t test.Test, c cluster.Cluster, snapshots []vm.VolumeSnapshot,
) {
	t.Status("copying snapshot data to CRDB nodes (avoiding GCE hydration problem)")

	g := t.NewGroup(task.WithContext(ctx))
	for _, nodeID := range c.CRDBNodes() {
		nodeID := nodeID
		g.Go(func(ctx context.Context, l *logger.Logger) error {
			return copySnapshotDataToNode(ctx, l, c, snapshots, nodeID)
		}, task.Name(fmt.Sprintf("copy-snapshot-n%d", nodeID)))
	}
	g.Wait()

	t.L().Printf("snapshot data successfully copied to all %d CRDB nodes",
		len(c.CRDBNodes()))
}

func copySnapshotDataToNode(
	ctx context.Context,
	l *logger.Logger,
	c cluster.Cluster,
	snapshots []vm.VolumeSnapshot,
	nodeID int,
) error {
	node := option.NodeListOption{nodeID}

	// Find the snapshot for this node. Snapshot names end with a zero-padded
	// node number, e.g. "index-backfill-tpce-100k-v24.3.0-n10-0003".
	suffix := fmt.Sprintf("-%04d", nodeID)
	var snap vm.VolumeSnapshot
	for _, s := range snapshots {
		if strings.HasSuffix(s.Name, suffix) {
			snap = s
			break
		}
	}
	if snap.ID == "" {
		return fmt.Errorf("no snapshot found for node %d (suffix %s)", nodeID, suffix)
	}
	l.Printf("n%d: using snapshot %s (ID: %s)", nodeID, snap.Name, snap.ID)

	// Query the GCE zone from instance metadata rather than hardcoding it.
	getZoneCmd := `curl -sf -H "Metadata-Flavor: Google" ` +
		`http://metadata.google.internal/computeMetadata/v1/instance/zone`
	zoneOutput, err := c.RunWithDetailsSingleNode(
		ctx, l, option.WithNodes(node), getZoneCmd,
	)
	if err != nil {
		return fmt.Errorf("n%d: failed to get zone from GCE metadata: %w", nodeID, err)
	}
	// Zone format: projects/PROJECT_NUMBER/zones/ZONE_NAME
	zoneParts := strings.Split(strings.TrimSpace(zoneOutput.Stdout), "/")
	if len(zoneParts) < 4 || zoneParts[len(zoneParts)-2] != "zones" {
		return fmt.Errorf(
			"n%d: unexpected zone format from metadata: %q", nodeID, zoneOutput.Stdout,
		)
	}
	zone := zoneParts[len(zoneParts)-1]

	// Use a per-node temp disk name to avoid collisions when running in
	// parallel across nodes.
	tempDiskName := fmt.Sprintf("%s-temp-snapshot-%04d", c.Name(), nodeID)
	deviceName := fmt.Sprintf("snapshot-disk-%04d", nodeID)

	// Create a temporary disk from the snapshot.
	l.Printf("n%d: creating temp disk %s from snapshot in zone %s",
		nodeID, tempDiskName, zone)
	createDiskCmd := fmt.Sprintf(
		`gcloud compute disks create %s --source-snapshot=%s --zone=%s --type=pd-ssd --quiet`,
		tempDiskName, snap.ID, zone,
	)
	if err := c.RunE(ctx, option.WithNodes(node), createDiskCmd); err != nil {
		return fmt.Errorf("n%d: failed to create disk from snapshot: %w", nodeID, err)
	}

	// From this point on, best-effort cleanup of the temp disk on failure.
	cleanupDisk := func() {
		l.Printf("n%d: cleaning up temp disk %s", nodeID, tempDiskName)
		detachCmd := fmt.Sprintf(
			`gcloud compute instances detach-disk $(hostname) --disk=%s --zone=%s --quiet`,
			tempDiskName, zone,
		)
		_ = c.RunE(ctx, option.WithNodes(node), detachCmd)
		deleteCmd := fmt.Sprintf(
			`gcloud compute disks delete %s --zone=%s --quiet`,
			tempDiskName, zone,
		)
		_ = c.RunE(ctx, option.WithNodes(node), deleteCmd)
	}

	// Attach the temporary disk.
	l.Printf("n%d: attaching temp disk", nodeID)
	attachCmd := fmt.Sprintf(
		`gcloud compute instances attach-disk $(hostname) --disk=%s --zone=%s --device-name=%s --quiet`,
		tempDiskName, zone, deviceName,
	)
	if err := c.RunE(ctx, option.WithNodes(node), attachCmd); err != nil {
		cleanupDisk()
		return fmt.Errorf("n%d: failed to attach snapshot disk: %w", nodeID, err)
	}

	// Mount the temporary disk read-only.
	l.Printf("n%d: mounting snapshot disk", nodeID)
	mountCmd := fmt.Sprintf(
		`sudo mkdir -p /mnt/snapshot && sudo mount -o ro /dev/disk/by-id/google-%s /mnt/snapshot`,
		deviceName,
	)
	if err := c.RunE(ctx, option.WithNodes(node), mountCmd); err != nil {
		cleanupDisk()
		return fmt.Errorf("n%d: failed to mount snapshot disk: %w", nodeID, err)
	}

	// Copy data from the snapshot to the primary disk.
	l.Printf("n%d: copying data from snapshot to local disk", nodeID)
	copyCmd := `sudo rm -rf /mnt/data1/cockroach && ` +
		`sudo rsync -ah --info=progress2 /mnt/snapshot/cockroach /mnt/data1/`
	if err := c.RunE(ctx, option.WithNodes(node), copyCmd); err != nil {
		// Attempt unmount before disk cleanup.
		_ = c.RunE(ctx, option.WithNodes(node),
			`sudo umount /mnt/snapshot && sudo rmdir /mnt/snapshot`)
		cleanupDisk()
		return fmt.Errorf("n%d: failed to copy data from snapshot: %w", nodeID, err)
	}
	l.Printf("n%d: data copy complete", nodeID)

	// Unmount, detach, and delete the temporary disk.
	unmountCmd := `sudo umount /mnt/snapshot && sudo rmdir /mnt/snapshot`
	if err := c.RunE(ctx, option.WithNodes(node), unmountCmd); err != nil {
		l.Printf("n%d: warning: failed to unmount snapshot disk: %v", nodeID, err)
	}

	detachCmd := fmt.Sprintf(
		`gcloud compute instances detach-disk $(hostname) --disk=%s --zone=%s --quiet`,
		tempDiskName, zone,
	)
	if err := c.RunE(ctx, option.WithNodes(node), detachCmd); err != nil {
		l.Printf("n%d: warning: failed to detach snapshot disk: %v", nodeID, err)
	}

	deleteCmd := fmt.Sprintf(
		`gcloud compute disks delete %s --zone=%s --quiet`,
		tempDiskName, zone,
	)
	if err := c.RunE(ctx, option.WithNodes(node), deleteCmd); err != nil {
		l.Printf("n%d: warning: failed to delete temp disk: %v", nodeID, err)
	}

	return nil
}
