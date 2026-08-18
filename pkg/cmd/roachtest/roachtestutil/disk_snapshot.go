// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// CopySnapshotDataToNodes copies CockroachDB data from GCE disk snapshots to
// each CRDB node's primary disk, replacing any existing data in
// /mnt/data1/cockroach. This avoids using cluster.ApplySnapshots directly
// because GCE snapshot-backed disks have a hydration problem: reads are very
// slow (~50 MB/s with 250ms latency) until the disk is fully hydrated in the
// background.
//
// For each CRDB node, the function:
//  1. Finds the matching snapshot by node number suffix
//  2. Creates a temporary pd-ssd disk from the snapshot and attaches it
//  3. Mounts it read-only
//  4. Removes existing data at /mnt/data1/cockroach and copies snapshot
//     data in its place via rsync
//  5. Cleans up (unmount, detach, delete the temporary disk)
//
// Steps 2 and 5 run on the roachtest runner, since cluster VMs hold no
// permission to mutate cloud resources.
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

	// Use a per-node temp disk name to avoid collisions when running in
	// parallel across nodes.
	tempDiskName := fmt.Sprintf("%s-temp-snapshot-%04d", c.Name(), nodeID)

	// Create a temporary disk from the snapshot and attach it. Retry transient
	// GCE API errors (e.g. HTTP 502) which can occur when many nodes create
	// disks in parallel against the same zone.
	//
	// We use a count-based retry (rather than a duration-based one) because a
	// single disk creation call can itself take several minutes to fail with a
	// transient error; a short duration-based budget would be exhausted on the
	// first attempt before any retry occurs (see #170038). Retries are bounded
	// only by the test's context, not by an extra timeout layered on top.
	l.Printf("n%d: creating temp disk %s from snapshot", nodeID, tempDiskName)
	createOpts := vm.VolumeCreateOpts{
		Name: tempDiskName,
		Type: "pd-ssd",
	}
	const createDiskAttempts = 3
	createDiskBackoff := retry.Options{
		InitialBackoff: 30 * time.Second,
		MaxBackoff:     30 * time.Second,
		Multiplier:     1,
	}
	var attached vm.AttachedVolume
	var createDiskErr error
	if err := retry.WithMaxAttempts(ctx, createDiskBackoff, createDiskAttempts, func() error {
		var err error
		attached, err = roachprod.AttachVolumeFromSnapshot(
			ctx, l, c.Name(), nodeID, snap, createOpts,
		)
		if err == nil {
			return nil
		}
		// A prior attempt can create the disk and then lose the response (e.g. an
		// HTTP 502 after a successful create), so the retry finds our
		// uniquely-named disk already there. No provider API describes an
		// unattached disk; attach and delete need only its name and zone.
		if isGCEAlreadyExistsError(err.Error()) {
			l.Printf("n%d: disk %s already exists (created by prior attempt), reusing it",
				nodeID, tempDiskName)
			attached, createDiskErr = roachprod.AttachVolume(ctx, l, c.Name(), nodeID, vm.Volume{
				ProviderResourceID: tempDiskName,
				Name:               tempDiskName,
			})
			return nil
		}
		if isGCETransientError(err.Error()) {
			l.Printf("n%d: transient GCE error creating disk, retrying: %v", nodeID, err)
			return err
		}
		// Non-transient error; stop retrying.
		createDiskErr = err
		return nil
	}); err != nil {
		// Transient errors exhausted the retry attempts.
		return fmt.Errorf("n%d: failed to create disk from snapshot after %d attempts: %w",
			nodeID, createDiskAttempts, err)
	}
	if createDiskErr != nil {
		return fmt.Errorf("n%d: failed to create disk from snapshot: %w", nodeID, createDiskErr)
	}

	// Clean up the temp disk when we're done, regardless of success or
	// failure. Use a fresh context since the original may be cancelled
	// on test timeout, and we don't want to leave orphaned GCE disks.
	unmountCmd := `sudo umount /mnt/snapshot && sudo rmdir /mnt/snapshot`
	mounted := false
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		if mounted {
			if err := c.RunE(cleanupCtx, option.WithNodes(node), unmountCmd); err != nil {
				l.Printf("n%d: warning: failed to unmount snapshot disk: %v", nodeID, err)
			}
		}

		l.Printf("n%d: cleaning up temp disk %s", nodeID, tempDiskName)
		if err := roachprod.DetachAndDeleteVolume(
			cleanupCtx, l, c.Name(), nodeID, attached.Volume,
		); err != nil {
			l.Printf("n%d: warning: failed to delete temp disk %s: %v", nodeID, tempDiskName, err)
		}
	}()

	// Mount the temporary disk read-only.
	l.Printf("n%d: mounting snapshot disk at %s", nodeID, attached.DevicePath)
	mountCmd := fmt.Sprintf(
		`sudo mkdir -p /mnt/snapshot && sudo mount -o ro %s /mnt/snapshot`,
		attached.DevicePath,
	)
	if err := c.RunE(ctx, option.WithNodes(node), mountCmd); err != nil {
		return fmt.Errorf("n%d: failed to mount snapshot disk: %w", nodeID, err)
	}
	mounted = true

	// Remove existing data and copy snapshot data to the primary disk.
	l.Printf("n%d: wiping /mnt/data1/cockroach and copying snapshot data", nodeID)
	copyCmd := `sudo rm -rf /mnt/data1/cockroach && ` +
		`sudo rsync -ah --info=progress2 /mnt/snapshot/cockroach /mnt/data1/`
	if err := c.RunE(ctx, option.WithNodes(node), copyCmd); err != nil {
		return fmt.Errorf("n%d: failed to copy data from snapshot: %w", nodeID, err)
	}
	l.Printf("n%d: data copy complete", nodeID)

	return nil
}

// isGCEAlreadyExistsError checks whether a gcloud failure indicates the
// resource already exists. This can happen on a retry after a prior attempt's
// response was lost (e.g. HTTP 502): the server created the resource but the
// client never got the success response.
func isGCEAlreadyExistsError(msg string) bool {
	return strings.Contains(msg, "already exists")
}

// isGCETransientError checks whether a gcloud failure is a transient
// server-side error worth retrying.
//
// The failure this guards against is an HTML error page served by Google's
// front end rather than a structured Compute API error, so there is no reason
// code to match on and substring matching is the only option (see #168296):
//
//	ERROR: (gcloud.compute.disks.create) Could not fetch resource:
//	 - <html>...<title>Error 502 (Server Error)!!1</title>...
//	   <p><b>502.</b> <ins>That's an error.</ins>
//	   <p>Please try again in 30 seconds.
//
// Patterns must not collide with a digit sequence in the gcloud command line,
// which is part of the matched text: a bare "502" also matches a snapshot ID.
func isGCETransientError(msg string) bool {
	transientPatterns := []string{
		"Error 502",
		"Error 503",
		"Server Error",
		"Please try again",
		"Rate Limit",
		"RATE_LIMIT",
		"rate limit",
	}
	for _, p := range transientPatterns {
		if strings.Contains(msg, p) {
			return true
		}
	}
	return false
}
