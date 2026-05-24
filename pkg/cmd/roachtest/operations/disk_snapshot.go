// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations/helpers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	snapshotMaxAge         = 31 * 24 * time.Hour
	snapshotCreateCadence  = 14 * 24 * time.Hour
	snapshotRecoverCadence = 28 * 24 * time.Hour
	maxSnapshotPrefixLen   = 30
)

// snapshotPrefix returns the snapshot name prefix to use. If the
// --snapshot-prefix flag is set it takes precedence; otherwise the
// cluster name is used. The result is truncated to fit within the
// 63-character GCE snapshot name limit after roachprod appends its
// version/node/store suffix.
func snapshotPrefix(c cluster.Cluster) string {
	name := roachtestflags.SnapshotPrefix
	if name == "" {
		name = c.Name()
	}
	if len(name) > maxSnapshotPrefixLen {
		name = name[:maxSnapshotPrefixLen]
	}
	return name
}

// cleanupEnsureNodesRunning is a shared cleanup handler for both
// snapshot operations. It restarts any nodes that are down.
type cleanupEnsureNodesRunning struct{}

func (cl *cleanupEnsureNodesRunning) Cleanup(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) {
	var failed []int
	for _, nodeID := range c.All() {
		if helpers.CheckNodeHealth(ctx, o, c, nodeID, 3 /* maxAttempts */, 5*time.Second) {
			continue
		}
		o.Status(fmt.Sprintf("node %d is down, restarting via cockroach.sh", nodeID))
		if err := c.RunE(ctx, option.WithNodes(c.Node(nodeID)), "./cockroach.sh"); err != nil {
			o.L().Printf("failed to restart node %d: %v", nodeID, err)
			failed = append(failed, int(nodeID))
		}
	}
	if len(failed) > 0 {
		o.Errorf("failed to restart nodes %v", failed)
	}
}

// restartAllNodes restarts every node via cockroach.sh, which
// preserves the original startup flags.
func restartAllNodes(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) {
	o.Status("restarting all nodes via cockroach.sh")
	for _, nodeID := range c.All() {
		if err := c.RunE(ctx, option.WithNodes(c.Node(nodeID)), "./cockroach.sh"); err != nil {
			o.Fatalf("failed to restart node %d: %v", nodeID, err)
		}
	}
}

// runSnapshotCreate checks for existing snapshots and creates new ones
// if none exist or if all existing ones have exceeded snapshotMaxAge.
func runSnapshotCreate(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) (cleanup registry.OperationCleanup) {
	prefix := snapshotPrefix(c)
	cleanup = &cleanupEnsureNodesRunning{}

	snapshots, err := c.ListSnapshots(ctx, vm.VolumeSnapshotListOpts{
		NamePrefix: prefix,
	})
	if err != nil {
		o.Fatalf("failed to list snapshots: %v", err)
	}

	if len(snapshots) > 0 {
		oldSnapshots, err := c.ListSnapshots(ctx, vm.VolumeSnapshotListOpts{
			NamePrefix:    prefix,
			CreatedBefore: timeutil.Now().Add(-snapshotMaxAge),
		})
		if err != nil {
			o.Fatalf("failed to list old snapshots: %v", err)
		}
		if len(oldSnapshots) != len(snapshots) {
			o.L().Printf(
				"snapshots with prefix %q exist and %d of %d are still fresh; nothing to do",
				prefix, len(snapshots)-len(oldSnapshots), len(snapshots),
			)
			return cleanup
		}

		o.L().Printf("deleting %d expired snapshot(s)", len(oldSnapshots))

		if err := c.DeleteSnapshots(ctx, oldSnapshots...); err != nil {
			o.Fatalf("failed to delete old snapshot: %v", err)
		}
		o.Status("deleted expired snapshot")
	}

	o.Status("stopping all nodes before snapshot creation")
	if err := c.StopE(ctx, o.L(), option.DefaultStopOpts()); err != nil {
		o.Fatalf("failed to stop cluster: %v", err)
	}

	o.Status("creating snapshots")
	snapshots, err = c.CreateSnapshot(ctx, prefix)
	if err != nil {
		o.Fatalf("failed to create snapshots: %v", err)
	}
	o.L().Printf("created %d snapshot(s) with prefix %q", len(snapshots), prefix)

	restartAllNodes(ctx, o, c)
	return cleanup
}

// runSnapshotRecover restores the cluster from existing snapshots.
func runSnapshotRecover(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) (cleanup registry.OperationCleanup) {
	prefix := snapshotPrefix(c)
	cleanup = &cleanupEnsureNodesRunning{}

	snapshots, err := c.ListSnapshots(ctx, vm.VolumeSnapshotListOpts{
		NamePrefix: prefix,
	})
	if err != nil {
		o.Fatalf("failed to list snapshots: %v", err)
	}

	if len(snapshots) == 0 {
		o.L().Printf("no snapshots found with prefix %q; nothing to recover", prefix)
		return cleanup
	}

	for _, snap := range snapshots {
		if !snap.IsReady() {
			o.L().Printf(
				"snapshot %q is not ready (status=%s) for prefix %q; skipping recovery",
				snap.Name, snap.Status, prefix,
			)
			return cleanup
		}
	}
	o.L().Printf("found %d ready snapshot(s) with prefix %q", len(snapshots), prefix)

	o.Status("stopping all nodes before applying snapshot")
	if err := c.StopE(ctx, o.L(), option.DefaultStopOpts()); err != nil {
		o.Fatalf("failed to stop cluster: %v", err)
	}

	o.Status("wiping cluster data directories")
	if err := c.WipeE(ctx, o.L()); err != nil {
		o.Fatalf("failed to wipe cluster: %v", err)
	}

	o.Status(fmt.Sprintf("applying %d snapshot(s) to restore cluster volumes", len(snapshots)))
	if err := c.ApplySnapshots(ctx, snapshots); err != nil {
		o.Fatalf("failed to apply snapshots: %v", err)
	}

	restartAllNodes(ctx, o, c)

	o.Status("verifying cluster health after snapshot recovery")
	for _, nodeID := range c.All() {
		if !helpers.CheckNodeHealth(ctx, o, c, nodeID, 10 /* maxAttempts */, 10*time.Second) {
			o.Fatalf("node %d failed health check after snapshot recovery", nodeID)
		}
	}
	o.L().Printf("all nodes healthy — snapshot recovery complete")

	return cleanup
}

func registerDiskSnapshot(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "snapshot-create",
		Owner:              registry.OwnerTestEng,
		Timeout:            1 * time.Hour,
		CompatibleClouds:   registry.Clouds(spec.GCE, spec.AWS),
		CanRunConcurrently: registry.OperationCannotRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresNodes},
		Cadence:            snapshotCreateCadence,
		Run:                runSnapshotCreate,
	})
	r.AddOperation(registry.OperationSpec{
		Name:               "snapshot-recover",
		Owner:              registry.OwnerTestEng,
		Timeout:            1 * time.Hour,
		CompatibleClouds:   registry.Clouds(spec.GCE, spec.AWS),
		CanRunConcurrently: registry.OperationCannotRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresNodes},
		Cadence:            snapshotRecoverCadence,
		Run:                runSnapshotRecover,
	})
}
