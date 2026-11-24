// Copyright 2024 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/roachprod/failureinjection/failures"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

type cleanupDiskStall struct {
	failer *failures.Failer
	// fullLifecycle indicates whether the failer owns the full Setup/Cleanup
	// lifecycle. When true (cgroup), cleanup calls Recover, waits for
	// stabilization, then tears down via Failer.Cleanup. When false
	// (dmsetup), cleanup only calls Recover and waits because Setup was
	// performed at cluster creation time and calling Failer.Cleanup would
	// destroy the dmsetup device permanently.
	fullLifecycle bool
}

func (cl *cleanupDiskStall) Cleanup(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	o.Status("recovering from disk stall")
	if err := cl.failer.Recover(ctx, o.L()); err != nil {
		o.L().Printf("failed to recover from disk stall: %v", err)
	}
	if err := cl.failer.WaitForFailureToRecover(ctx, o.L()); err != nil {
		o.L().Printf("disk stall: node failed to stabilize: %v", err)
	}
	if cl.fullLifecycle {
		if err := cl.failer.Cleanup(ctx, o.L()); err != nil {
			o.Fatalf("failed to cleanup disk stall: %v", err)
		}
	}
}

func runDmsetupDiskStall(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) (cleanup registry.OperationCleanup) {
	rng, _ := randutil.NewPseudoRand()

	nodes := c.All()
	nid := nodes[rng.Intn(len(nodes))]

	// Disable state validation since dmsetup Setup() runs during cluster
	// creation, not as part of this operation.
	failer, args, err := roachtestutil.MakeDmsetupDiskStallFailer(
		o.L(), c, c.Node(nid), true, /* disableStateValidation */
	)
	if err != nil {
		o.Fatal(err)
	}

	// Assign cleanup handler early, before stalling the disk.
	cleanup = &cleanupDiskStall{failer: failer, fullLifecycle: false}

	o.Status(fmt.Sprintf("stalling disk on node %d via dmsetup", nid))
	if err := failer.Inject(ctx, o.L(), args); err != nil {
		o.Fatal(err)
	}

	return cleanup
}

func runCgroupDiskStall(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) (cleanup registry.OperationCleanup) {
	rng, _ := randutil.NewPseudoRand()

	nodes := c.All()
	nid := nodes[rng.Intn(len(nodes))]

	failer, args, err := roachtestutil.MakeCgroupDiskStallFailer(
		o.L(), c, c.Node(nid),
		true,  /* stallWrites */
		false, /* stallReads */
		false, /* stallLogs */
	)
	if err != nil {
		o.Fatal(err)
	}

	// Assign cleanup handler before Setup so partial failures are cleaned up.
	cleanup = &cleanupDiskStall{failer: failer, fullLifecycle: true}

	if err := failer.Setup(ctx, o.L(), args); err != nil {
		o.Fatal(err)
	}

	o.Status(fmt.Sprintf("stalling disk on node %d via cgroup", nid))
	if err := failer.Inject(ctx, o.L(), args); err != nil {
		o.Fatal(err)
	}

	return cleanup
}

func registerDiskStall(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "disk-stall/dmsetup",
		Owner:              registry.OwnerStorage,
		Timeout:            10 * time.Minute,
		CompatibleClouds:   registry.OnlyGCE,
		CanRunConcurrently: registry.OperationCannotRunConcurrentlyWithItself,
		Dependencies: []registry.OperationDependency{
			registry.OperationRequiresZeroUnderreplicatedRanges,
		},
		Run: runDmsetupDiskStall,
	})
	r.AddOperation(registry.OperationSpec{
		Name:               "disk-stall/cgroup",
		Owner:              registry.OwnerStorage,
		Timeout:            10 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCannotRunConcurrentlyWithItself,
		Dependencies: []registry.OperationDependency{
			registry.OperationRequiresZeroUnderreplicatedRanges,
		},
		Run: runCgroupDiskStall,
	})
}
