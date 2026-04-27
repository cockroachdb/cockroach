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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations/helpers"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/roachprod/failureinjection/failures"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

//lint:ignore U1000 temporarily disabled
type cleanupNodeKill struct {
	failer *failures.Failer
}

//lint:ignore U1000 temporarily disabled
func (cl *cleanupNodeKill) Cleanup(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	o.Status("recovering killed node")
	if err := cl.failer.Recover(ctx, o.L()); err != nil {
		o.L().Printf("failed to recover node: %v", err)
	}
	o.Status("waiting for node to stabilize")
	if err := cl.failer.WaitForFailureToRecover(ctx, o.L()); err != nil {
		o.L().Printf("node failed to stabilize: %v", err)
	}
	if err := cl.failer.Cleanup(ctx, o.L()); err != nil {
		o.Fatalf("failed to cleanup: %v", err)
	}
}

//lint:ignore U1000 temporarily disabled
func nodeKillRunner(
	graceful bool, drain bool, gracePeriod time.Duration,
) func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
	return func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
		return runNodeKill(ctx, o, c, graceful, drain, gracePeriod)
	}
}

//lint:ignore U1000 temporarily disabled
func runNodeKill(
	ctx context.Context,
	o operation.Operation,
	c cluster.Cluster,
	graceful bool,
	drain bool,
	gracePeriod time.Duration,
) registry.OperationCleanup {
	rng, _ := randutil.NewPseudoRand()
	node := c.All().SeededRandNode(rng)

	if drain {
		helpers.DrainNode(ctx, o, c, node)
	}

	failer, args, err := roachtestutil.MakeProcessKillFailer(
		o.L(), c, node, graceful, gracePeriod,
	)
	if err != nil {
		o.Fatal(err)
	}

	// Assign cleanup handler before Setup so partial failures are cleaned up.
	cleanup := &cleanupNodeKill{failer: failer}

	if err := failer.Setup(ctx, o.L(), args); err != nil {
		o.Fatal(err)
	}

	o.Status(fmt.Sprintf("killing node %s (graceful=%t)", node.NodeIDsString(), graceful))
	if err := failer.Inject(ctx, o.L(), args); err != nil {
		o.Fatal(err)
	}
	if err := failer.WaitForFailureToPropagate(ctx, o.L()); err != nil {
		o.Fatal(err)
	}
	o.Status(fmt.Sprintf("killed node %s", node.NodeIDsString()))

	return cleanup
}

//lint:ignore U1000 temporarily disabled
func registerNodeKill(r registry.Registry) {
	for _, spec := range []struct {
		name        string
		graceful    bool
		drain       bool
		gracePeriod time.Duration
		downtime    time.Duration
		timeout     time.Duration
	}{
		// SIGKILL + drain: drain first, then hard kill.
		{
			name:     "node-kill/sigkill/drain=true/downtime=10m",
			graceful: false, drain: true, gracePeriod: 0,
			downtime: 10 * time.Minute, timeout: 35 * time.Minute,
		},
		// SIGKILL, no drain.
		{
			name:     "node-kill/sigkill/drain=false/downtime=10m",
			graceful: false, drain: false, gracePeriod: 0,
			downtime: 10 * time.Minute, timeout: 35 * time.Minute,
		},
		// SIGTERM + drain: drain first, then graceful shutdown.
		{
			name:     "node-kill/sigterm/drain=true/downtime=10m",
			graceful: true, drain: true, gracePeriod: 5 * time.Minute,
			downtime: 10 * time.Minute, timeout: 35 * time.Minute,
		},
		// SIGTERM, no drain.
		{
			name:     "node-kill/sigterm/drain=false/downtime=10m",
			graceful: true, drain: false, gracePeriod: 5 * time.Minute,
			downtime: 10 * time.Minute, timeout: 35 * time.Minute,
		},
	} {
		s := spec
		r.AddOperation(registry.OperationSpec{
			Name:               s.name,
			Owner:              registry.OwnerServer,
			Timeout:            s.timeout,
			CompatibleClouds:   registry.AllClouds,
			CanRunConcurrently: registry.OperationCannotRunConcurrently,
			Dependencies: []registry.OperationDependency{
				registry.OperationRequiresZeroUnderreplicatedRanges,
			},
			WaitBeforeCleanup: s.downtime,
			Run:               nodeKillRunner(s.graceful, s.drain, s.gracePeriod),
		})
	}
}
