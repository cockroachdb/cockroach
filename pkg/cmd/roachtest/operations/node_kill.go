// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

type cleanupNodeKill struct {
	nodes option.NodeListOption
}

func (cl *cleanupNodeKill) Cleanup(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	// We might need to restart the node if it isn't live.
	db, err := c.ConnE(ctx, o.L(), cl.nodes[0])
	if err != nil {
		err = c.RunE(ctx, option.WithNodes(cl.nodes), "./cockroach.sh")
		if err != nil {
			o.Status(fmt.Sprintf("restarted node with error %s", err))
		} else {
			o.Status("restarted node with no error")
		}
		return
	}
	defer db.Close()
	_, err = db.Query("SELECT 1")
	if err != nil {
		err = c.RunE(ctx, option.WithNodes(cl.nodes), "./cockroach.sh")
		if err != nil {
			o.Status(fmt.Sprintf("restarted node with error %s", err))
		} else {
			o.Status("restarted node with no error")
		}
	}
}

func nodeKillRunner(
	signal int, drain bool,
) func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
	return func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
		return runNodeKill(ctx, o, c, signal, drain)
	}
}

func runNodeKill(
	ctx context.Context, o operation.Operation, c cluster.Cluster, signal int, drain bool,
) registry.OperationCleanup {
	rng, _ := randutil.NewPseudoRand()
	node := c.All().SeededRandNode(rng)

	if drain {
		drainNode(ctx, o, c, node)
	}

	o.Status(fmt.Sprintf("killing node %s with signal %d", node.NodeIDsString(), signal))
	err := c.RunE(ctx, option.WithNodes(node), "pkill", fmt.Sprintf("-%d", signal), "-f", "cockroach\\ start")
	if err != nil {
		o.Fatal(err)
	}

	o.Status(fmt.Sprintf("sent signal %d to node %s, waiting for process to exit", signal, node.NodeIDsString()))
	for {
		if err := ctx.Err(); err != nil {
			o.Fatal(err)
		}
		err := c.RunE(ctx, option.WithNodes(node), "pgrep", "-f", "cockroach\\ start")
		if err != nil {
			if strings.Contains(err.Error(), "status 1") {
				// pgrep returns error code 1 if no processes are found.
				o.Status(fmt.Sprintf("killed node %s with signal %d", node.NodeIDsString(), signal))
				break
			}
			o.Fatal(err)
		}
		time.Sleep(1 * time.Second)
	}

	return &cleanupNodeKill{
		nodes: node,
	}
}

func registerNodeKill(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "node-kill/sigkill/drain=true",
		Owner:              registry.OwnerServer,
		Timeout:            15 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCannotRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresZeroUnderreplicatedRanges},
		Run:                nodeKillRunner(9 /* signal */, true /* drain */),
	})
	r.AddOperation(registry.OperationSpec{
		Name:               "node-kill/sigkill/drain=false",
		Owner:              registry.OwnerServer,
		Timeout:            10 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCannotRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresZeroUnderreplicatedRanges},
		Run:                nodeKillRunner(9 /* signal */, false /* drain */),
	})
	r.AddOperation(registry.OperationSpec{
		Name:               "node-kill/sigterm/drain=true",
		Owner:              registry.OwnerServer,
		Timeout:            15 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCannotRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresZeroUnderreplicatedRanges},
		Run:                nodeKillRunner(15 /* signal */, true /* drain */),
	})
	r.AddOperation(registry.OperationSpec{
		Name:               "node-kill/sigterm/drain=false",
		Owner:              registry.OwnerServer,
		Timeout:            10 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCannotRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresZeroUnderreplicatedRanges},
		Run:                nodeKillRunner(15 /* signal */, false /* drain */),
	})
}
