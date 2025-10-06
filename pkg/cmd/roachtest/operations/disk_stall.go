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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

type cleanupDiskStall struct {
	nodes   option.NodeListOption
	staller roachtestutil.DiskStaller
}

func (cl *cleanupDiskStall) Cleanup(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	cl.staller.Unstall(ctx, cl.nodes)
	o.Status("unstalled nodes; waiting 10 seconds before restarting")
	time.Sleep(10 * time.Second)
	// We might need to restart the node if it isn't live.
	db, err := c.ConnE(ctx, o.L(), cl.nodes[0])
	if err != nil {
		c.Run(ctx, option.WithNodes(cl.nodes), "./cockroach.sh")
		return
	}
	defer db.Close()
	_, err = db.Query("SELECT 1")
	if err != nil {
		c.Run(ctx, option.WithNodes(cl.nodes), "./cockroach.sh")
	}
}

func runDiskStall(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	rng, _ := randutil.NewPseudoRand()

	nodes := c.All()
	nid := nodes[rng.Intn(len(nodes))]
	ds := roachtestutil.MakeDmsetupDiskStaller(o, c)

	o.Status(fmt.Sprintf("stalling disk on node %d", nid))
	ds.Stall(ctx, c.Node(nid))

	return &cleanupDiskStall{
		nodes:   c.Node(nid),
		staller: ds,
	}
}

func registerDiskStall(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "disk-stall/dmsetup",
		Owner:              registry.OwnerStorage,
		Timeout:            10 * time.Minute,
		CompatibleClouds:   registry.OnlyGCE,
		CanRunConcurrently: registry.OperationCannotRunConcurrentlyWithItself,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresZeroUnderreplicatedRanges},
		Run:                runDiskStall,
	})
}
