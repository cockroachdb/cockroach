// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package operations

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

type cleanupNodeKill struct {
	nodes option.NodeListOption
}

func (cl *cleanupNodeKill) Cleanup(ctx context.Context, o operation.Operation, c cluster.Cluster) {
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

func nodeKillRunner(signal int, drain bool) func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
	return func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
		return runNodeKill(ctx, o, c, signal, drain)
	}
}

func runNodeKill(
	ctx context.Context, o operation.Operation, c cluster.Cluster, signal int, drain bool,
) registry.OperationCleanup {
	rng, _ := randutil.NewPseudoRand()

	nodes := c.All()
	nid := nodes[rng.Intn(len(nodes))]

	if drain {
		o.Status(fmt.Sprintf("draining node %d", nid))

		url, err := c.ExternalPGUrl(ctx, o.L(), c.Node(nid), roachprod.PGURLOptions{
			External: false,
			Secure:   c.IsSecure(),
		})
		if err != nil {
			o.Fatal(err)
		}
		err = c.RunE(ctx, option.WithNodes(c.Node(nid)),
			o.ClusterCockroach(), "node", "drain", "--logtostderr=INFO",
			fmt.Sprintf("--url=%s", url[0]), "--self",
		)
		if err != nil {
			o.Fatal(err)
		}
	}
	o.Status(fmt.Sprintf("killing node %d with signal %d", nid, signal))

	stopOpts := option.DefaultStopOpts()
	stopOpts.RoachprodOpts.Sig = signal
	if err := c.StopE(ctx, o.L(), stopOpts, c.Node(nid)); err != nil {
		o.Fatal(err)
	}

	return &cleanupNodeKill{
		nodes: c.Node(nid),
	}
}

func registerNodeKill(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:             "node-kill/sigkill/drain=true",
		Owner:            registry.OwnerServer,
		Timeout:          10 * time.Minute,
		CompatibleClouds: registry.AllClouds,
		Dependencies:     []registry.OperationDependency{registry.OperationRequiresZeroUnderreplicatedRanges},
		Run:              nodeKillRunner(9 /* signal */, true /* drain */),
	})
	r.AddOperation(registry.OperationSpec{
		Name:             "node-kill/sigkill/drain=false",
		Owner:            registry.OwnerServer,
		Timeout:          10 * time.Minute,
		CompatibleClouds: registry.AllClouds,
		Dependencies:     []registry.OperationDependency{registry.OperationRequiresZeroUnderreplicatedRanges},
		Run:              nodeKillRunner(9 /* signal */, false /* drain */),
	})
	r.AddOperation(registry.OperationSpec{
		Name:             "node-kill/sigterm/drain=true",
		Owner:            registry.OwnerServer,
		Timeout:          10 * time.Minute,
		CompatibleClouds: registry.AllClouds,
		Dependencies:     []registry.OperationDependency{registry.OperationRequiresZeroUnderreplicatedRanges},
		Run:              nodeKillRunner(15 /* signal */, true /* drain */),
	})
	r.AddOperation(registry.OperationSpec{
		Name:             "node-kill/sigterm/drain=false",
		Owner:            registry.OwnerServer,
		Timeout:          10 * time.Minute,
		CompatibleClouds: registry.AllClouds,
		Dependencies:     []registry.OperationDependency{registry.OperationRequiresZeroUnderreplicatedRanges},
		Run:              nodeKillRunner(15 /* signal */, false /* drain */),
	})
}
