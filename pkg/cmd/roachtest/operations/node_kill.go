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
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
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
		o.Status(fmt.Sprintf("restarted node with error %s", err))
		return
	}
	defer db.Close()
	_, err = db.Query("SELECT 1")
	if err != nil {
		err = c.RunE(ctx, option.WithNodes(cl.nodes), "./cockroach.sh")
		o.Status(fmt.Sprintf("restarted node with error %s", err))
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
		o.Status(fmt.Sprintf("draining node %s", node.NodeIDsString()))

		url, err := c.ExternalPGUrl(ctx, o.L(), node, roachprod.PGURLOptions{
			External: false,
			Secure:   c.IsSecure(),
		})
		if err != nil {
			o.Fatal(err)
		}
		err = c.RunE(ctx, option.WithNodes(node),
			o.ClusterCockroach(), "node", "drain", "--logtostderr=INFO",
			fmt.Sprintf("--url=%s", url[0]), "--self",
		)
		if err != nil {
			o.Fatal(err)
		}
	}
	o.Status(fmt.Sprintf("killing node %s with signal %d", node.NodeIDsString(), signal))

	stopOpts := option.StopVirtualClusterOpts(install.SystemInterfaceName, node)
	stopOpts.RoachprodOpts.Sig = signal
	stopOpts.RoachprodOpts.Wait = true
	stopOpts.RoachprodOpts.MaxWait = 300 // 5 minutes
	c.StopServiceForVirtualCluster(ctx, o.L(), stopOpts)
	o.Status(fmt.Sprintf("killed node %s with signal %d", node.NodeIDsString(), signal))

	return &cleanupNodeKill{
		nodes: node,
	}
}

func registerNodeKill(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:             "node-kill/sigkill/drain=true",
		Owner:            registry.OwnerServer,
		Timeout:          15 * time.Minute,
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
		Timeout:          15 * time.Minute,
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
