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
	"github.com/cockroachdb/cockroach/pkg/roachprod/failureinjection/failures"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

type cleanupNetworkPartition struct {
	failer *failures.Failer
}

// Cleanup removes the network partition created by the operation.
func (np *cleanupNetworkPartition) Cleanup(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) {
	o.Status("removing network partition")
	if err := np.failer.Recover(ctx, o.L()); err != nil {
		o.Fatalf("failed to recover from network partition: %v", err)
	}
	if err := np.failer.Cleanup(ctx, o.L()); err != nil {
		o.Fatalf("failed to cleanup network partition: %v", err)
	}
}

// createNetworkPartialPartition creates a bidirectional network partition
// between two random nodes.
func createNetworkPartialPartition(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) (cleanup registry.OperationCleanup) {
	defer func() {
		if r := recover(); r != nil {
			o.Errorf("error during network partition: %v", r)
		}
	}()
	nodeCount := c.Spec().NodeCount
	if nodeCount <= 1 {
		o.Fatal("not enough nodes to create a partition")
	}

	rng, _ := randutil.NewPseudoRand()
	nodes := c.All()
	nodeID := nodes[rng.Intn(len(nodes))]

	// Choose a different node to partition from.
	var otherNodeID int
	for {
		otherNodeID = nodes[rng.Intn(len(nodes))]
		if otherNodeID != nodeID {
			break
		}
	}

	o.Status(fmt.Sprintf(
		"creating a partition between nodes n%d and n%d", nodeID, otherNodeID,
	))

	failer, args, err := roachtestutil.MakeBidirectionalPartitionFailer(
		o.L(), c, c.Node(nodeID), c.Node(otherNodeID),
	)
	if err != nil {
		o.Fatal(err)
	}
	if err := failer.Setup(ctx, o.L(), args); err != nil {
		o.Fatal(err)
	}

	// Assign cleanup handler before injecting the failure.
	cleanup = &cleanupNetworkPartition{failer: failer}

	if err := failer.Inject(ctx, o.L(), args); err != nil {
		o.Fatal(err)
	}

	return cleanup
}

// createNetworkFullPartition creates a bidirectional network partition between
// a random node and all other nodes in the cluster.
func createNetworkFullPartition(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) (cleanup registry.OperationCleanup) {
	defer func() {
		if r := recover(); r != nil {
			o.Errorf("error during network partition: %v", r)
		}
	}()
	nodeCount := c.Spec().NodeCount
	if nodeCount <= 1 {
		o.Fatal("not enough nodes to create a partition")
	}

	rng, _ := randutil.NewPseudoRand()
	nodes := c.All()
	nodeID := nodes[rng.Intn(len(nodes))]

	// Build the list of all other nodes.
	var otherNodes option.NodeListOption
	for _, n := range nodes {
		if n != nodeID {
			otherNodes = append(otherNodes, n)
		}
	}

	o.Status(fmt.Sprintf("partition node n%d from the cluster", nodeID))

	failer, args, err := roachtestutil.MakeBidirectionalPartitionFailer(
		o.L(), c, c.Node(nodeID), otherNodes,
	)
	if err != nil {
		o.Fatal(err)
	}
	if err := failer.Setup(ctx, o.L(), args); err != nil {
		o.Fatal(err)
	}

	// Assign cleanup handler before injecting the failure.
	cleanup = &cleanupNetworkPartition{failer: failer}

	if err := failer.Inject(ctx, o.L(), args); err != nil {
		o.Fatal(err)
	}

	return cleanup
}

// registerNetworkPartition registers both the full and partial network
// partition operations.
func registerNetworkPartition(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "network-partition/full",
		Owner:              registry.OwnerKV,
		Timeout:            1 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCannotRunConcurrently,
		Dependencies: []registry.OperationDependency{
			registry.OperationRequiresZeroUnderreplicatedRanges,
		},
		Run: createNetworkFullPartition,
	})
	r.AddOperation(registry.OperationSpec{
		Name:               "network-partition/partial",
		Owner:              registry.OwnerKV,
		Timeout:            1 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCannotRunConcurrently,
		Dependencies: []registry.OperationDependency{
			registry.OperationRequiresZeroUnderreplicatedRanges,
		},
		Run: createNetworkPartialPartition,
	})
}
