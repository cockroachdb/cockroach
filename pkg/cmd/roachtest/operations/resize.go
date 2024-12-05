// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"context"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/errors"
)

type cleanupResize struct {
	origClusterSize int
	growCount       int
}

// Cleanup shrinks the cluster back to its original size.
func (cl *cleanupResize) Cleanup(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	dynamicCluster := c.(cluster.DynamicCluster)
	defer func() {
		err := dynamicCluster.Shrink(ctx, o.L(), cl.growCount)
		if err != nil {
			o.Status(fmt.Sprintf("error shrinking cluster: %s", err))
		} else {
			o.Status("shrunk cluster back to original size")
		}
	}()
	for i := 0; i < cl.growCount; i++ {
		drainNode(ctx, o, c, c.Node(cl.origClusterSize+i+1))
		decommissionNode(ctx, o, c, c.Node(cl.origClusterSize+i+1))
	}
}

// resizeCluster grows the cluster by the specified count, waits for a specified
// duration, then drains and decommissions the new nodes.
func resizeCluster(
	ctx context.Context, o operation.Operation, c cluster.Cluster, growCount int,
) *cleanupResize {
	// Create a temporary directory to store the required files for this operation.
	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		o.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()
	dynamicCluster, ok := c.(cluster.DynamicCluster)
	if !ok {
		o.Fatal("cluster does not implement the DynamicCluster interface")
	}

	// Grab required files from the first node.
	err = c.Get(ctx, o.L(), "cockroach", path.Join(tmpDir, "cockroach"), c.Node(1))
	if err != nil {
		o.Fatal(errors.Wrap(err, "failed to get cockroach, please ensure cockroach is running on this cluster"))
	}
	err = c.Get(ctx, o.L(), "lib", path.Join(tmpDir, "lib"), c.Node(1))
	if err != nil {
		o.Fatal(errors.Wrap(err, "failed to get cockroach libs"))
	}

	// Grow the cluster, but keep track of the original cluster size.
	origClusterSize := c.Spec().NodeCount
	err = dynamicCluster.Grow(ctx, o.L(), growCount)
	if err != nil {
		o.Fatal(err)
	}
	// Grow command generate new certificates, update certificate on workload cluster.
	if wc := o.WorkloadCluster(); wc != nil {
		_ = c.Get(ctx, o.L(), "certs", path.Join(tmpDir, "certs"), c.Node(1))
		wc.Put(ctx, path.Join(tmpDir, "certs"), "./", wc.All())
	}
	newNodes := c.Range(origClusterSize+1, origClusterSize+growCount)

	// Copy the required files to the new nodes.
	c.Put(ctx, path.Join(tmpDir, "cockroach"), "cockroach", newNodes)
	c.Put(ctx, path.Join(tmpDir, "lib"), "lib", newNodes)

	// Start the new nodes.
	startOpts := o.StartOpts()
	startOpts.RoachprodOpts.IsRestart = false
	c.Start(ctx, o.L(), startOpts, o.ClusterSettings(), newNodes)

	return &cleanupResize{growCount: growCount, origClusterSize: origClusterSize}
}

func registerResize(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "resize/grow=3",
		Owner:              registry.OwnerStorage,
		Timeout:            30 * time.Minute,
		CompatibleClouds:   registry.OnlyGCE,
		CanRunConcurrently: registry.OperationCannotRunConcurrentlyWithItself,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresNodes},
		Run: func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
			return resizeCluster(ctx, o, c, 3)
		},
	})
}
