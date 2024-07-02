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
	"os"
	"path"
	"regexp"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/errors"
)

var advertiseAddrRegex = regexp.MustCompile("(?m)^--advertise-addr=.*:")
var roachprodRegex = regexp.MustCompile("(?m)^ROACHPROD=1$")
var nodeIDRegex = regexp.MustCompile("node[0-9]+")

func getStartScript(ctx context.Context, c cluster.Cluster) (string, error) {
	data, err := c.GetString(ctx, "cockroach.sh", c.Node(1))
	if err != nil {
		return "", err
	}
	return data[0], nil
}

// transformStartScript takes a start script and returns a new copy that
// replaces the listen IP and node specific arguments with a new IP and node ID.
// This is required because the operation has no knowledge of the original start
// options and the best we can currently do is to copy it from the first node
// and modify the relevant parts for the new node.
func transformStartScript(startScript, newIP string, newNodeID int) string {
	newLine := fmt.Sprintf("--listen-addr=%s:", newIP)
	output := advertiseAddrRegex.ReplaceAllString(startScript, newLine)
	output = roachprodRegex.ReplaceAllString(output, fmt.Sprintf("ROACHPROD=%d", newNodeID))
	return nodeIDRegex.ReplaceAllString(output, fmt.Sprintf("node%d", newNodeID))
}

type cleanupResize struct {
	growCount int
}

// Cleanup shrinks the cluster back to its original size.
func (cl *cleanupResize) Cleanup(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	err := c.Shrink(ctx, o.L(), cl.growCount)
	if err != nil {
		o.Status(fmt.Sprintf("error shrinking cluster %s", err))
	} else {
		o.Status("shrunk cluster back to original size")
	}
}

// resizeCluster grows the cluster by the specified count, waits for a specified
// duration, then drains and decommissions the new nodes.
func resizeCluster(
	ctx context.Context,
	o operation.Operation,
	c cluster.Cluster,
	growCount int,
	pauseDuration time.Duration,
) *cleanupResize {
	// Create a temporary directory to store the required files for this operation.
	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		o.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Grab required files from the first node.
	startScript, err := getStartScript(ctx, c)
	if err != nil {
		o.Fatal(err)
	}
	err = c.Get(ctx, o.L(), "cockroach", path.Join(tmpDir, "cockroach"), c.Node(1))
	if err != nil {
		o.Fatal(errors.Wrap(err, "failed to get cockroach, please ensure cockroach is running on this cluster."))
	}
	err = c.Get(ctx, o.L(), "lib", path.Join(tmpDir, "lib"), c.Node(1))
	if err != nil {
		o.Fatal(errors.Wrap(err, "failed to get cockroach libs."))
	}

	// Grow the cluster, but keep track of the original cluster size.
	origClusterSize := c.Spec().NodeCount
	err = c.Grow(ctx, o.L(), growCount)
	if err != nil {
		o.Fatal(err)
	}
	newNodes := c.Range(origClusterSize+1, origClusterSize+growCount)

	// Copy the required files to the new nodes.
	c.Put(ctx, path.Join(tmpDir, "cockroach"), "cockroach", newNodes)
	c.Put(ctx, path.Join(tmpDir, "lib"), "lib", newNodes)
	newIPs, err := c.InternalIP(ctx, o.L(), newNodes)
	if err != nil {
		o.Fatal(err)
	}
	for i := 0; i < growCount; i++ {
		newStartScript := transformStartScript(startScript, newIPs[i], origClusterSize+i+1)
		err = c.PutString(ctx, newStartScript, "cockroach.sh", 0700, c.Node(origClusterSize+i+1))
		if err != nil {
			o.Fatal(err)
		}
	}

	// Start the new nodes.
	c.Run(ctx, option.WithNodes(newNodes), "./cockroach.sh")

	// Wait for a specified duration before draining and decommissioning the nodes.
	time.Sleep(pauseDuration)
	for i := 0; i < growCount; i++ {
		drainNode(ctx, o, c, c.Node(1), c.Node(origClusterSize+i+1))
		decommissionNode(ctx, o, c, c.Node(1), c.Node(origClusterSize+i+1))
	}
	return &cleanupResize{growCount: growCount}
}

func registerResize(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:             "resize/grow=3/wait=20m",
		Owner:            registry.OwnerStorage,
		Timeout:          30 * time.Minute,
		CompatibleClouds: registry.OnlyGCE,
		Dependencies:     []registry.OperationDependency{registry.OperationRequiresNodes},
		Run: func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
			return resizeCluster(ctx, o, c, 3, 20*time.Minute)
		},
	})
}
