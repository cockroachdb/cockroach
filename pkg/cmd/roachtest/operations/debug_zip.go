// Copyright 2025 The Cockroach Authors.
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

func debugZipRunner() func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
	return func(ctx context.Context, o operation.Operation, c cluster.Cluster) registry.OperationCleanup {
		return runDebugZip(ctx, o, c)
	}
}

func runDebugZip(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	rng, _ := randutil.NewPseudoRand()
	node := c.All().SeededRandNode(rng)

	addr, err := c.InternalAddr(ctx, o.L(), node)
	if err != nil {
		o.Fatal(err)
	}

	// NOTE(seanc@): we don't actually want the payload from the debug zip, we
	// just want to run the debug zip command to observe its impact. We write to
	// a temporary file because /dev/null causes a "zip: not a valid zip file"
	// error when the command validates the output.
	zipFile := fmt.Sprintf("/tmp/debug-zip-%d.zip", rng.Uint32())
	debugZipCmd := roachtestutil.NewCommand("./%s debug zip %s", o.ClusterCockroach(), zipFile).
		WithEqualsSyntax().
		Flag("host", addr[0]).
		Flag("logtostderr", "INFO").
		MaybeFlag(c.IsSecure(), "certs-dir", "certs").
		MaybeOption(!c.IsSecure(), "insecure")

	o.Status(fmt.Sprintf("running %q on node %s", debugZipCmd.String(), node.NodeIDsString()))
	err = c.RunE(ctx, option.WithNodes(node), debugZipCmd.String())
	// Clean up the temporary zip file regardless of success or failure.
	_ = c.RunE(ctx, option.WithNodes(node), fmt.Sprintf("rm -f %s", zipFile))
	if err != nil {
		o.Fatal(err)
	}

	return nil
}

func registerDebugZip(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "debug-zip/default",
		Owner:              registry.OwnerServer,
		Timeout:            60 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:                debugZipRunner(),
	})
}
