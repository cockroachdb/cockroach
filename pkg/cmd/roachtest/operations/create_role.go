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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

type cleanupCreatedRole struct {
	role string
}

func (cl *cleanupCreatedRole) Cleanup(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	o.Status(fmt.Sprintf("dropping role %s", cl.role))
	_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP ROLE IF EXISTS %s", cl.role))
	if err != nil {
		o.Fatal(err)
	}
}

func runCreateRole(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer conn.Close()

	rng, _ := randutil.NewPseudoRand()
	roleName := fmt.Sprintf("roachtest_role_%d", rng.Uint32())

	o.Status(fmt.Sprintf("creating role %s", roleName))
	_, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE ROLE %s", roleName))
	if err != nil {
		o.Fatal(err)
	}

	o.Status(fmt.Sprintf("role %s created", roleName))

	return &cleanupCreatedRole{
		role: roleName,
	}
}

func registerCreateRole(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "create-role",
		Owner:              registry.OwnerSQLFoundations,
		Timeout:            1 * time.Hour,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresPopulatedDatabase},
		Run:                runCreateRole,
	})
}
