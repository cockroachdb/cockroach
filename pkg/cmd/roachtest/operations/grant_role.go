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

// This is a schema change operation that does the following:
// Operation Run:
// 1. Create two roles with randomly generated names.
// 2. GRANT one role to the other role.
// Operation Cleanup:
// 1. REVOKE the granted role.
// 2. Drop both roles.

type revokeRole struct {
	grantedRole, granteeRole string
}

func (cl *revokeRole) Cleanup(ctx context.Context, o operation.Operation, c cluster.Cluster) {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer func() { _ = conn.Close() }()

	o.Status(fmt.Sprintf("Revoking role %s from role %s", cl.grantedRole, cl.granteeRole))
	_, err := conn.ExecContext(ctx, fmt.Sprintf("REVOKE %s FROM %s", cl.grantedRole, cl.granteeRole))
	if err != nil {
		o.Fatal(err)
	}
	o.Status(fmt.Sprintf("Revoked role %s from role %s", cl.grantedRole, cl.granteeRole))

	// Drop both roles
	for _, role := range []string{cl.granteeRole, cl.grantedRole} {
		o.Status(fmt.Sprintf("Dropping role %s", role))
		_, err = conn.ExecContext(ctx, fmt.Sprintf("DROP ROLE %s", role))
		if err != nil {
			o.Fatal(err)
		}
		o.Status(fmt.Sprintf("Dropped role %s", role))
	}
}

func runGrantRole(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	conn := c.Conn(ctx, o.L(), 1, option.VirtualClusterName(roachtestflags.VirtualCluster))
	defer func() { _ = conn.Close() }()

	rng, _ := randutil.NewTestRand()
	// Create random role names with a prefix to avoid conflicts.
	grantedRole := fmt.Sprintf("roachprod_ops_granted_role_%s", randutil.RandString(rng, 10, randutil.PrintableKeyAlphabet))
	granteeRole := fmt.Sprintf("roachprod_ops_grantee_role_%s", randutil.RandString(rng, 10, randutil.PrintableKeyAlphabet))

	// Create both roles.
	for _, role := range []string{grantedRole, granteeRole} {
		o.Status(fmt.Sprintf("Creating role %s", role))
		_, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE ROLE %s", role))
		if err != nil {
			o.Fatal(err)
		}
		o.Status(fmt.Sprintf("Created role %s", role))
	}

	// Grant one role to the other.
	o.Status(fmt.Sprintf("Granting role %s to role %s", grantedRole, granteeRole))
	_, err := conn.ExecContext(ctx, fmt.Sprintf("GRANT %s TO %s", grantedRole, granteeRole))
	if err != nil {
		o.Fatal(err)
	}
	o.Status(fmt.Sprintf("Granted role %s to role %s", grantedRole, granteeRole))

	return &revokeRole{
		grantedRole: grantedRole,
		granteeRole: granteeRole,
	}
}

func registerGrantRole(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "grant-role",
		Owner:              registry.OwnerSQLFoundations,
		Timeout:            10 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresNodes},
		Run:                runGrantRole,
		WaitBeforeCleanup:  1 * time.Hour,
	})
}
