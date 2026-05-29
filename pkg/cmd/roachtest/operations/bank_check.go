// Copyright 2026 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
)

// bankDBWhitelist lists the database names the bank workload may be running
// under. The operation checks each one and uses the first match.
var bankDBWhitelist = []string{"cct_bank", "bank"}

// runBankCheck verifies the bank workload's consistency by calling the
// bank workload's CheckConsistency hook directly. The check validates
// that the sum of all balances is zero and that no rows have been
// inserted or deleted.
func runBankCheck(
	ctx context.Context, o operation.Operation, c cluster.Cluster,
) registry.OperationCleanup {
	rng, _ := randutil.NewPseudoRand()
	nodes := c.All()
	node := nodes[rng.Intn(len(nodes))]

	o.Status(fmt.Sprintf(
		"connecting to node %d for bank consistency check", node,
	))
	conn := c.Conn(
		ctx, o.L(), node,
		option.VirtualClusterName(roachtestflags.VirtualCluster),
	)
	defer conn.Close()

	// Discover the bank database.
	var dbName string
	rows, err := conn.QueryContext(
		ctx, "SELECT database_name FROM [SHOW DATABASES]",
	)
	if err != nil {
		o.Fatal(err)
	}
	defer rows.Close()
outer:
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			o.Fatal(err)
		}
		for _, candidate := range bankDBWhitelist {
			if name == candidate {
				dbName = name
				break outer
			}
		}
	}
	if dbName == "" {
		o.Status("no bank database found, skipping check")
		return nil
	}
	o.Status(fmt.Sprintf(
		"running bank consistency check on database %s", dbName,
	))

	// Switch to the bank database so CheckConsistency's SHOW TABLES
	// discovers the right tables.
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("USE %s", dbName)); err != nil {
		o.Fatal(err)
	}

	// The row count passed to FromRows is irrelevant; we only need
	// the generator to access the CheckConsistency hook.
	gen := bank.FromRows(2 /* rows */)
	hooks := gen.(workload.Hookser).Hooks()
	if err := hooks.CheckConsistency(ctx, conn); err != nil {
		o.Fatal(err)
	}
	o.Status("bank consistency check passed")

	return nil
}

func registerBankCheck(r registry.Registry) {
	r.AddOperation(registry.OperationSpec{
		Name:               "bank-consistency-check",
		Owner:              registry.OwnerTestEng,
		Timeout:            10 * time.Minute,
		CompatibleClouds:   registry.AllClouds,
		CanRunConcurrently: registry.OperationCanRunConcurrently,
		Dependencies:       []registry.OperationDependency{registry.OperationRequiresNodes},
		Run:                runBankCheck,
	})
}
