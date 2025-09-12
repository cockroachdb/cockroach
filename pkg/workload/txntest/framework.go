// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txntest

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
)

// SchemaSpec defines DDL and optional initialization logic for a triad.
type SchemaSpec struct {
	// DDL statements to run (in order) before the workload.
	DDL []string
	// Init seeds initial data for the schema.
	Init func(ctx context.Context, db *gosql.DB) error
	// Database is the database name to use. If empty, defaults to "defaultdb".
	Database string
}

// WorkloadSpec is the vertically-defined workload spec for a triad.
type WorkloadSpec struct {
	Templates []TxnTemplate
}

// RunConfig controls concurrency and iteration counts.
type RunConfig struct {
	Concurrency int
	// Iterations, if <= 0, defaults to 1.
	Iterations int
}

// Invariant defines a post-run check.
type Invariant struct {
	Name string
	Fn   func(ctx context.Context, db *gosql.DB, h History) error
}

// TestSpec vertically integrates schema, workload, and invariants.
type TestSpec struct {
	Schema     SchemaSpec
	Workload   WorkloadSpec
	Invariants []Invariant
	RunConfig  RunConfig
}

// Run runs a triad on an in-process testcluster with the requested number of nodes.
func Run(ctx context.Context, t testing.TB, spec TestSpec, nodes int) error {
	if spec.RunConfig.Concurrency <= 0 {
		spec.RunConfig.Concurrency = 1
	}
	if spec.RunConfig.Iterations <= 0 {
		spec.RunConfig.Iterations = 1
	}

	dbName := spec.Schema.Database
	if dbName == "" {
		dbName = defaultDatabase
	}

	if nodes <= 0 {
		nodes = 1
	}
	// Start an in-process test cluster.
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())

	// Open a SQL connection to node 0 using the requested database.
	if dbName == "" {
		dbName = catalogkeys.DefaultDatabaseName
	}
	db := tc.Server(0).SQLConn(t, serverutils.DBName(dbName))

	// Collect URLs for each node to run workloads via pgx.
	urls := make([]string, 0, tc.NumServers())
	for i := 0; i < tc.NumServers(); i++ {
		u, _ := tc.Server(i).PGUrl(t, serverutils.DBName(dbName))
		urls = append(urls, u.String())
	}

	// Ensure database is selected before running DDLs.
	if _, err := db.ExecContext(ctx, "USE "+dbName); err != nil {
		return err
	}
	for _, stmt := range spec.Schema.DDL {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	if spec.Schema.Init != nil {
		if err := spec.Schema.Init(ctx, db); err != nil {
			return err
		}
	}

	// Run the workload.
	hist := NewHistory()
	if err := executeWorkload(ctx, urls, spec.Workload, spec.RunConfig, hist); err != nil {
		return err
	}

	// Run invariants.
	for _, inv := range spec.Invariants {
		if inv.Fn == nil {
			continue
		}
		if err := inv.Fn(ctx, db, hist); err != nil {
			return err
		}
	}
	return nil
}

// defaultDatabase is used when SchemaSpec.Database is empty.
const defaultDatabase = "defaultdb"
