// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

type randomLoadBenchSpec struct {
	Nodes       int
	Ops         int
	Concurrency int
}

func registerSchemaChangeRandomLoad(r registry.Registry) {
	geoZones := []string{"us-east1-b", "us-west1-b", "europe-west2-b"}
	if r.MakeClusterSpec(1).Cloud == spec.AWS {
		geoZones = []string{"us-east-2b", "us-west-1a", "eu-west-1a"}
	}
	geoZonesStr := strings.Join(geoZones, ",")
	r.Add(registry.TestSpec{
		Name:  "schemachange/random-load",
		Owner: registry.OwnerSQLSchema,
		Cluster: r.MakeClusterSpec(
			3,
			spec.Geo(),
			spec.Zones(geoZonesStr),
		),
		// This is set while development is still happening on the workload and we
		// fix (or bypass) minor schema change bugs that are discovered.
		NonReleaseBlocker: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			maxOps := 5000
			concurrency := 20
			if c.IsLocal() {
				maxOps = 200
				concurrency = 2
			}
			runSchemaChangeRandomLoad(ctx, t, c, maxOps, concurrency)
		},
	})

	// Run a few representative scbench specs in CI.
	registerRandomLoadBenchSpec(r, randomLoadBenchSpec{
		Nodes:       3,
		Ops:         2000,
		Concurrency: 1,
	})

	registerRandomLoadBenchSpec(r, randomLoadBenchSpec{
		Nodes:       3,
		Ops:         10000,
		Concurrency: 20,
	})
}

func registerRandomLoadBenchSpec(r registry.Registry, b randomLoadBenchSpec) {
	nameParts := []string{
		"scbench",
		"randomload",
		fmt.Sprintf("nodes=%d", b.Nodes),
		fmt.Sprintf("ops=%d", b.Ops),
		fmt.Sprintf("conc=%d", b.Concurrency),
	}
	name := strings.Join(nameParts, "/")

	r.Add(registry.TestSpec{
		Name:    name,
		Owner:   registry.OwnerSQLSchema,
		Cluster: r.MakeClusterSpec(b.Nodes),
		// This is set while development is still happening on the workload and we
		// fix (or bypass) minor schema change bugs that are discovered.
		NonReleaseBlocker: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runSchemaChangeRandomLoad(ctx, t, c, b.Ops, b.Concurrency)
		},
	})
}

func runSchemaChangeRandomLoad(
	ctx context.Context, t test.Test, c cluster.Cluster, maxOps, concurrency int,
) {
	validate := func(db *gosql.DB) {
		var (
			id           int
			databaseName string
			schemaName   string
			objName      string
			objError     string
		)
		numInvalidObjects := 0
		rows, err := db.QueryContext(ctx, `SELECT id, database_name, schema_name, obj_name, error FROM crdb_internal.invalid_objects`)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			numInvalidObjects++
			if err := rows.Scan(&id, &databaseName, &schemaName, &objName, &objError); err != nil {
				t.Fatal(err)
			}
			t.L().Errorf(
				"invalid object found: id: %d, database_name: %s, schema_name: %s, obj_name: %s, error: %s",
				id, databaseName, schemaName, objName, objError,
			)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if numInvalidObjects > 0 {
			t.Fatalf("found %d invalid objects", numInvalidObjects)
		}
	}

	loadNode := c.Node(1)
	roachNodes := c.Range(1, c.Spec().NodeCount)
	t.Status("copying binaries")
	c.Put(ctx, t.Cockroach(), "./cockroach", roachNodes)
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", loadNode)

	t.Status("starting cockroach nodes")
	c.Start(ctx, roachNodes)
	c.Run(ctx, loadNode, "./workload init schemachange")

	storeDirectory, err := c.RunWithBuffer(ctx, t.L(), c.Node(1), "echo", "-n", "{store-dir}")
	if err != nil {
		t.L().Printf("Failed to retrieve store directory from node 1: %v\n", err.Error())
	}

	runCmd := []string{
		"./workload run schemachange --verbose=1",
		"--tolerate-errors=false",
		// Save the histograms so that they can be reported to https://roachperf.crdb.dev/.
		" --histograms=" + t.PerfArtifactsDir() + "/stats.json",
		fmt.Sprintf("--max-ops %d", maxOps),
		fmt.Sprintf("--concurrency %d", concurrency),
		fmt.Sprintf("--txn-log %s", filepath.Join(string(storeDirectory), "transactions.json")),
	}
	t.Status("running schemachange workload")
	err = c.RunE(ctx, loadNode, runCmd...)
	if err != nil {
		saveArtifacts(ctx, t, c, string(storeDirectory))
		t.Fatal(err)
	}

	// Drop the database to test the correctness of DROP DATABASE CASCADE, which
	// has been a source of schema change bugs (mostly orphaned descriptors) in
	// the past.
	// TODO (lucy): When the workload supports multiple databases and running
	// schema changes on them, we may want to push this into the post-run hook for
	// the workload itself (if we even still want it, considering that the
	// workload itself would be running DROP DATABASE CASCADE).

	db := c.Conn(ctx, 1)
	defer db.Close()

	t.Status("performing validation after workload")
	validate(db)
	t.Status("dropping database")
	_, err = db.ExecContext(ctx, `USE defaultdb; DROP DATABASE schemachange CASCADE;`)
	if err != nil {
		t.Fatal(err)
	}
	t.Status("performing validation after dropping database")
	validate(db)
}

// saveArtifacts saves important test artifacts in the artifacts directory.
func saveArtifacts(ctx context.Context, t test.Test, c cluster.Cluster, storeDirectory string) {
	db := c.Conn(ctx, 1)
	defer db.Close()

	// Save a backup file called schemachange to the store directory.
	_, err := db.Exec("BACKUP DATABASE schemachange to 'nodelocal://1/schemachange'")
	if err != nil {
		t.L().Printf("Failed execute backup command on node 1: %v\n", err.Error())
	}

	remoteBackupFilePath := filepath.Join(storeDirectory, "extern", "schemachange")
	localBackupFilePath := filepath.Join(t.ArtifactsDir(), "backup")
	remoteTransactionsFilePath := filepath.Join(storeDirectory, "transactions.ndjson")
	localTransactionsFilePath := filepath.Join(t.ArtifactsDir(), "transactions.ndjson")

	// Copy the backup from the store directory to the artifacts directory.
	err = c.Get(ctx, t.L(), remoteBackupFilePath, localBackupFilePath, c.Node(1))
	if err != nil {
		t.L().Printf("Failed to copy backup file from node 1 to artifacts directory: %v\n", err.Error())
	}

	// Copy the txn log from the store directory to the artifacts directory.
	err = c.Get(ctx, t.L(), remoteTransactionsFilePath, localTransactionsFilePath, c.Node(1))
	if err != nil {
		t.L().Printf("Failed to copy txn log file from node 1 to artifacts directory: %v\n", err.Error())
	}
}
