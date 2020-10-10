// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
)

type randomLoadBenchSpec struct {
	Nodes       int
	Ops         int
	Concurrency int
}

func registerSchemaChangeRandomLoad(r *testRegistry) {
	r.Add(testSpec{
		Name:       "schemachange/random-load",
		Owner:      OwnerSQLSchema,
		Cluster:    makeClusterSpec(3),
		MinVersion: "v20.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			maxOps := 5000
			concurrency := 20
			if local {
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

func registerRandomLoadBenchSpec(r *testRegistry, b randomLoadBenchSpec) {
	nameParts := []string{
		"scbench",
		"randomload",
		fmt.Sprintf("nodes=%d", b.Nodes),
		fmt.Sprintf("ops=%d", b.Ops),
		fmt.Sprintf("conc=%d", b.Concurrency),
	}
	name := strings.Join(nameParts, "/")

	r.Add(testSpec{
		Name:       name,
		Owner:      OwnerSQLSchema,
		Cluster:    makeClusterSpec(b.Nodes),
		MinVersion: "v20.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			runSchemaChangeRandomLoad(ctx, t, c, b.Ops, b.Concurrency)
		},
	})
}

func runSchemaChangeRandomLoad(ctx context.Context, t *test, c *cluster, maxOps, concurrency int) {
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
			t.logger().Errorf(
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
	roachNodes := c.Range(1, c.spec.NodeCount)
	t.Status("copying binaries")
	c.Put(ctx, cockroach, "./cockroach", roachNodes)
	c.Put(ctx, workload, "./workload", loadNode)

	t.Status("starting cockroach nodes")
	c.Start(ctx, t, roachNodes)
	c.Run(ctx, loadNode, "./workload init schemachange")

	runCmd := []string{
		"./workload run schemachange --verbose=1",
		"--tolerate-errors=false",
		// Save the histograms so that they can be reported to https://roachperf.crdb.dev/.
		" --histograms=" + perfArtifactsDir + "/stats.json",
		fmt.Sprintf("--max-ops %d", maxOps),
		fmt.Sprintf("--concurrency %d", concurrency),
	}
	t.Status("running schemachange workload")
	c.Run(ctx, loadNode, runCmd...)

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
	_, err := db.ExecContext(ctx, `USE defaultdb; DROP DATABASE schemachange CASCADE;`)
	if err != nil {
		t.Fatal(err)
	}
	t.Status("performing validation after dropping database")
	validate(db)
}
