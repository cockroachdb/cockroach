// Copyright 2019 The Cockroach Authors.
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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func registerSchemaChangeInvertedIndex(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "schemachange/invertedindex",
		Owner:   registry.OwnerSQLSchema,
		Cluster: r.MakeClusterSpec(5),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runSchemaChangeInvertedIndex(ctx, t, c)
		},
	})
}

// runInvertedIndex tests the correctness and performance of building an
// inverted index on randomly generated JSON data (from the JSON workload).
func runSchemaChangeInvertedIndex(ctx context.Context, t test.Test, c cluster.Cluster) {
	crdbNodes := c.Range(1, c.Spec().NodeCount-1)
	workloadNode := c.Node(c.Spec().NodeCount)

	c.Put(ctx, t.Cockroach(), "./cockroach", crdbNodes)
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", workloadNode)
	c.Start(ctx, crdbNodes)

	cmdInit := "./workload init json {pgurl:1}"
	c.Run(ctx, workloadNode, cmdInit)

	// On a 4-node GCE cluster with the standard configuration, this generates ~10 million rows
	initialDataDuration := time.Minute * 20
	indexDuration := time.Hour
	if c.IsLocal() {
		initialDataDuration = time.Minute
		indexDuration = time.Minute
	}

	// First generate random JSON data using the JSON workload.
	// TODO (lucy): Using a pre-generated test fixture would be much faster
	m := c.NewMonitor(ctx, crdbNodes)

	cmdWrite := fmt.Sprintf(
		"./workload run json --read-percent=0 --duration %s {pgurl:1-%d} --batch 1000 --sequential",
		initialDataDuration.String(), c.Spec().NodeCount-1,
	)
	m.Go(func(ctx context.Context) error {
		c.Run(ctx, workloadNode, cmdWrite)

		db := c.Conn(ctx, 1)
		defer db.Close()

		var count int
		if err := db.QueryRow(`SELECT count(*) FROM json.j`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		t.L().Printf("finished writing %d rows to table", count)

		return nil
	})

	m.Wait()

	// Run the workload (with both reads and writes), and create the index at the same time.
	m = c.NewMonitor(ctx, crdbNodes)

	cmdWriteAndRead := fmt.Sprintf(
		"./workload run json --read-percent=50 --duration %s {pgurl:1-%d} --sequential",
		indexDuration.String(), c.Spec().NodeCount-1,
	)
	m.Go(func(ctx context.Context) error {
		c.Run(ctx, workloadNode, cmdWriteAndRead)
		return nil
	})

	m.Go(func(ctx context.Context) error {
		db := c.Conn(ctx, 1)
		defer db.Close()

		t.L().Printf("creating index")
		start := timeutil.Now()
		if _, err := db.Exec(`CREATE INVERTED INDEX ON json.j (v)`); err != nil {
			return err
		}
		t.L().Printf("index was created, took %v", timeutil.Since(start))

		return nil
	})

	m.Wait()
}
