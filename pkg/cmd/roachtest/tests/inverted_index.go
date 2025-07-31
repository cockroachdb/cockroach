// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func registerSchemaChangeInvertedIndex(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:                       "schemachange/invertedindex",
		Owner:                      registry.OwnerSQLFoundations,
		Cluster:                    r.MakeClusterSpec(5, spec.WorkloadNode()),
		CompatibleClouds:           registry.AllExceptAWS,
		Suites:                     registry.Suites(registry.Nightly),
		Leases:                     registry.MetamorphicLeases,
		RequiresDeprecatedWorkload: true, // uses json
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runSchemaChangeInvertedIndex(ctx, t, c)
		},
	})
}

// runInvertedIndex tests the correctness and performance of building an
// inverted index on randomly generated JSON data (from the JSON workload).
func runSchemaChangeInvertedIndex(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.CRDBNodes())

	cmdInit := "./workload init json {pgurl:1}"
	c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmdInit)

	// On a 4-node GCE cluster with the standard configuration, this generates ~10 million rows
	initialDataDuration := time.Minute * 20
	indexDuration := time.Hour
	if c.IsLocal() {
		initialDataDuration = time.Minute
		indexDuration = time.Minute
	}

	// First generate random JSON data using the JSON workload.
	// TODO (lucy): Using a pre-generated test fixture would be much faster
	m := c.NewMonitor(ctx, c.CRDBNodes())

	cmdWrite := fmt.Sprintf(
		"./workload run json --read-percent=0 --duration %s {pgurl%s} --batch 200 --sequential",
		initialDataDuration.String(), c.CRDBNodes(),
	)
	m.Go(func(ctx context.Context) error {
		c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmdWrite)

		db := c.Conn(ctx, t.L(), 1)
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
	m = c.NewMonitor(ctx, c.CRDBNodes())

	cmdWriteAndRead := fmt.Sprintf(
		"./workload run json --read-percent=50 --duration %s {pgurl%s} --sequential",
		indexDuration.String(), c.CRDBNodes(),
	)
	m.Go(func(ctx context.Context) error {
		c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmdWriteAndRead)
		return nil
	})

	m.Go(func(ctx context.Context) error {
		db := c.Conn(ctx, t.L(), 1)
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
