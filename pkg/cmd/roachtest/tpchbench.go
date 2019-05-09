// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/lib/pq"
)

// tpchBench is a benchmark run on tpch data. There are different groups of
// queries we run against tpch data, represented by different tpchBench values.
type tpchBench int

//go:generate stringer -type=tpchBench

const (
	sql20 tpchBench = iota
	tpch
)

var urlMap = map[tpchBench]string{
	sql20: `https://raw.githubusercontent.com/cockroachdb/cockroach/master/pkg/workload/querybench/2.1-sql-20`,
	tpch:  `https://raw.githubusercontent.com/cockroachdb/cockroach/master/pkg/workload/querybench/tpch-queries`,
}

type tpchBenchSpec struct {
	Nodes             int
	CPUs              int
	ScaleFactor       int
	benchType         tpchBench
	durationInMinutes int
}

// runTPCHBench runs sets of queries against CockroachDB clusters in different
// configurations.
//
// In order to run a benchmark, a TPC-H dataset must first be loaded. To reuse
// this data across runs, it is recommended to use a combination of
// `--cluster=<cluster>` and `--wipe=false` flags to limit the loading phase to
// the first run.
//
// This benchmark runs with a single load generator node running a single
// worker.
func runTPCHBench(ctx context.Context, t *test, c *cluster, b tpchBenchSpec) {
	roachNodes := c.Range(1, c.nodes-1)
	loadNode := c.Node(c.nodes)

	t.Status("copying binaries")
	c.Put(ctx, cockroach, "./cockroach", roachNodes)
	c.Put(ctx, workload, "./workload", loadNode)

	url := urlMap[b.benchType]
	filename := b.benchType.String()
	t.Status(fmt.Sprintf("downloading %s query file from %s", filename, url))
	if err := c.RunE(ctx, loadNode, fmt.Sprintf("curl %s > %s", url, filename)); err != nil {
		t.Fatal(err)
	}

	t.Status("starting nodes")
	c.Start(ctx, t, roachNodes)

	m := newMonitor(ctx, c, roachNodes)
	m.Go(func(ctx context.Context) error {
		t.Status("setting up dataset")
		err := loadTPCHBench(ctx, t, c, b, m, roachNodes, loadNode)
		if err != nil {
			return err
		}

		t.l.Printf("running %s benchmark on tpch scale-factor=%d", filename, b.ScaleFactor)

		// Run with only one worker to get best-case single-query performance.
		// TODO(yuzefovich): instead of duration, specify --max-ops (we'll need to
		// know the number of queries in file - probably just add it as the first
		// non-comment line of the queries' files).
		cmd := fmt.Sprintf(
			"./workload run querybench --db=tpch --concurrency=1 --query-file=%s --duration=%dm {pgurl%s} --histograms=logs/stats.json",
			filename,
			b.durationInMinutes,
			roachNodes,
		)
		if err := c.RunE(ctx, loadNode, cmd); err != nil {
			t.Fatal(err)
		}
		return nil
	})
	m.Wait()
}

// loadTPCHBench loads a TPC-H dataset for the specific benchmark spec. The
// function is idempotent and first checks whether a compatible dataset exists,
// performing an expensive dataset restore only if it doesn't.
func loadTPCHBench(
	ctx context.Context,
	t *test,
	c *cluster,
	b tpchBenchSpec,
	m *monitor,
	roachNodes, loadNode nodeListOption,
) error {
	db := c.Conn(ctx, roachNodes[0])
	defer db.Close()

	if _, err := db.ExecContext(ctx, `USE tpch`); err == nil {
		t.l.Printf("found existing tpch dataset, verifying scale factor\n")

		var supplierCardinality int
		if err := db.QueryRowContext(
			ctx, `SELECT count(*) FROM tpch.supplier`,
		).Scan(&supplierCardinality); err != nil {
			if pqErr, ok := err.(*pq.Error); !(ok && pqErr.Code == pgerror.CodeUndefinedTableError) {
				return err
			}
			// Table does not exist. Set cardinality to 0.
			supplierCardinality = 0
		}

		// Check if a tpch database with the required scale factor exists.
		// 10000 is the number of rows in the supplier table at scale factor 1.
		// supplier is the smallest table whose cardinality scales with the scale
		// factor.
		expectedSupplierCardinality := 10000 * b.ScaleFactor
		if supplierCardinality >= expectedSupplierCardinality {
			t.l.Printf("dataset is at least of scale factor %d, continuing", b.ScaleFactor)
			return nil
		}

		// If the scale factor was smaller than the required scale factor, wipe the
		// cluster and restore.
		m.ExpectDeaths(int32(c.nodes))
		c.Wipe(ctx, roachNodes)
		c.Start(ctx, t, roachNodes)
		m.ResetDeaths()
	} else if pqErr, ok := err.(*pq.Error); !ok ||
		string(pqErr.Code) != pgerror.CodeInvalidCatalogNameError {
		return err
	}

	t.l.Printf("restoring tpch scale factor %d\n", b.ScaleFactor)
	tpchURL := fmt.Sprintf("gs://cockroach-fixtures/workload/tpch/scalefactor=%d/backup", b.ScaleFactor)
	query := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS tpch; RESTORE tpch.* FROM '%s' WITH into_db = 'tpch';`, tpchURL)
	_, err := db.ExecContext(ctx, query)
	return err
}

func registerTPCHBenchSpec(r *registry, b tpchBenchSpec) {
	nameParts := []string{
		"tpchbench",
		b.benchType.String(),
		fmt.Sprintf("nodes=%d", b.Nodes),
		fmt.Sprintf("cpu=%d", b.CPUs),
		fmt.Sprintf("sf=%d", b.ScaleFactor),
	}

	// Add a load generator node.
	numNodes := b.Nodes + 1

	r.Add(testSpec{
		Name:       strings.Join(nameParts, "/"),
		Cluster:    makeClusterSpec(numNodes),
		MinVersion: maybeMinVersionForFixturesImport(cloud),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCHBench(ctx, t, c, b)
		},
	})
}

func registerTPCHBench(r *registry) {
	specs := []tpchBenchSpec{
		{
			Nodes:             3,
			CPUs:              4,
			ScaleFactor:       1,
			benchType:         sql20,
			durationInMinutes: 5,
		},
		{
			Nodes:             3,
			CPUs:              4,
			ScaleFactor:       1,
			benchType:         tpch,
			durationInMinutes: 30,
		},
	}

	for _, b := range specs {
		registerTPCHBenchSpec(r, b)
	}
}
