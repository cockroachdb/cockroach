// Copyright 2019 The Cockroach Authors.
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
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/workload/querybench"
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
	Nodes           int
	CPUs            int
	ScaleFactor     int
	benchType       tpchBench
	numRunsPerQuery int
	// minVersion specifies the minimum version of CRDB nodes. If omitted, it
	// will default to maybeMinVersionForFixturesImport.
	minVersion string
	// maxLatency is the expected maximum time that a query will take to execute
	// needed to correctly initialize histograms.
	maxLatency time.Duration
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
	roachNodes := c.Range(1, c.spec.NodeCount-1)
	loadNode := c.Node(c.spec.NodeCount)

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

		numQueries, err := getNumQueriesInFile(filename, url)
		if err != nil {
			t.Fatal(err)
		}
		// maxOps flag will allow us to exit the workload once all the queries were
		// run b.numRunsPerQuery number of times.
		maxOps := b.numRunsPerQuery * numQueries

		// Run with only one worker to get best-case single-query performance.
		cmd := fmt.Sprintf(
			"./workload run querybench --db=tpch --concurrency=1 --query-file=%s "+
				"--num-runs=%d --max-ops=%d {pgurl%s} "+
				"--histograms="+perfArtifactsDir+"/stats.json --histograms-max-latency=%s",
			filename,
			b.numRunsPerQuery,
			maxOps,
			roachNodes,
			b.maxLatency.String(),
		)
		if err := c.RunE(ctx, loadNode, cmd); err != nil {
			t.Fatal(err)
		}
		return nil
	})
	m.Wait()
}

// getNumQueriesInFile downloads a file that url points to, stores it at a
// temporary location, parses it using querybench, and deletes the file. It
// returns the number of queries in the file.
func getNumQueriesInFile(filename, url string) (int, error) {
	tempFile, err := downloadFile(filename, url)
	if err != nil {
		return 0, err
	}
	// Use closure to make linter happy about unchecked error.
	defer func() {
		_ = os.Remove(tempFile.Name())
	}()

	queries, err := querybench.GetQueries(tempFile.Name())
	if err != nil {
		return 0, err
	}
	return len(queries), nil
}

// downloadFile will download a url as a local temporary file.
func downloadFile(filename string, url string) (*os.File, error) {
	// These files may be a bit large, so give ourselves
	// some room before the timeout expires.
	httpClient := httputil.NewClientWithTimeout(30 * time.Second)
	// Get the data.
	resp, err := httpClient.Get(context.TODO(), url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Create the file.
	out, err := ioutil.TempFile(`` /* dir */, filename)
	if err != nil {
		return nil, err
	}
	defer out.Close()

	// Write the body to file.
	_, err = io.Copy(out, resp.Body)
	return out, err
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
			if pqErr, ok := err.(*pq.Error); !(ok && pqErr.Code == pgcode.UndefinedTable) {
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
		m.ExpectDeaths(int32(c.spec.NodeCount))
		c.Wipe(ctx, roachNodes)
		c.Start(ctx, t, roachNodes)
		m.ResetDeaths()
	} else if pqErr, ok := err.(*pq.Error); !ok ||
		string(pqErr.Code) != pgcode.InvalidCatalogName {
		return err
	}

	t.l.Printf("restoring tpch scale factor %d\n", b.ScaleFactor)
	tpchURL := fmt.Sprintf("gs://cockroach-fixtures/workload/tpch/scalefactor=%d/backup", b.ScaleFactor)
	query := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS tpch; RESTORE tpch.* FROM '%s' WITH into_db = 'tpch';`, tpchURL)
	_, err := db.ExecContext(ctx, query)
	return err
}

func registerTPCHBenchSpec(r *testRegistry, b tpchBenchSpec) {
	nameParts := []string{
		"tpchbench",
		b.benchType.String(),
		fmt.Sprintf("nodes=%d", b.Nodes),
		fmt.Sprintf("cpu=%d", b.CPUs),
		fmt.Sprintf("sf=%d", b.ScaleFactor),
	}

	// Add a load generator node.
	numNodes := b.Nodes + 1
	minVersion := b.minVersion
	if minVersion == `` {
		minVersion = maybeMinVersionForFixturesImport(cloud)
	}

	r.Add(testSpec{
		Name:       strings.Join(nameParts, "/"),
		Cluster:    makeClusterSpec(numNodes),
		MinVersion: minVersion,
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCHBench(ctx, t, c, b)
		},
	})
}

func registerTPCHBench(r *testRegistry) {
	specs := []tpchBenchSpec{
		{
			Nodes:           3,
			CPUs:            4,
			ScaleFactor:     1,
			benchType:       sql20,
			numRunsPerQuery: 3,
			maxLatency:      100 * time.Second,
		},
		{
			Nodes:           3,
			CPUs:            4,
			ScaleFactor:     1,
			benchType:       tpch,
			numRunsPerQuery: 3,
			minVersion:      `v19.1.0`,
			maxLatency:      500 * time.Second,
		},
	}

	for _, b := range specs {
		registerTPCHBenchSpec(r, b)
	}
}
