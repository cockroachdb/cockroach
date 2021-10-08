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
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/workload/querybench"
)

type tpchBenchSpec struct {
	Nodes           int
	CPUs            int
	ScaleFactor     int
	benchType       string
	url             string
	numRunsPerQuery int
	// minVersion specifies the minimum version of CRDB nodes. If omitted, it
	// will default to v19.1.0.
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
func runTPCHBench(ctx context.Context, t test.Test, c cluster.Cluster, b tpchBenchSpec) {
	roachNodes := c.Range(1, c.Spec().NodeCount-1)
	loadNode := c.Node(c.Spec().NodeCount)

	t.Status("copying binaries")
	c.Put(ctx, t.Cockroach(), "./cockroach", roachNodes)
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", loadNode)

	filename := b.benchType
	t.Status(fmt.Sprintf("downloading %s query file from %s", filename, b.url))
	if err := c.RunE(ctx, loadNode, fmt.Sprintf("curl %s > %s", b.url, filename)); err != nil {
		t.Fatal(err)
	}

	t.Status("starting nodes")
	c.Start(ctx, roachNodes)

	m := c.NewMonitor(ctx, roachNodes)
	m.Go(func(ctx context.Context) error {
		t.Status("setting up dataset")
		err := loadTPCHDataset(ctx, t, c, b.ScaleFactor, m, roachNodes)
		if err != nil {
			return err
		}

		t.L().Printf("running %s benchmark on tpch scale-factor=%d", filename, b.ScaleFactor)

		numQueries, err := getNumQueriesInFile(filename, b.url)
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
				"--histograms="+t.PerfArtifactsDir()+"/stats.json --histograms-max-latency=%s",
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

func registerTPCHBenchSpec(r registry.Registry, b tpchBenchSpec) {
	nameParts := []string{
		"tpchbench",
		b.benchType,
		fmt.Sprintf("nodes=%d", b.Nodes),
		fmt.Sprintf("cpu=%d", b.CPUs),
		fmt.Sprintf("sf=%d", b.ScaleFactor),
	}

	// Add a load generator node.
	numNodes := b.Nodes + 1
	minVersion := b.minVersion
	if minVersion == `` {
		minVersion = "v19.1.0" // needed for import
	}

	r.Add(registry.TestSpec{
		Name:    strings.Join(nameParts, "/"),
		Owner:   registry.OwnerSQLQueries,
		Cluster: r.MakeClusterSpec(numNodes),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCHBench(ctx, t, c, b)
		},
	})
}

func registerTPCHBench(r registry.Registry) {
	specs := []tpchBenchSpec{
		{
			Nodes:           3,
			CPUs:            4,
			ScaleFactor:     1,
			benchType:       `sql20`,
			url:             `https://raw.githubusercontent.com/cockroachdb/cockroach/master/pkg/workload/querybench/2.1-sql-20`,
			numRunsPerQuery: 3,
			maxLatency:      100 * time.Second,
		},
		{
			Nodes:           3,
			CPUs:            4,
			ScaleFactor:     1,
			benchType:       `tpch`,
			url:             `https://raw.githubusercontent.com/cockroachdb/cockroach/master/pkg/workload/querybench/tpch-queries`,
			numRunsPerQuery: 3,
			minVersion:      `v19.2.0`,
			maxLatency:      500 * time.Second,
		},
	}

	for _, b := range specs {
		registerTPCHBenchSpec(r, b)
	}
}
