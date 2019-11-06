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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
)

func registerTPCHVec(r *testRegistry) {
	const (
		nodeCount       = 3
		numTPCHQueries  = 22
		vecOnConfig     = 0
		vecOffConfig    = 1
		numRunsPerQuery = 3
	)

	var vecOnQueriesToSkip = map[int]string{
		// TODO(yuzefovich): remove this once we have memory monitoring.
		9:  "can cause OOM",
		10: "incorrect output: #42049",
		12: "unsupported: sum_int #38845",
		// TODO(yuzefovich): triage this failure.
		13: "incorrect output: unknown",
		15: "unsupported: create view",
		16: "unsupported: distinct aggregation #39242",
		20: "incorrect output: #42047",
		21: "unsupported: non-inner hash join with ON expression #38018",
	}
	var vecOffQueriesToSkip = map[int]string{
		// TODO(yuzefovich): remove this once we're not skipping query 9 with vec
		// on.
		9:  "too long",
		12: "the query is skipped by tpch workload",
	}

	runTPCHVec := func(ctx context.Context, t *test, c *cluster) {
		TPCHTables := []string{
			"nation", "region", "part", "supplier",
			"partsupp", "customer", "orders", "lineitem",
		}

		firstNode := c.Node(1)
		c.Put(ctx, cockroach, "./cockroach", c.All())
		c.Put(ctx, workload, "./workload", firstNode)
		c.Start(ctx, t)

		conn := c.Conn(ctx, 1)
		t.Status("restoring TPCH dataset for Scale Factor 1")
		setup := `
CREATE DATABASE tpch;
RESTORE tpch.* FROM 'gs://cockroach-fixtures/workload/tpch/scalefactor=1/backup' WITH into_db = 'tpch';
`
		if _, err := conn.Exec(setup); err != nil {
			t.Fatal(err)
		}

		t.Status("scattering the data")
		if _, err := conn.Exec("USE tpch;"); err != nil {
			t.Fatal(err)
		}
		for _, table := range TPCHTables {
			scatter := fmt.Sprintf("ALTER TABLE %s SCATTER;", table)
			if _, err := conn.Exec(scatter); err != nil {
				t.Fatal(err)
			}
		}
		t.Status("waiting for full replication")
		waitForFullReplication(t, conn)
		timeByQueryNum := make([]map[int][]float64, 2)
		// Note that the order in which we run the configuration is important:
		// there are some issues with dropping a view (created in query 15), so if
		// we run vec off first, the vec on config run might error out.
		// TODO(yuzefovich): figure out what is the root problem or create an issue
		// about it.
		for configIdx, vectorize := range []bool{true, false} {
			// To reduce the variance on the first query we're interested in, we'll
			// do an aggregation over all tables. This will make comparison on two
			// different configs more fair.
			t.Status("reading all tables to populate the caches")
			for _, table := range TPCHTables {
				count := fmt.Sprintf("SELECT count(*) FROM %s;", table)
				if _, err := conn.Exec(count); err != nil {
					t.Fatal(err)
				}
			}
			queriesToSkip := vecOffQueriesToSkip
			if vectorize {
				queriesToSkip = vecOnQueriesToSkip
			}
			var queriesToRun string
			for queryNum := 1; queryNum <= numTPCHQueries; queryNum++ {
				if _, ok := queriesToSkip[queryNum]; !ok {
					if queriesToRun == "" {
						queriesToRun = fmt.Sprintf("%d", queryNum)
					} else {
						queriesToRun = fmt.Sprintf("%s,%d", queriesToRun, queryNum)
					}
				}
			}
			vectorizeSetting := "off"
			if vectorize {
				vectorizeSetting = "experimental_always"
			}
			operation := fmt.Sprintf(
				"running TPCH queries %s with vectorize=%s %d times each",
				queriesToRun, vectorizeSetting, numRunsPerQuery,
			)
			cmd := fmt.Sprintf("./workload run tpch --concurrency=1 --db=tpch "+
				"--max-ops=%d --queries=%s --vectorize=%s {pgurl:1-%d}",
				numRunsPerQuery*(numTPCHQueries-len(queriesToSkip)),
				queriesToRun, vectorizeSetting, nodeCount)
			workloadOutput, err := repeatRunWithBuffer(ctx, c, t.l, firstNode, operation, cmd)
			if err != nil {
				t.Fatal(err)
			}
			t.l.Printf(string(workloadOutput))
			timeByQueryNum[configIdx] = make(map[int][]float64)
			parseOutput := func(output []byte, timeByQueryNum map[int][]float64) {
				runtimeRegex := regexp.MustCompile(`.*\[q([\d]+)\] returned \d+ rows after ([\d]+\.[\d]+) seconds.*`)
				scanner := bufio.NewScanner(bytes.NewReader(output))
				for scanner.Scan() {
					line := scanner.Bytes()
					match := runtimeRegex.FindSubmatch(line)
					if match != nil {
						queryNum, err := strconv.Atoi(string(match[1]))
						if err != nil {
							t.Fatalf("failed parsing %q as int with %s", match[1], err)
						}
						queryTime, err := strconv.ParseFloat(string(match[2]), 64)
						if err != nil {
							t.Fatalf("failed parsing %q as float with %s", match[2], err)
						}
						timeByQueryNum[queryNum] = append(timeByQueryNum[queryNum], queryTime)
					}
				}
			}
			parseOutput(workloadOutput, timeByQueryNum[configIdx])
		}
		t.Status("comparing the runtimes (only median values for each query are compared)")
		for queryNum := 1; queryNum <= numTPCHQueries; queryNum++ {
			if _, skipped := vecOnQueriesToSkip[queryNum]; skipped {
				continue
			} else if _, skipped := vecOffQueriesToSkip[queryNum]; skipped {
				continue
			}
			findMedian := func(times []float64) float64 {
				sort.Float64s(times)
				return times[len(times)/2]
			}
			vecOnTime := findMedian(timeByQueryNum[vecOnConfig][queryNum])
			vecOffTime := findMedian(timeByQueryNum[vecOffConfig][queryNum])
			if vecOffTime < vecOnTime {
				t.l.Printf(
					fmt.Sprintf("[q%d] vec OFF was faster by %.2f%%: "+
						"%.2fs ON vs %.2fs OFF --- WARNING",
						queryNum, 100*(vecOnTime-vecOffTime)/vecOffTime, vecOnTime, vecOffTime))
			} else {
				t.l.Printf(
					fmt.Sprintf("[q%d] vec ON was faster by %.2f%%: "+
						"%.2fs ON vs %.2fs OFF",
						queryNum, 100*(vecOffTime-vecOnTime)/vecOnTime, vecOnTime, vecOffTime))
			}
		}
	}

	r.Add(testSpec{
		Name:       "tpchvec",
		Cluster:    makeClusterSpec(nodeCount),
		MinVersion: "v19.2.0",
		Run:        runTPCHVec,
	})
}
