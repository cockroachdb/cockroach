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
	"strings"

	tpchworkload "github.com/cockroachdb/cockroach/pkg/workload/tpch"
)

func registerTPCHVec(r *testRegistry) {
	const (
		nodeCount       = 3
		numTPCHQueries  = 22
		vecOnConfig     = 0
		vecOffConfig    = 1
		numRunsPerQuery = 3
		// vecOnSlowerFailFactor describes the threshold at which we fail the test
		// if vec ON is slower that vec OFF, meaning that if
		// vec_on_time > vecOnSlowerFailFactor * vec_off_time, the test is failed.
		// This will help catch any regressions.
		vecOnSlowerFailFactor = 1.5
	)

	// queriesToSkipByVersionPrefix is a map from version prefix to another map
	// that contains query numbers to be skipped (as well as the reasons for why
	// they are skipped).
	queriesToSkipByVersionPrefix := make(map[string]map[int]string)
	queriesToSkipByVersionPrefix["v19.2"] = map[int]string{
		5:  "can cause OOM",
		7:  "can cause OOM",
		8:  "can cause OOM",
		9:  "can cause OOM",
		12: "the query is skipped by tpch workload",
		19: "can cause OOM",
	}
	queriesToSkipByVersionPrefix["v20.1"] = map[int]string{
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

		// TODO(yuzefovich): remove this once we have disk spilling.
		workmem := "1GiB"
		t.Status(fmt.Sprintf("setting workmem='%s'", workmem))
		if _, err := conn.Exec(fmt.Sprintf("SET CLUSTER SETTING sql.distsql.temp_storage.workmem='%s'", workmem)); err != nil {
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
		version, err := fetchCockroachVersion(ctx, c, c.Node(1)[0])
		if err != nil {
			t.Fatal(err)
		}
		var queriesToSkip map[int]string
		for versionPrefix, toSkip := range queriesToSkipByVersionPrefix {
			if strings.HasPrefix(version, versionPrefix) {
				queriesToSkip = toSkip
				break
			}
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
			vectorizeSetting := "off"
			if vectorize {
				vectorizeSetting = "experimental_on"
			}
			operation := fmt.Sprintf(
				"running TPCH queries %s with vectorize=%s %d times each",
				queriesToRun, vectorizeSetting, numRunsPerQuery,
			)
			cmd := fmt.Sprintf("./workload run tpch --concurrency=1 --db=tpch "+
				"--max-ops=%d --queries=%s --vectorize=%s --tolerate-errors {pgurl:1-%d}",
				numRunsPerQuery*(numTPCHQueries-len(queriesToSkip)),
				queriesToRun, vectorizeSetting, nodeCount)
			workloadOutput, err := repeatRunWithBuffer(ctx, c, t.l, firstNode, operation, cmd)
			if err != nil {
				// Note: if you see an error like "exit status 1", it is likely caused
				// by the erroneous output of the query.
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
			// We want to fail the test only if wrong results were returned (we
			// ignore errors like OOM and unsupported features in order to not
			// short-circuit the run of this test).
			if strings.Contains(string(workloadOutput), tpchworkload.TPCHWrongOutputErrorPrefix) {
				t.Fatal("tpch workload found wrong results")
			}
		}
		// TODO(yuzefovich): remove the note when disk spilling is in place.
		t.Status("comparing the runtimes (only median values for each query are compared).\n" +
			"NOTE: the comparison might not be fair because vec ON doesn't spill to disk")
		for queryNum := 1; queryNum <= numTPCHQueries; queryNum++ {
			findMedian := func(times []float64) float64 {
				sort.Float64s(times)
				return times[len(times)/2]
			}
			vecOnTimes := timeByQueryNum[vecOnConfig][queryNum]
			vecOffTimes := timeByQueryNum[vecOffConfig][queryNum]
			// It is possible that the query errored out on one of the configs. We
			// want to compare the runtimes only if both have not errored out.
			if len(vecOnTimes) > 0 && len(vecOffTimes) > 0 {
				vecOnTime := findMedian(vecOnTimes)
				vecOffTime := findMedian(vecOffTimes)
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
				// Note that it is possible that some queries will hit memory limits on
				// some of the runs when running with vec ON. In order to reduce the
				// noise about regressions, we require that a query had at least 3
				// successful runs (some queries might have 4 because of the memory
				// limit errors - those do not count towards --max-ops flag of tpch
				// workload).
				// TODO(yuzefovich): remove the first condition once we have disk
				// spilling in all components.
				if len(vecOnTimes) >= numRunsPerQuery && vecOnTime >= vecOnSlowerFailFactor*vecOffTime {
					t.Fatal(fmt.Sprintf("[q%d] vec ON is slower by %.2f%% that vec OFF",
						queryNum, 100*(vecOnTime-vecOffTime)/vecOffTime))
				}
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
