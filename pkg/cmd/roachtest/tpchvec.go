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
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	tpchworkload "github.com/cockroachdb/cockroach/pkg/workload/tpch"
)

func registerTPCHVec(r *testRegistry) {
	const (
		nodeCount       = 3
		numTPCHQueries  = 22
		vecOnConfig     = 0
		vecOffConfig    = 1
		numRunsPerQuery = 5
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
		19: "can cause OOM",
	}
	queriesToSkipByVersionPrefix["v20.1"] = map[int]string{
		// TODO(yuzefovich): remove this once disk spilling is in place.
		9: "needs disk spilling",
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

		rng, _ := randutil.NewPseudoRand()
		workmemInMiB := 1 + rng.Intn(64)
		workmem := fmt.Sprintf("%dMiB", workmemInMiB)
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
		numMinutesToSleep := 3
		t.Status(fmt.Sprintf("sleeping for %d minutes to allow for automatic"+
			" statistics collection to be performed", numMinutesToSleep))
		for i := 0; i < numMinutesToSleep; i++ {
			<-time.After(time.Minute)
			t.Status(fmt.Sprintf("%d minutes have passed", i+1))
		}
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
		t.Status("setting vmodule=bytes_usage=1 on all nodes")
		for node := 1; node <= nodeCount; node++ {
			conn := c.Conn(ctx, node)
			if _, err := conn.Exec("SELECT crdb_internal.set_vmodule('bytes_usage=1');"); err != nil {
				t.Fatal(err)
			}
		}
		timeByQueryNum := []map[int][]float64{make(map[int][]float64), make(map[int][]float64)}
		for queryNum := 1; queryNum <= numTPCHQueries; queryNum++ {
			for configIdx, vectorize := range []bool{true, false} {
				if reason, skip := queriesToSkip[queryNum]; skip {
					t.Status(fmt.Sprintf("skipping q%d because of %q", queryNum, reason))
					continue
				}
				vectorizeSetting := "off"
				if vectorize {
					vectorizeSetting = "experimental_on"
				}
				cmd := fmt.Sprintf("./workload run tpch --concurrency=1 --db=tpch "+
					"--max-ops=%d --queries=%d --vectorize=%s {pgurl:1-%d}",
					numRunsPerQuery, queryNum, vectorizeSetting, nodeCount)
				workloadOutput, err := c.RunWithBuffer(ctx, t.l, firstNode, cmd)
				t.l.Printf("\n" + string(workloadOutput))
				if err != nil {
					// Note: if you see an error like "exit status 1", it is likely caused
					// by the erroneous output of the query.
					if strings.Contains(string(workloadOutput), "memory budget exceeded") {
						t.Status(fmt.Sprintf("\n%s", err))
					} else {
						t.Fatal(err)
					}
				}
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
			if len(vecOffTimes) != numRunsPerQuery {
				t.Fatal(fmt.Sprintf("[q%d] unexpectedly wrong number of run times "+
					"recorded with vec OFF config: %v", queryNum, vecOffTimes))
			}
			// It is possible that the query errored out on vec ON config. We want to
			// compare the run times only if that's not the case.
			if len(vecOnTimes) > 0 {
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
				if vecOnTime >= vecOnSlowerFailFactor*vecOffTime {
					t.Fatal(fmt.Sprintf(
						"[q%d] vec ON is slower by %.2f%% than vec OFF\n"+
							"vec ON times: %v\nvec OFF times: %v",
						queryNum, 100*(vecOnTime-vecOffTime)/vecOffTime, vecOnTimes, vecOffTimes))
				}
			}
		}
	}

	r.Add(testSpec{
		Name:       "tpchvec",
		Owner:      OwnerSQLExec,
		Cluster:    makeClusterSpec(nodeCount),
		MinVersion: "v19.2.0",
		Run:        runTPCHVec,
	})
}
