// Copyright 2018 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const (
	maxQueryTime        = 10 * time.Second
	mbInBytes    uint64 = 1 << 20
)

type diskUsageTestCase struct {
	name string
	// Negative values here imply given space has to be left in the store directory.
	ratioDiskFilled  map[int]float64
	diskEmptyInBytes map[int]uint64
	singleKeyOpProb  float64
}

func (tc *diskUsageTestCase) numNodes() (numNodes int, err error) {
	if tc.ratioDiskFilled != nil && tc.diskEmptyInBytes != nil {
		return 0, errors.Errorf("both ratioDiskFilled and diskEmptyInBytes can't be set in test case")
	}
	if tc.ratioDiskFilled != nil {
		numNodes = len(tc.ratioDiskFilled)
	} else {
		numNodes = len(tc.diskEmptyInBytes)
	}
	return
}

func runDiskUsage(t *test, c *cluster, duration time.Duration, tc diskUsageTestCase) {
	numNodes, err := tc.numNodes()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	c.Put(ctx, workload, "./workload", c.Node(numNodes))
	c.Put(ctx, cockroach, "./cockroach", c.All())
	c.Start(ctx, t, c.All())

	loadDuration := " --duration=" + (duration / 2).String()

	workloads := []string{
		"./workload run kv --max-rate 500 --tolerate-errors --init" + loadDuration + " {pgurl:1-%d}",
	}

	m, ctxWG := errgroup.WithContext(ctx)
	for i, cmd := range workloads {
		cmd := cmd // Copy is important for goroutine.
		i := i     // Ditto.

		cmd = fmt.Sprintf(cmd, nodes)
		m.Go(func() error {
			quietL, err := c.l.ChildLogger("kv-"+strconv.Itoa(i), quietStdout)
			if err != nil {
				return err
			}
			defer quietL.close()
			return c.RunL(ctxWG, quietL, c.Node(numNodes), cmd)
		})
	}

	if err := m.Wait(); err != nil {
		t.Fatal(err)
	}
	ballastFilePath := "{store-dir}/ballast_file_to_fill_store"
	mFillBallast, ctxFillBallast := errgroup.WithContext(ctx)
	for i := 1; i <= numNodes; i++ {
		nodeIdx := i
		mFillBallast.Go(
			func() error {
				fillBallastCommand := []string{"./cockroach", "debug", "ballast", ballastFilePath, "--size"}
				if ratio, ok := tc.ratioDiskFilled[nodeIdx]; ok {
					fillBallastCommand = append(fillBallastCommand, fmt.Sprint(ratio))
				}
				if diskEmpty, ok := tc.diskEmptyInBytes[nodeIdx]; ok {
					fillBallastCommand = append(fillBallastCommand, fmt.Sprintf("-%v", diskEmpty))
				}
				if err := c.RunE(ctxFillBallast, c.Node(nodeIdx), fillBallastCommand...); err != nil {
					return errors.Errorf("failed to create ballast file on node %d due to error: %v", i, err)
				}
				return nil
			},
		)
	}
	if err := mFillBallast.Wait(); err != nil {
		t.Fatal(err)
	}

	dbOne := c.Conn(ctx, 1)
	defer dbOne.Close()

	clusterDiskUsage := func() int {
		totalDiskUsage := 0
		for i := 1; i <= numNodes; i++ {
			diskUsageOnNodeI, err := getDiskUsageInBytes(ctx, c, c.l, i)
			if err != nil {
				t.Fatalf("Failed to get disk usage on node %d, error: %v", i, err)
			}
			totalDiskUsage += diskUsageOnNodeI
		}
		return totalDiskUsage
	}

	// We are removing the EXPERIMENTAL keyword in 2.1. For compatibility
	// with 2.0 clusters we still need to try with it if the
	// syntax without EXPERIMENTAL fails.
	// TODO(knz): Remove this in 2.2.
	makeStmt := func(s string) string { return fmt.Sprintf(s, "RANGE default", "'gc: {ttlseconds: 10}'") }
	stmt := makeStmt("ALTER %[1]s CONFIGURE ZONE TO %[2]s")
	t.Status(stmt)
	_, err = dbOne.ExecContext(ctx, stmt)
	if err != nil && strings.Contains(err.Error(), "syntax error") {
		stmt = makeStmt("ALTER %[1]s EXPERIMENTAL CONFIGURE ZONE %[2]s")
		_, err = dbOne.ExecContext(ctx, stmt)
	}
	if err != nil {
		t.Fatal(err)
	}

	t.Status("Running inserts and deletes")
	m2, ctxWorkLoad := errgroup.WithContext(ctx)
	for j := 1; j <= numNodes; j++ {
		nodeID := j
		m2.Go(
			func() error {
				db := c.Conn(ctxWorkLoad, nodeID)
				start := timeutil.Now()
				count := 0
				for timeutil.Since(start) < (duration / 2) {
					randomSeed := rand.Float64()
					var k int64
					if randomSeed < tc.singleKeyOpProb {
						// Simulate hot row update loop.
						k = 0
					} else {
						// This number is different across nodes for the whole test phase assuming there
						// are less than math.MaxInt32 inserts per node.
						k = int64(j)*int64(math.MaxInt32) + int64(count)
					}
					ctxQuery, cancelQuery1 := context.WithTimeout(ctxWorkLoad, maxQueryTime)
					defer cancelQuery1()
					rng, _ := randutil.NewPseudoRand()
					bytes := randutil.RandBytes(rng, 2000)
					if _, err := db.ExecContext(ctxQuery, "UPSERT INTO kv.kv(k, v) Values($1, $2)", k, bytes); err != nil {
						return err
					}
					ctxQuery, cancelQuery2 := context.WithTimeout(ctxWorkLoad, maxQueryTime)
					defer cancelQuery2()
					if _, err := db.ExecContext(ctxQuery, "DELETE FROM kv.kv WHERE k=$1", k); err != nil {
						return err
					}
					count++
				}
				return nil
			},
		)
	}

	printDiskUsages := func() {
		for i := 1; i <= numNodes; i++ {
			quietL, err := c.l.ChildLogger(fmt.Sprintf("df-%d", i), quietStdout)
			if err != nil {
				t.Fatal(err)
			}
			defer quietL.close()
			output, err := c.RunWithBuffer(ctx, quietL, c.Node(i), "df", "-h", "{store-dir}")
			if err != nil {
				c.l.Printf("Failed to run df on node %d due to error: %v\n", i, err)
			} else {
				c.l.Printf("node %d, df output:\n %s\n", i, string(output))
			}
		}
	}

	var steadyStateDiskUsage int
	done := make(chan struct{})
	go func() {
		i := 0
		ticker := time.NewTicker(time.Minute)
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				printDiskUsages()
				// In steady state, we expect around 10 seconds(TTL) of MVCC data that hasn't been GCed,
				// which should remain constant. As the disk space depends on various things including
				// when RocksDB memtable gets flushed, we keep a buffer of extra 10 secs MVCC data as
				// breathing space for GC. We are checking the whole cluster disk space usage to avoid
				// discrepancies due to range rebalancing.
				if i == 0 {
					steadyStateDiskUsage = clusterDiskUsage()
				} else {
					currentDiskUsages := clusterDiskUsage()
					// Assuming lower bound of 1ms for inserts and deletes, 2KB of data per row, and
					// replication factor of 3, we expect 60MB of data per node per 10 second(TTL).
					// This counts for around 240MB data for the whole cluster. This doesn't account for
					// RocksDB Write Amplification and the double space that a range takes just after
					// rebalancing, before the old replica gets cleaned up. But empirically,
					// there is less than 150MB increase for ttlInSeconds of 10 secs, so keeping the buffer
					// to be 200MB.
					const maxIncreaseInDiskUsageAllowed = int(200 * mbInBytes)
					if currentDiskUsages > steadyStateDiskUsage {
						increaseInDiskUsage := currentDiskUsages - steadyStateDiskUsage
						if increaseInDiskUsage > maxIncreaseInDiskUsageAllowed {
							t.Fatalf(
								"Difference between disk usage since last checked %d is more than max allowed %d",
								increaseInDiskUsage,
								maxIncreaseInDiskUsageAllowed,
							)
						}
					} else {
						steadyStateDiskUsage = currentDiskUsages
					}
				}
				i++
			}
		}
	}()

	if err := m2.Wait(); err != nil {
		t.Fatal(err)
	}
	close(done)
	for i := 1; i <= numNodes; i++ {
		if err := c.RunE(
			ctx,
			c.Node(i),
			"rm",
			ballastFilePath,
		); err != nil {
			c.l.Printf("Failed to remove ballast file on node %d due to error: %v\n", i, err)
		}
	}
}

func registerDiskUsage(r *registry) {
	duration := 20 * time.Minute
	const twoHundredMB = uint64(200) * mbInBytes

	testCases := []diskUsageTestCase{
		// Check the behavior when disk space left is of around one range.
		{
			// The workload involves both hot row update and mix rows with equal probability.
			name:             "update_mix_hot_key",
			diskEmptyInBytes: map[int]uint64{1: twoHundredMB, 2: twoHundredMB, 3: twoHundredMB, 4: twoHundredMB},
			singleKeyOpProb:  0.5,
		},
		{
			// The workload involves only update to a single row (hot row update).
			name:             "update_hot_key",
			diskEmptyInBytes: map[int]uint64{1: twoHundredMB, 2: twoHundredMB, 3: twoHundredMB, 4: twoHundredMB},
			singleKeyOpProb:  1.0,
		},
		{
			// The workload involves only update to a random mix of rows.
			name:             "update_mix",
			diskEmptyInBytes: map[int]uint64{1: twoHundredMB, 2: twoHundredMB, 3: twoHundredMB, 4: twoHundredMB},
			singleKeyOpProb:  0.0,
		},
		// Check the behavior when disk space left is around 90%. This is to trigger
		// specific logic in cockroach that uses specific %age of disk used(Mainly rebalancing).
		{
			// The workload involves both hot row update and mix rows with equal probability.
			name:            "update_mix_hot_key_percent",
			ratioDiskFilled: map[int]float64{1: -0.1, 2: -0.05, 3: -0.1, 4: -0.1},
			singleKeyOpProb: 0.5,
		},
		{
			// The workload involves only update to a single row (hot row update).
			name:            "update_hot_key_percent",
			ratioDiskFilled: map[int]float64{1: -0.1, 2: -0.05, 3: -0.1, 4: -0.1},
			singleKeyOpProb: 1.0,
		},
		{
			// The workload involves only update to a random mix of rows.
			name:            "update_mix_percent",
			ratioDiskFilled: map[int]float64{1: -0.1, 2: -0.05, 3: -0.1, 4: -0.1},
			singleKeyOpProb: 0.0,
		},
	}

	for i := range testCases {
		testCase := testCases[i]
		numNodes, err := testCase.numNodes()
		if err != nil {
			continue
		}
		r.Add(
			testSpec{
				Name:       fmt.Sprintf("disk_space/tc=%s", testCase.name),
				Nodes:      nodes(numNodes),
				MinVersion: "2.1", // cockroach debug ballast
				Run: func(ctx context.Context, t *test, c *cluster) {
					if local {
						duration = 30 * time.Minute
						fmt.Printf("running with duration=%s in local mode\n", duration)
					}
					runDiskUsage(t, c, duration, testCase)
				},
			},
		)
	}
}
