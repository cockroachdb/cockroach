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
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	_ "github.com/lib/pq"
	"golang.org/x/sync/errgroup"
)

const (
	maxQueryTime        = 10 * time.Second
	mbInBytes    uint64 = 1024 * 1024
)

type diskUsageTestCase struct {
	name             string
	ratioDiskFilled  map[int]float64
	diskEmptyInBytes map[int]uint64
	singleKeyOpProb  float64
}

func (tc *diskUsageTestCase) numNodes() (numNodes int) {
	if tc.ratioDiskFilled != nil {
		numNodes = len(tc.ratioDiskFilled)
	} else {
		numNodes = len(tc.diskEmptyInBytes)
	}
	return
}

func runDiskUsage(t *test, c *cluster, duration time.Duration, tc diskUsageTestCase) {
	numNodes := tc.numNodes()
	ctx := context.Background()
	c.Put(ctx, workload, "./workload", c.Node(numNodes))
	c.Put(ctx, cockroach, "./cockroach", c.All())
	fillBallast := filepath.Join(filepath.Dir(workload), "fill_ballast")
	fillBallastBinary := "./fill_ballast"
	c.Put(ctx, fillBallast, fillBallastBinary, c.All())
	c.Start(ctx, c.All())

	loadDuration := " --duration=" + (duration / 2).String()

	workloads := []string{
		"./workload run kv --max-rate 500 --tolerate-errors --init" + loadDuration + " {pgurl:1-%d}",
	}

	m, ctxWG := errgroup.WithContext(ctx)
	for i, cmd := range workloads {
		cmd := cmd // copy is important for goroutine
		i := i     // ditto

		cmd = fmt.Sprintf(cmd, nodes)
		m.Go(func() error {
			quietL, err := newLogger(cmd, strconv.Itoa(i), "workload"+strconv.Itoa(i), ioutil.Discard, os.Stderr)
			if err != nil {
				return err
			}
			return c.RunL(ctxWG, quietL, c.Node(numNodes), cmd)
		})
	}

	if err := m.Wait(); err != nil {
		t.Fatal(err)
	}
	ballastFilePath := "data/ballast_file_to_fill_store"
	for i := 1; i <= numNodes; i++ {
		fillBallastCommand := []string{fillBallastBinary, "--ballast_file", ballastFilePath}
		if tc.ratioDiskFilled != nil {
			fillBallastCommand = append(fillBallastCommand, "--fill_ratio", fmt.Sprint(tc.ratioDiskFilled[i]))
		}
		if tc.diskEmptyInBytes != nil {
			fillBallastCommand = append(fillBallastCommand, "--disk_left_bytes", fmt.Sprint(tc.diskEmptyInBytes[i-1]))
		}
		if err := c.RunE(ctx, c.Node(i), fillBallastCommand...); err != nil {
			c.l.printf("Failed to create ballast file on node %d due to error: %v\n", i, err)
		}
	}

	dbOne := c.Conn(ctx, 1)
	defer dbOne.Close()
	run := func(stmt string) {
		t.Status(stmt)
		_, err := dbOne.ExecContext(ctx, stmt)
		if err != nil {
			t.Fatal(err)
		}
	}

	diskUsages := func() []int {
		diskUsageOnNodes := make([]int, numNodes)
		for i := 1; i <= numNodes; i++ {
			var err error
			diskUsageOnNodes[i-1], err = getDiskUsageInByte(ctx, c, i)
			if err != nil {
				t.Fatalf("Failed to get disk usage on node %d, error: %v", i, err)
			}
		}
		return diskUsageOnNodes
	}

	const stmtZone = "CONFIGURE ZONE 'gc: {ttlseconds: 10}'"
	run(stmtZone)

	t.Status("Starting inserts and deletes")
	m2, ctxWorkLoad := errgroup.WithContext(ctx)
	for j := 3; j <= numNodes; j++ {
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
						// Simulate hot row update loop
						k = 0
					} else {
						k = int64(j)*int64(math.MaxInt32) + int64(count)
					}
					ctxQuery, cancelQuery1 := context.WithTimeout(ctxWorkLoad, maxQueryTime)
					defer cancelQuery1()
					bytes := make([]byte, 10)
					_, err := rand.Read(bytes)
					if err != nil {
						c.l.printf("Failed to generate random bytes, error: %v\n", err)
						bytes = []byte{1, 2}
					}
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
			quietL, _ := newLogger("disk_usage", strconv.Itoa(i), "", ioutil.Discard, os.Stderr)
			output, err := c.RunWithBuffer(ctx, quietL, c.Node(i), "df", "-h", "data/")
			if err != nil {
				c.l.printf("Failed to run df on node %d due to error: %v\n", i, err)
			} else {
				c.l.printf("node %d, df output:\n %s\n", i, string(output))
			}
		}
	}

	var lastDiskUsages []int
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
				if i < 2 {
					lastDiskUsages = diskUsages()
				} else {
					currentDiskUsages := diskUsages()
					const maxIncreaseInDiskUsageAllowed = int(10 * mbInBytes)
					for j := 0; j < numNodes; j++ {
						increaseInDiskUsage := currentDiskUsages[j] - lastDiskUsages[j]
						if increaseInDiskUsage > maxIncreaseInDiskUsageAllowed {
							t.Fatalf(
								"Difference between disk usage since last checked %d is more than max allowed %d",
								increaseInDiskUsage,
								maxIncreaseInDiskUsageAllowed,
							)
						}
					}
					lastDiskUsages = currentDiskUsages
				}
				i++
			}
		}
	}()

	if err := m2.Wait(); err != nil {
		t.Fatal(err)
	}
	close(done)
	printDiskUsages()
	for i := 1; i <= numNodes; i++ {
		if err := c.RunE(
			ctx,
			c.Node(i),
			"rm",
			ballastFilePath,
		); err != nil {
			c.l.printf("Failed to remove ballast file on node %d due to error: %v\n", i, err)
		}
	}
}

func registerDiskUsage(r *registry) {
	duration := 20 * time.Minute
	const seventyMB = uint64(70) * mbInBytes

	testCases := []diskUsageTestCase{
		// Check the behaviour when disk space left is of around one range
		{
			// The workload involves both hot row update and mix rows with equal probability
			name:             "update_mix_hot_key",
			diskEmptyInBytes: map[int]uint64{1: seventyMB, 2: seventyMB, 3: seventyMB, 4: seventyMB},
			singleKeyOpProb:  0.5,
		},
		{
			// The workload involves only update to a single row (hot row update)
			name:             "update_hot_key",
			diskEmptyInBytes: map[int]uint64{1: seventyMB, 2: seventyMB, 3: seventyMB, 4: seventyMB},
			singleKeyOpProb:  1.0,
		},
		{
			// The workload involves only update to a random mix of rows
			name:             "update_mix",
			diskEmptyInBytes: map[int]uint64{1: seventyMB, 2: seventyMB, 3: seventyMB, 4: seventyMB},
			singleKeyOpProb:  0.0,
		},
		// Check the behaviour when disk space left is around 90%. This is to trigger
		// specific logic in cockroach that uses specific %age of disk used(Mainly rebalancing).
		{
			// The workload involves both hot row update and mix rows with equal probability
			name:            "update_mix_hot_key_percent",
			ratioDiskFilled: map[int]float64{1: 0.9, 2: 0.9, 3: 0.9, 4: 0.9},
			singleKeyOpProb: 0.5,
		},
		{
			// The workload involves only update to a single row (hot row update)
			name:            "update_hot_key_percent",
			ratioDiskFilled: map[int]float64{1: 0.9, 2: 0.9, 3: 0.9, 4: 0.9},
			singleKeyOpProb: 1.0,
		},
		{
			// The workload involves only update to a random mix of rows
			name:            "update_mix_percent",
			ratioDiskFilled: map[int]float64{1: 0.9, 2: 0.9, 3: 0.9, 4: 0.9},
			singleKeyOpProb: 0.0,
		},
	}

	for i := range testCases {
		testCase := testCases[i]
		numNodes := testCase.numNodes()
		r.Add(
			testSpec{
				Name:  fmt.Sprintf("disk_space/nodes=%d/duration=%s/tc=%s", numNodes, duration, testCase.name),
				Nodes: nodes(numNodes),
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
