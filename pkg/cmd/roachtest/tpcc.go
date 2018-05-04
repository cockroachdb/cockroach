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
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/color"
	"github.com/cockroachdb/cockroach/pkg/util/search"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

func registerTPCC(r *registry) {
	runTPCC := func(ctx context.Context, t *test, c *cluster, warehouses int, extra string) {
		nodes := c.nodes - 1

		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Put(ctx, workload, "./workload", c.Node(nodes+1))
		c.Start(ctx, c.Range(1, nodes))

		t.Status("running workload")
		m := newMonitor(ctx, c, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			duration := " --duration=" + ifLocal("10s", "10m")
			cmd := fmt.Sprintf(
				"./workload run tpcc --init --warehouses=%d"+
					extra+duration+" {pgurl:1-%d}",
				warehouses, nodes)
			c.Run(ctx, c.Node(nodes+1), cmd)
			return nil
		})
		m.Wait()
	}

	r.Add(testSpec{
		Name:  "tpcc/w=1/nodes=3",
		Nodes: nodes(4),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCC(ctx, t, c, 1, " --wait=false")
		},
	})
	r.Add(testSpec{
		Name:  "tpmc/w=1/nodes=3",
		Nodes: nodes(4),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCC(ctx, t, c, 1, "")
		},
	})
}

func registerTPCCBench(r *registry) {
	// TODO(nvanbenschoten): give this a description and include tips for
	// running it repeatedly. For instance, using a combination of
	// `--cluster=<cluster>` and `--wipe=false` flags allows it to be run
	// repeatedly without the need to create a new cluster or restore the
	// dataset each time. This allows for quick iteration on experimental
	// changes.
	runTPCCBench := func(ctx context.Context, t *test, c *cluster) {
		// Disable write barrier on mounted SSDs.
		if !c.isLocal() {
			c.Run(ctx, c.All(),
				"sudo", "umount", "/mnt/data1", ";",
				"sudo", "mount", "-o", "discard,defaults,nobarrier",
				"/dev/disk/by-id/google-local-ssd-0", "/mnt/data1")
		}

		roachNodeCount := c.nodes - 1
		roachNodes := c.Range(1, roachNodeCount)
		loadNode := c.Node(c.nodes)

		c.Put(ctx, cockroach, "./cockroach", roachNodes)
		c.Put(ctx, workload, "./workload", loadNode)
		c.Start(ctx, roachNodes)

		var m *errgroup.Group // see comment in version.go
		m, ctx = errgroup.WithContext(ctx)
		m.Go(func() error {
			const maxWarehouses = 1000
			if err := func() error {
				t.Status("setting up dataset")
				db := c.Conn(ctx, 1)
				defer db.Close()

				// Check if the dataset already exists and is already large
				// enough to accommodate this benchmarking. if so, we can
				// skip the fixture RESTORE.
				var tableExists bool
				err := db.QueryRowContext(ctx,
					`SELECT COUNT(*) > 0 FROM INFORMATION_SCHEMA.TABLES
					 WHERE TABLE_CATALOG = 'tpcc' AND TABLE_NAME = 'warehouse'`).Scan(&tableExists)
				if err != nil {
					return err
				}
				if tableExists {
					var curWarehouses int
					err := db.QueryRowContext(ctx,
						`SELECT COUNT(*) FROM tpcc.warehouse`).Scan(&curWarehouses)
					if err != nil {
						return err
					}
					if curWarehouses >= maxWarehouses {
						return nil
					}

					// If the dataset exists but is not large enough, wipe the
					// cluster before restoring.
					c.Wipe(ctx, roachNodes)
					c.Start(ctx, roachNodes)
				}

				cmd := fmt.Sprintf(
					"./workload fixtures load tpcc --checks=false --warehouses=%d {pgurl:1}",
					maxWarehouses)
				return c.RunE(ctx, loadNode, cmd)
			}(); err != nil {
				return err
			}

			// Search between 1 and maxWarehouses for the largest number of
			// warehouses that can be operated on while sustaining a throughput
			// threshold, set to a fraction of max tpmC.
			const estimate = 350
			const stepSize = 5
			const precision = 5
			s := search.NewLineSearcher(1, maxWarehouses, estimate, stepSize, precision)
			res, err := s.Search(func(warehouses int) (bool, error) {
				// Restart the cluster before each iteration to help eliminate
				// inter-trial interactions.
				c.Stop(ctx, roachNodes)
				c.Start(ctx, roachNodes)
				time.Sleep(10 * time.Second)

				t.Status(fmt.Sprintf("running benchmark, warehouses=%d", warehouses))
				cmd := fmt.Sprintf(
					"./workload run tpcc --warehouses=%d --ramp=30s "+
						"--duration=3m --split --scatter {pgurl:1-%d}",
					warehouses, roachNodeCount)
				out, err := c.RunWithBuffer(ctx, c.l, loadNode, cmd)
				if err != nil {
					return false, err
				}

				// Parse the stats header and stats lines from the output.
				str := string(out)
				lines := strings.Split(str, "\n")
				for i, line := range lines {
					if strings.Contains(line, "tpmC") {
						lines = lines[i:]
					}
					if i == len(lines)-1 {
						return false, errors.New("tpmC not found in output")
					}
				}
				headerLine, statsLine := lines[0], lines[1]
				c.l.printf("%s\n%s\n", headerLine, statsLine)

				// Parse tpmC value from stats line.
				fields := strings.Fields(statsLine)
				tpmC, err := strconv.ParseFloat(fields[1], 64)
				if err != nil {
					return false, err
				}

				// Determine the fraction of the maximum possible tpmC realized.
				maxTpmC := 12.8 * float64(warehouses)
				tpmCRatio := tpmC / maxTpmC

				// Determine whether this means the test passed or not.
				const passRatio = 0.85
				pass := tpmCRatio > passRatio

				// Print the result.
				color.Stdout(color.Green)
				passStr := "PASS"
				if !pass {
					color.Stdout(color.Red)
					passStr = "FAIL"
				}
				c.l.printf("--- %s: tpcc %d resulted in %.1f tpmC (%.1f%% of max tpmC)\n\n",
					passStr, warehouses, tpmC, tpmCRatio*100)
				color.Stdout(color.Reset)

				return pass, nil
			})
			if err != nil {
				return err
			}

			color.Stdout(color.Green)
			c.l.printf("------\nMAX WAREHOUSES = %d\n------\n\n", res)
			color.Stdout(color.Reset)
			return nil
		})
		if err := m.Wait(); err != nil {
			t.Fatal(err)
		}
	}

	r.Add(testSpec{
		Name:  "tpccbench/nodes=3",
		Nodes: nodes(4),
		Run:   runTPCCBench,
	})
	// TODO(nvanbenschoten): add more configs here, including:
	// - different size clusters
	// - with chaos
	// - with distributed load generator
}
