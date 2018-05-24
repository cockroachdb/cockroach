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

	"github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/color"
	"github.com/cockroachdb/cockroach/pkg/util/search"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
		Name:   "tpcc/w=1/nodes=3",
		Nodes:  nodes(4),
		Stable: true, // DO NOT COPY to new tests
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCC(ctx, t, c, 1, " --wait=false")
		},
	})
	r.Add(testSpec{
		Name:   "tpmc/w=1/nodes=3",
		Nodes:  nodes(4),
		Stable: true, // DO NOT COPY to new tests
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCC(ctx, t, c, 1, "")
		},
	})

	// Run a single tpccbench spec in CI.
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  4,
		// TODO(m-schneider): enable when geo-distributed benchmarking is supported.
		// Chaos: true,
		// Geo: true,

		LoadWarehouses: 1000,
		EstimatedMax:   300,
	})
}

type tpccBenchSpec struct {
	Nodes int
	CPUs  int
	Chaos bool
	Geo   bool

	// The number of warehouses to load into the cluster before beginning
	// benchmarking. Should be larger than EstimatedMax and should be a
	// value that is unlikely to be achievable.
	LoadWarehouses int
	// An estimate of the maximum number of warehouses achievable in the
	// cluster config. The closer this is to the actual max achievable
	// warehouse count, the faster the benchmark will be in producing a
	// result. This can be adjusted over time as performance characteristics
	// change (i.e. CockroachDB gets faster!).
	EstimatedMax int
}

func registerTPCCBenchSpec(r *registry, b tpccBenchSpec) {
	nameParts := []string{
		"tpccbench",
		fmt.Sprintf("nodes=%d", b.Nodes),
		fmt.Sprintf("cpu=%d", b.CPUs),
	}
	if b.Chaos {
		nameParts = append(nameParts, "chaos")
	}
	if b.Geo {
		nameParts = append(nameParts, "geo")
	}
	name := strings.Join(nameParts, "/")

	opts := []createOption{cpu(b.CPUs)}
	if b.Geo {
		opts = append(opts, geo())
	}
	nodes := nodes(b.Nodes+1, opts...)

	r.Add(testSpec{
		Name:  name,
		Nodes: nodes,
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCCBench(ctx, t, c, b)
		},
	})
}

// loadTPCCBench loads a TPCC dataset for the specific benchmark spec. The
// function is idempotent and first checks whether a compatible dataset exists,
// performing an expensive dataset restore only if it doesn't.
func loadTPCCBench(
	ctx context.Context, c *cluster, roachNodes, loadNode nodeListOption, warehouses int,
) error {
	db := c.Conn(ctx, 1)
	defer db.Close()

	// Check if the dataset already exists and is already large enough to
	// accommodate this benchmarking. If so, we can skip the fixture RESTORE.
	if _, err := db.ExecContext(ctx, `USE tpcc`); err == nil {
		c.l.printf("found existing tpcc database\n")

		var curWarehouses int
		if err := db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM tpcc.warehouse`,
		).Scan(&curWarehouses); err != nil {
			return err
		}
		if curWarehouses >= warehouses {
			// The cluster has enough warehouses. Nothing to do.
			return nil
		}

		// If the dataset exists but is not large enough, wipe the cluster
		// before restoring.
		c.Wipe(ctx, roachNodes)
		c.Start(ctx, roachNodes)
	} else if pqErr, ok := err.(*pq.Error); !ok ||
		string(pqErr.Code) != pgerror.CodeInvalidCatalogNameError {
		return err
	}

	// Load the corresponding fixture.
	cmd := fmt.Sprintf(
		"./workload fixtures load tpcc --checks=false --warehouses=%d {pgurl:1}", warehouses)
	if err := c.RunE(ctx, loadNode, cmd); err != nil {
		return err
	}

	// Split and scatter the tables. Set duration to 1ms so that the load
	// generation doesn't actually run.
	cmd = fmt.Sprintf(
		"./workload run tpcc --warehouses=%d --split --scatter "+
			"--duration=1ms {pgurl:1}", warehouses)
	return c.RunE(ctx, loadNode, cmd)
}

// tpccbench is a suite of benchmarking tools that run TPC-C against CockroachDB
// clusters in different configurations. The tools search for the maximum number
// of warehouses that a load generator can run TPC-C against while still
// maintaining a minimum acceptable throughput. This maximum warehouse value is
// directly comparable to other runs of the tool in the same cluster config, and
// expresses how well CockroachDB performance scales.
//
// In order to run a benchmark spec, the tool must first load a TPC-C dataset
// large enough to accommodate it. This can take a while, so it is recommended
// to use a combination of `--cluster=<cluster>` and `--wipe=false` flags to
// limit the loading phase to the first run of the tool. Subsequent runs will be
// able to avoid the dataset restore as long as they are not wiped. This allows
// for quick iteration on experimental changes.
//
// It can also be useful to omit the `--cluster` flag during the first run of
// the tool to allow roachtest to create the correct set of VMs required by the
// test. The `--wipe` flag will prevent this cluster from being destroyed, so it
// can then be used during future runs.
func runTPCCBench(ctx context.Context, t *test, c *cluster, b tpccBenchSpec) {
	if b.Geo {
		// TODO(m-schneider): add support for geo-distributed benchmarking.
		t.Fatal("geo-distributed benchmarking not supported")
	}

	roachNodeCount := c.nodes - 1
	roachNodes := c.Range(1, roachNodeCount)
	loadNode := c.Node(c.nodes)

	// Disable write barrier on mounted SSDs.
	if !c.isLocal() {
		c.RemountNoBarrier(ctx)
	}

	c.Put(ctx, cockroach, "./cockroach", roachNodes)
	c.Put(ctx, workload, "./workload", loadNode)
	c.Start(ctx, roachNodes)

	m := newMonitor(ctx, c, roachNodes)
	m.Go(func(ctx context.Context) error {
		t.Status("setting up dataset")
		err := loadTPCCBench(ctx, c, roachNodes, loadNode, b.LoadWarehouses)
		if err != nil {
			return err
		}

		// Search between 1 and b.LoadWarehouses for the largest number of
		// warehouses that can be operated on while sustaining a throughput
		// threshold, set to a fraction of max tpmC.
		precision := b.LoadWarehouses / 200
		initStepSize := precision
		s := search.NewLineSearcher(1, b.LoadWarehouses, b.EstimatedMax, initStepSize, precision)
		res, err := s.Search(func(warehouses int) (bool, error) {
			// Restart the cluster before each iteration to help eliminate
			// inter-trial interactions.
			m.ExpectDeaths(int32(roachNodeCount))
			c.Stop(ctx, roachNodes)
			c.Start(ctx, roachNodes)
			time.Sleep(10 * time.Second)

			// Set up the load geneartion configuration.
			loadDur := 3 * time.Minute
			loadDone := make(chan time.Time, 1)
			extraFlags := ""

			// If we're running chaos in this configuration, modify this config.
			if b.Chaos {
				// Increase the load generation duration.
				loadDur = 5 * time.Minute
				extraFlags = " --tolerate-errors"

				// Kill one node at a time.
				ch := Chaos{
					Timer:   Periodic{Down: 1 * time.Second, Up: 90 * time.Second},
					Target:  roachNodes.randNode,
					Stopper: loadDone,
				}
				m.Go(ch.Runner(c, m))
			}

			t.Status(fmt.Sprintf("running benchmark, warehouses=%d", warehouses))
			cmd := fmt.Sprintf("ulimit -n 32768; "+
				"./workload run tpcc --warehouses=%d --ramp=30s --duration=%s%s {pgurl:1-%d}",
				warehouses, loadDur, extraFlags, roachNodeCount)
			out, err := c.RunWithBuffer(ctx, c.l, loadNode, cmd)
			loadDone <- timeutil.Now()
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
					return false, errors.Errorf("tpmC not found in output:\n\n%s\n", out)
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

			// Determine whether this means the test passed or not. We use a
			// threshold of 85% of max tpmC as the "passing" criteria for a
			// given number of warehouses. This does not take response latencies
			// for different op types into account directly as required by the
			// formal TPC-C spec, but in practice it results in stable results.
			passRatio := 0.85
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
	m.Wait()
	c.Stop(ctx, c.All())
}

func registerTPCCBench(r *registry) {
	specs := []tpccBenchSpec{
		{
			Nodes: 3,
			CPUs:  4,

			LoadWarehouses: 1000,
			EstimatedMax:   325,
		},
		{
			Nodes: 3,
			CPUs:  16,

			LoadWarehouses: 2000,
			EstimatedMax:   1300,
		},
		// objective 1, key result 1 & 2.
		{
			Nodes: 18,
			CPUs:  16,

			LoadWarehouses: 10000,
			EstimatedMax:   5300,
		},
		// objective 2, key result 1.
		{
			Nodes: 7,
			CPUs:  16,
			Chaos: true,

			LoadWarehouses: 5000,
			EstimatedMax:   500,
		},
		// objective 3, key result 2.
		{
			Nodes: 9,
			CPUs:  16,
			Geo:   true,

			LoadWarehouses: 5000,
			EstimatedMax:   2000,
		},
		// objective 4, key result 2.
		{
			Nodes: 64,
			CPUs:  16,

			LoadWarehouses: 50000,
			EstimatedMax:   40000,
		},
	}

	for _, b := range specs {
		registerTPCCBenchSpec(r, b)
	}
}
