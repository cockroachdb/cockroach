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

	"github.com/pkg/errors"
)

// performanceExpectations is a map from workload to a map from core count to
// expected throughput below which we consider the test to have failed.
var performanceExpectations = map[string]map[int]float64{
	// The below numbers are minimum expectations based on historical data.
	"A": {8: 2000},
	"B": {8: 15000},
}

func getPerformanceExpectation(wl string, cpus int) (float64, bool) {
	m, exists := performanceExpectations[cloud]
	if !exists {
		return 0, false
	}
	expected, exists := m[cpus]
	return expected, exists
}

func parseThroughputFromOutput(output string) (opsPerSec float64, _ error) {
	prefix := "__result\n" // this string precedes the cumulative results
	idx := strings.LastIndex(output, prefix)
	if idx == -1 {
		return 0, fmt.Errorf("failed to find %q in output", prefix)
	}
	return strconv.ParseFloat(strings.Fields(output[idx+len(prefix):])[3], 64)
}

func registerYCSB(r *registry) {
	runYCSB := func(ctx context.Context, t *test, c *cluster, wl string, cpus int) {
		nodes := c.nodes - 1

		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Put(ctx, workload, "./workload", c.Node(nodes+1))
		c.Start(ctx, t, c.Range(1, nodes))

		t.Status("running workload")
		m := newMonitor(ctx, c, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			ramp := " --ramp=" + ifLocal("0s", "1m")
			duration := " --duration=" + ifLocal("10s", "10m")
			cmd := fmt.Sprintf(
				"./workload run ycsb --init --initial-rows=1000000 --splits=100"+
					" --workload=%s --concurrency=64 --histograms=logs/stats.json"+
					ramp+duration+" {pgurl:1-%d}",
				wl, nodes)
			out, err := c.RunWithBuffer(ctx, t.l, c.Node(nodes+1), cmd)
			if err != nil {
				return errors.Wrapf(err, "failed with output %q", string(out))
			}
			if expected, ok := getPerformanceExpectation(wl, cpus); ok {
				throughput, err := parseThroughputFromOutput(string(out))
				if err != nil {
					return err
				}
				t.debugEnabled = teamCity
				if throughput < expected {
					return fmt.Errorf("%v failed to meet throughput expectations: "+
						"observed %v, expected at least %v", t.Name(), expected, throughput)
				}
				c.l.Printf("Observed throughput of %v > %v", throughput, expected)
			}
			return nil
		})
		m.Wait()
	}

	for _, wl := range []string{"A", "B", "C", "D", "E", "F"} {
		if wl == "D" || wl == "E" {
			// These workloads are currently unsupported by workload.
			// See TODOs in workload/ycsb/ycsb.go.
			continue
		}
		for _, cpus := range []int{8, 32} {
			var name string
			if cpus == 8 { // support legacy test name which didn't include cpu
				name = fmt.Sprintf("ycsb/%s/nodes=3", wl)
			} else {
				name = fmt.Sprintf("ycsb/%s/nodes=3/cpu=%d", wl, cpus)
			}
			wl, cpus := wl, cpus
			r.Add(testSpec{
				Name:    name,
				Cluster: makeClusterSpec(4, cpu(cpus)),
				Run: func(ctx context.Context, t *test, c *cluster) {
					runYCSB(ctx, t, c, wl, cpus)
				},
			})
		}
	}
}
