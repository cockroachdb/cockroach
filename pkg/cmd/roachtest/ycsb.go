// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"fmt"
	"strconv"
)

func runYCSB(
	ctx context.Context, t *test, c *cluster, wl string, uniform bool, cpus, splits, concurrency int,
) {
	nodes := c.nodes - 1

	c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
	c.Put(ctx, workload, "./workload", c.Node(nodes+1))
	c.Start(ctx, t, c.Range(1, nodes))
	t.Status("running workload")
	m := newMonitor(ctx, c, c.Range(1, nodes))
	m.Go(func(ctx context.Context) error {
		ramp := " --ramp=" + ifLocal("0s", "1m")
		duration := " --duration=" + ifLocal("10s", "10m")
		splits := " --splits=" + strconv.Itoa(splits)
		concurrency := " --concurrency=" + strconv.Itoa(concurrency)
		var distribution string
		if uniform {
			distribution = " --request-distribution=uniform"
		}
		workload := " --workload=" + wl
		cmd := fmt.Sprintf(
			"./workload run ycsb --init --record-count=1000000"+
				" --histograms=logs/stats.json"+
				workload+ramp+duration+splits+concurrency+distribution+" {pgurl:1-%d}",
			nodes)
		c.Run(ctx, c.Node(nodes+1), cmd)
		return nil
	})
	m.Wait()
}

func registerYCSB(r *registry) {
	for _, wl := range []string{"A", "B", "C", "D", "E", "F"} {
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
					const (
						splits      = 100
						concurrency = 64
					)
					runYCSB(ctx, t, c, wl, false /* uniform */, cpus, splits, concurrency)
				},
			})
		}
	}
}

func registerYCSBBenchUniformA(r *registry) {
	const (
		wl      = "A"
		ebsSize = 350 /* GB */
		ebsIOPs = 5 * ebsSize
	)
	for _, cpus := range []int{2, 4, 8, 16, 32} {
		name := fmt.Sprintf("ycsbbench/uniform/%s/nodes=1/cpu=%d", wl, cpus)
		wl, cpus, splits, concurrency := wl, cpus, 16*cpus, 8*cpus
		r.Add(testSpec{
			Name:    name,
			Cluster: makeClusterSpec(2, cpu(cpus), io1EBS(ebsSize, ebsIOPs)),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runYCSB(ctx, t, c, wl, true, cpus, splits, concurrency)
			},
		})
	}
}
