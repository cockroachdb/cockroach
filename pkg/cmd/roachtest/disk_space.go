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
	"os"
	"time"

	"strconv"

	"path/filepath"

	"io/ioutil"

	"math"

	_ "github.com/lib/pq"
	"golang.org/x/sync/errgroup"
)

func runDiskUsage(t *test, c *cluster, nodes int, duration time.Duration) {
	ctx := context.Background()
	c.Put(ctx, workload, "./workload", c.Node(nodes))
	c.Put(ctx, cockroach, "./cockroach", c.All())
	fill_ballast := filepath.Join(filepath.Dir(workload), "fill_ballast")
	c.Put(ctx, fill_ballast, "./fill_ballast", c.All())
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
			return c.RunL(ctxWG, quietL, c.Node(nodes), cmd)
		})
	}

	if err := m.Wait(); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= nodes; i++ {
		if err := c.RunE(
			ctx,
			c.Node(i),
			"./fill_ballast", "--data_directory", "data/", "--fill_ratio", "0.9",
		); err != nil {
			t.Status(err)
		}
	}
	m2, ctxWL := errgroup.WithContext(ctx)
	for j := 1; j <= nodes; j++ {
		nodeId := j
		m2.Go(
			func() error {
				db := c.Conn(ctxWL, nodeId)
				start := time.Now()
				count := 0
				for time.Since(start) < (duration / 2) {
					k := int64(j)*int64(math.MaxInt32) + int64(count)
					if _, err := db.ExecContext(ctxWL, "UPSERT INTO kv.kv(k, v) Values($1, $2)", k, []byte{1}); err != nil {
						return err
					}
					if _, err := db.ExecContext(ctxWL, "DELETE FROM kv.kv WHERE k=$1", k); err != nil {
						return err
					}
					count++
				}
				return nil
			},
		)
	}
	if err := m2.Wait(); err != nil {
		t.Fatal(err)
	}
	for j := 1; j <= nodes; j++ {
		quietL, _ := newLogger("disk_usage", strconv.Itoa(j), "", ioutil.Discard, os.Stderr)
		output, err := c.RunWithBuffer(ctx, quietL, c.Node(j), "df", "-h", "data/")
		fmt.Println("df output\n", string(output), err)
	}
}

func registerDiskUsage(r *registry) {
	const numNodes = 4
	duration := time.Hour

	r.Add(testSpec{
		Name:  fmt.Sprintf("disk_space/nodes=%d/duration=%s", numNodes, duration),
		Nodes: nodes(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			if local {
				duration = 3 * time.Minute
				fmt.Printf("running with duration=%s in local mode\n", duration)
			}
			runDiskUsage(t, c, numNodes, duration)
		},
	})
}
