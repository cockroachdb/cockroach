// Copyright 2018 The Cockroach Authors.
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
	"context"
	"fmt"
	"time"
)

type sysbenchWorkload int

const (
	oltpDelete sysbenchWorkload = iota
	oltpInsert
	oltpPointSelect
	oltpUpdateNonIndex

	// TODO(nvanbenschoten): transactional workloads are not supported
	// because sysbench does not contain client-side retry loops.
	// oltpReadOnly
	// oltpReadWrite
	// oltpWriteOnly

	numSysbenchWorkloads
)

var sysbenchWorkloadName = map[sysbenchWorkload]string{
	oltpDelete:         "oltp_delete",
	oltpInsert:         "oltp_insert",
	oltpPointSelect:    "oltp_point_select",
	oltpUpdateNonIndex: "oltp_update_non_index",
	// oltpReadOnly:  "oltp_read_only",
	// oltpReadWrite: "oltp_read_write",
	// oltpWriteOnly: "oltp_write_only",
}

func (w sysbenchWorkload) String() string {
	return sysbenchWorkloadName[w]
}

type sysbenchOptions struct {
	workload     sysbenchWorkload
	duration     time.Duration
	concurrency  int
	tables       int
	rowsPerTable int
}

func (o *sysbenchOptions) cmd() string {
	return fmt.Sprintf(`sysbench \
		--db-driver=pgsql \
		--pgsql-host=127.0.0.1 \
		--pgsql-port=26257 \
		--pgsql-user=root \
		--pgsql-password= \
		--pgsql-db=sysbench \
		--report-interval=1 \
		--time=%d \
		--threads=%d \
		--tables=%d \
		--table_size=%d \
		%s`,
		int(o.duration.Seconds()),
		o.concurrency,
		o.tables,
		o.rowsPerTable,
		o.workload,
	)
}

func runSysbench(ctx context.Context, t *test, c *cluster, opts sysbenchOptions) {
	allNodes := c.Range(1, c.spec.NodeCount)
	roachNodes := c.Range(1, c.spec.NodeCount-1)
	loadNode := c.Node(c.spec.NodeCount)

	t.Status("installing cockroach")
	c.Put(ctx, cockroach, "./cockroach", allNodes)
	c.Start(ctx, t, roachNodes)

	t.Status("installing haproxy")
	if err := c.Install(ctx, t.l, loadNode, "haproxy"); err != nil {
		t.Fatal(err)
	}
	c.Run(ctx, loadNode, "./cockroach gen haproxy --insecure --url {pgurl:1}")
	c.Run(ctx, loadNode, "haproxy -f haproxy.cfg -D")

	t.Status("installing sysbench")
	if err := c.Install(ctx, t.l, loadNode, "sysbench"); err != nil {
		t.Fatal(err)
	}

	m := newMonitor(ctx, c, roachNodes)
	m.Go(func(ctx context.Context) error {
		t.Status("preparing workload")
		c.Run(ctx, c.Node(1), `./cockroach sql --insecure -e "CREATE DATABASE sysbench"`)
		c.Run(ctx, loadNode, opts.cmd()+" prepare")

		t.Status("running workload")
		c.Run(ctx, loadNode, opts.cmd()+" run")
		return nil
	})
	m.Wait()
}

func registerSysbench(r *testRegistry) {
	for w := sysbenchWorkload(0); w < numSysbenchWorkloads; w++ {
		const n = 3
		const cpus = 16
		opts := sysbenchOptions{
			workload:     w,
			duration:     10 * time.Minute,
			concurrency:  8 * cpus,
			tables:       4,
			rowsPerTable: 1000000,
		}

		r.Add(testSpec{
			Skip:    "https://github.com/cockroachdb/cockroach/issues/32738",
			Name:    fmt.Sprintf("sysbench/%s/nodes=%d", w, n),
			Cluster: makeClusterSpec(n+1, cpu(cpus)),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runSysbench(ctx, t, c, opts)
			},
		})
	}
}
