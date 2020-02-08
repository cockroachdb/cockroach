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
	oltpUpdateIndex
	oltpUpdateNonIndex
	oltpReadOnly
	oltpReadWrite
	oltpWriteOnly

	numSysbenchWorkloads
)

var sysbenchWorkloadName = map[sysbenchWorkload]string{
	oltpDelete:         "oltp_delete",
	oltpInsert:         "oltp_insert",
	oltpPointSelect:    "oltp_point_select",
	oltpUpdateIndex:    "oltp_update_index",
	oltpUpdateNonIndex: "oltp_update_non_index",
	oltpReadOnly:       "oltp_read_only",
	oltpReadWrite:      "oltp_read_write",
	oltpWriteOnly:      "oltp_write_only",
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

func (o *sysbenchOptions) cmd(haproxy bool) string {
	pghost := "{pghost:1}"
	if haproxy {
		pghost = "127.0.0.1"
	}
	return fmt.Sprintf(`sysbench \
		--db-driver=pgsql \
		--pgsql-host=%s \
		--pgsql-port=26257 \
		--pgsql-user=root \
		--pgsql-password= \
		--pgsql-db=sysbench \
		--report-interval=1 \
		--time=%d \
		--threads=%d \
		--tables=%d \
		--table_size=%d \
		--auto_inc=false \
		%s`,
		pghost,
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
	waitForFullReplication(t, c.Conn(ctx, allNodes[0]))

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
		c.Run(ctx, loadNode, opts.cmd(false /* haproxy */)+" prepare")

		t.Status("running workload")
		c.Run(ctx, loadNode, opts.cmd(true /* haproxy */)+" run")
		return nil
	})
	m.Wait()
}

func registerSysbench(r *testRegistry) {
	for w := sysbenchWorkload(0); w < numSysbenchWorkloads; w++ {
		const n = 3
		const cpus = 32
		const conc = 4 * cpus
		opts := sysbenchOptions{
			workload:     w,
			duration:     10 * time.Minute,
			concurrency:  conc,
			tables:       10,
			rowsPerTable: 10000000,
		}

		r.Add(testSpec{
			Name:    fmt.Sprintf("sysbench/%s/nodes=%d/cpu=%d/conc=%d", w, n, cpus, conc),
			Owner:   OwnerKV,
			Cluster: makeClusterSpec(n+1, cpu(cpus)),
			Run: func(ctx context.Context, t *test, c *cluster) {
				runSysbench(ctx, t, c, opts)
			},
		})
	}
}
