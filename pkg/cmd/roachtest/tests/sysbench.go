// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
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

func runSysbench(ctx context.Context, t test.Test, c cluster.Cluster, opts sysbenchOptions) {
	allNodes := c.Range(1, c.Spec().NodeCount)
	roachNodes := c.Range(1, c.Spec().NodeCount-1)
	loadNode := c.Node(c.Spec().NodeCount)

	t.Status("installing cockroach")
	c.Put(ctx, t.Cockroach(), "./cockroach", allNodes)
	c.Start(ctx, roachNodes)
	WaitFor3XReplication(t, c.Conn(ctx, allNodes[0]))

	t.Status("installing haproxy")
	if err := c.Install(ctx, loadNode, "haproxy"); err != nil {
		t.Fatal(err)
	}
	c.Run(ctx, loadNode, "./cockroach gen haproxy --insecure --url {pgurl:1}")
	c.Run(ctx, loadNode, "haproxy -f haproxy.cfg -D")

	t.Status("installing sysbench")
	if err := c.Install(ctx, loadNode, "sysbench"); err != nil {
		t.Fatal(err)
	}

	m := c.NewMonitor(ctx, roachNodes)
	m.Go(func(ctx context.Context) error {
		t.Status("preparing workload")
		c.Run(ctx, c.Node(1), `./cockroach sql --insecure -e "CREATE DATABASE sysbench"`)
		c.Run(ctx, loadNode, opts.cmd(false /* haproxy */)+" prepare")

		t.Status("running workload")
		err := c.RunE(ctx, loadNode, opts.cmd(true /* haproxy */)+" run")
		// Sysbench occasionally segfaults. When that happens, don't fail the
		// test.
		if err != nil && !strings.Contains(err.Error(), "Segmentation fault") {
			return err
		}
		t.L().Printf("sysbench segfaulted; passing test anyway")
		return nil
	})
	m.Wait()
}

func registerSysbench(r registry.Registry) {
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

		r.Add(registry.TestSpec{
			Name:    fmt.Sprintf("sysbench/%s/nodes=%d/cpu=%d/conc=%d", w, n, cpus, conc),
			Owner:   registry.OwnerKV,
			Cluster: r.MakeClusterSpec(n+1, spec.CPU(cpus)),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runSysbench(ctx, t, c, opts)
			},
		})
	}
}
