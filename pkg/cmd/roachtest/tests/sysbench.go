// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
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
	pgport := "{pgport:1}"
	if haproxy {
		pghost = "127.0.0.1"
		pgport = "26257"
	}
	return fmt.Sprintf(`sysbench \
		--db-driver=pgsql \
		--pgsql-host=%s \
		--pgsql-port=%s \
		--pgsql-user=%s \
		--pgsql-password=%s \
		--pgsql-db=sysbench \
		--report-interval=1 \
		--time=%d \
		--threads=%d \
		--tables=%d \
		--table_size=%d \
		--auto_inc=false \
		%s`,
		pghost,
		pgport,
		install.DefaultUser,
		install.DefaultPassword,
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
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), roachNodes)
	err := WaitFor3XReplication(ctx, t, c.Conn(ctx, t.L(), allNodes[0]))
	require.NoError(t, err)

	t.Status("installing haproxy")
	if err = c.Install(ctx, t.L(), loadNode, "haproxy"); err != nil {
		t.Fatal(err)
	}
	// cockroach gen haproxy does not support specifying a non root user
	pgurl, err := roachprod.PgURL(ctx, t.L(), c.MakeNodes(c.Node(1)), install.CockroachNodeCertsDir, roachprod.PGURLOptions{
		External: true,
		Auth:     install.AuthRootCert,
		Secure:   c.IsSecure(),
	})
	if err != nil {
		t.Fatal(err)
	}
	c.Run(ctx, loadNode, fmt.Sprintf("./cockroach gen haproxy --url %s", pgurl[0]))
	c.Run(ctx, loadNode, "haproxy -f haproxy.cfg -D")

	t.Status("installing sysbench")
	if err := c.Install(ctx, t.L(), loadNode, "sysbench"); err != nil {
		t.Fatal(err)
	}

	m := c.NewMonitor(ctx, roachNodes)
	m.Go(func(ctx context.Context) error {
		t.Status("preparing workload")
		c.Run(ctx, c.Node(1), `./cockroach sql --url={pgurl:1} -e "CREATE DATABASE sysbench"`)
		c.Run(ctx, loadNode, opts.cmd(false /* haproxy */)+" prepare")

		t.Status("running workload")
		cmd := opts.cmd(true /* haproxy */) + " run"
		result, err := c.RunWithDetailsSingleNode(ctx, t.L(), loadNode, cmd)

		// Sysbench occasionally segfaults. When that happens, don't fail the
		// test.
		if result.RemoteExitStatus == errors.SegmentationFaultExitCode {
			t.L().Printf("sysbench segfaulted; passing test anyway")
			return nil
		}

		if err != nil {
			return err
		}

		return nil
	})
	m.Wait()
}

func registerSysbench(r registry.Registry) {
	for w := sysbenchWorkload(0); w < numSysbenchWorkloads; w++ {
		const n = 3
		const cpus = 32
		const conc = 8 * cpus
		opts := sysbenchOptions{
			workload:     w,
			duration:     10 * time.Minute,
			concurrency:  conc,
			tables:       10,
			rowsPerTable: 10000000,
		}

		r.Add(registry.TestSpec{
			Name:             fmt.Sprintf("sysbench/%s/nodes=%d/cpu=%d/conc=%d", w, n, cpus, conc),
			Owner:            registry.OwnerTestEng,
			Cluster:          r.MakeClusterSpec(n+1, spec.CPU(cpus)),
			CompatibleClouds: registry.AllExceptAWS,
			Suites:           registry.Suites(registry.Nightly),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runSysbench(ctx, t, c, opts)
			},
		})
	}
}
