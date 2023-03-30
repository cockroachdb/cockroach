// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type tpceSpec struct {
	loadNode         int
	roachNodes       option.NodeListOption
	roachNodeIPFlags []string
}

type tpceCmdOptions struct {
	customers int
	racks     int
	duration  time.Duration
	threads   int
}

func (to tpceCmdOptions) AddCommandOptions(cmd *roachtestutil.Command) {
	cmd.MaybeFlag(to.customers != 0, "customers", to.customers)
	cmd.MaybeFlag(to.racks != 0, "racks", to.racks)
	cmd.MaybeFlag(to.duration != 0, "duration", to.duration)
	cmd.MaybeFlag(to.threads != 0, "threads", to.threads)
}

func initTPCESpec(
	ctx context.Context,
	l *logger.Logger,
	c cluster.Cluster,
	loadNode int,
	roachNodes option.NodeListOption,
) (*tpceSpec, error) {
	l.Printf("Installing docker")
	if err := c.Install(ctx, l, c.Nodes(loadNode), "docker"); err != nil {
		return nil, err
	}
	roachNodeIPs, err := c.InternalIP(ctx, l, roachNodes)
	if err != nil {
		return nil, err
	}
	roachNodeIPFlags := make([]string, len(roachNodeIPs))
	for i, ip := range roachNodeIPs {
		roachNodeIPFlags[i] = fmt.Sprintf("--hosts=%s", ip)
	}
	return &tpceSpec{
		loadNode:         loadNode,
		roachNodes:       roachNodes,
		roachNodeIPFlags: roachNodeIPFlags,
	}, nil
}

func (ts *tpceSpec) newCmd(o tpceCmdOptions) *roachtestutil.Command {
	cmd := roachtestutil.NewCommand(`sudo docker run cockroachdb/tpc-e:latest`)
	o.AddCommandOptions(cmd)
	return cmd
}

// init initializes an empty cluster with a tpce database. This includes a bulk
// import of the data and schema creation.
func (ts *tpceSpec) init(ctx context.Context, t test.Test, c cluster.Cluster, o tpceCmdOptions) {
	cmd := ts.newCmd(o).Flag("init", "")
	c.Run(ctx, c.Node(ts.loadNode), fmt.Sprintf("%s %s", cmd, ts.roachNodeIPFlags[0]))
}

// run runs the tpce workload on cluster that has been initialized with the tpce schema.
func (ts *tpceSpec) run(
	ctx context.Context, t test.Test, c cluster.Cluster, o tpceCmdOptions,
) (install.RunResultDetails, error) {
	cmd := fmt.Sprintf("%s %s", ts.newCmd(o), strings.Join(ts.roachNodeIPFlags, " "))
	return c.RunWithDetailsSingleNode(ctx, t.L(), c.Node(ts.loadNode), cmd)
}

func registerTPCE(r registry.Registry) {
	type tpceOptions struct {
		owner     registry.Owner // defaults to test-eng
		customers int
		nodes     int
		cpus      int
		ssds      int

		tags    []string
		timeout time.Duration
	}

	runTPCE := func(ctx context.Context, t test.Test, c cluster.Cluster, opts tpceOptions) {
		roachNodes := c.Range(1, opts.nodes)
		loadNode := opts.nodes + 1
		racks := opts.nodes

		t.Status("installing cockroach")
		c.Put(ctx, t.Cockroach(), "./cockroach", roachNodes)

		startOpts := option.DefaultStartOpts()
		startOpts.RoachprodOpts.StoreCount = opts.ssds
		settings := install.MakeClusterSettings(install.NumRacksOption(racks))
		c.Start(ctx, t.L(), startOpts, settings, roachNodes)

		tpceSpec, err := initTPCESpec(ctx, t.L(), c, loadNode, roachNodes)
		require.NoError(t, err)

		// Configure to increase the speed of the import.
		func() {
			db := c.Conn(ctx, t.L(), 1)
			defer db.Close()
			if _, err := db.ExecContext(
				ctx, "SET CLUSTER SETTING kv.bulk_io_write.concurrent_addsstable_requests = $1", 4*opts.ssds,
			); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(
				ctx, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false",
			); err != nil {
				t.Fatal(err)
			}
		}()

		m := c.NewMonitor(ctx, roachNodes)
		m.Go(func(ctx context.Context) error {

			t.Status("preparing workload")
			tpceSpec.init(ctx, t, c, tpceCmdOptions{
				customers: opts.customers,
				racks:     racks,
			})

			t.Status("running workload")
			result, err := tpceSpec.run(ctx, t, c, tpceCmdOptions{
				customers: opts.customers,
				racks:     racks,
				duration:  2 * time.Hour,
				threads:   opts.nodes * opts.cpus,
			})
			if err != nil {
				t.Fatal(err.Error())
			}
			t.L().Printf("workload output:\n%s\n", result.Stdout)
			if strings.Contains(result.Stdout, "Reported tpsE :    --   (not between 80% and 100%)") {
				return errors.New("invalid tpsE fraction")
			}
			return nil
		})
		m.Wait()
	}

	for _, opts := range []tpceOptions{
		// Nightly, small scale configurations.
		{customers: 5_000, nodes: 3, cpus: 4, ssds: 1, timeout: 4 * time.Hour},
		// Weekly, large scale configurations.
		{customers: 100_000, nodes: 5, cpus: 32, ssds: 2, tags: []string{"weekly"}, timeout: 36 * time.Hour},
	} {
		opts := opts
		owner := registry.OwnerTestEng
		if opts.owner != "" {
			owner = opts.owner
		}
		r.Add(registry.TestSpec{
			Name:    fmt.Sprintf("tpce/c=%d/nodes=%d", opts.customers, opts.nodes),
			Owner:   owner,
			Tags:    opts.tags,
			Timeout: opts.timeout,
			Cluster: r.MakeClusterSpec(opts.nodes+1, spec.CPU(opts.cpus), spec.SSD(opts.ssds)),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runTPCE(ctx, t, c, opts)
			},
		})
	}
}
