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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type tpceSpec struct {
	loadNode         option.NodeListOption
	roachNodes       option.NodeListOption
	roachNodeIPFlags []string
}

type tpceCmdOptions struct {
	customers int
	racks     int
	duration  time.Duration
	threads   int
}

func (to tpceCmdOptions) String() string {
	var builder strings.Builder
	if to.customers != 0 {
		builder.WriteString(fmt.Sprintf(" --customers=%d", to.customers))
	}
	if to.racks != 0 {
		builder.WriteString(fmt.Sprintf(" --racks=%d", to.racks))
	}
	if to.duration != 0 {
		builder.WriteString(fmt.Sprintf(" --duration=%s", to.duration))
	}
	if to.threads != 0 {
		builder.WriteString(fmt.Sprintf(" --threads=%d", to.threads))
	}
	return builder.String()
}

func initTPCESpec(
	ctx context.Context, t test.Test, c cluster.Cluster, loadNode,
	roachNodes option.NodeListOption,
) (*tpceSpec, error) {
	t.Status("installing docker")
	if err := c.Install(ctx, t.L(), loadNode, "docker"); err != nil {
		return nil, err
	}
	roachNodeIPs, err := c.InternalIP(ctx, t.L(), roachNodes)
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

func (ts *tpceSpec) dockerCmdPrefix() string {
	return `sudo docker run cockroachdb/tpc-e:latest`
}

// init initializes an empty cluster with a tpce database. This includes a bulk
// import of the data and schema creation.
func (ts *tpceSpec) init(ctx context.Context, t test.Test, c cluster.Cluster, o tpceCmdOptions) {
	c.Run(ctx, ts.loadNode, fmt.Sprintf("%s %s --init %s", ts.dockerCmdPrefix(), o.String(), ts.roachNodeIPFlags[0]))
}

// run runs the tpce workload on cluster that has been initialized with the tpce schema.
func (ts *tpceSpec) run(
	ctx context.Context, t test.Test, c cluster.Cluster, o tpceCmdOptions,
) (install.RunResultDetails, error) {
	cmd := fmt.Sprintf("%s %s %s",
		ts.dockerCmdPrefix(), o.String(), strings.Join(ts.roachNodeIPFlags, " "))
	return c.RunWithDetailsSingleNode(ctx, t.L(), ts.loadNode, cmd)
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
		loadNode := c.Node(opts.nodes + 1)
		racks := opts.nodes

		t.Status("installing cockroach")
		c.Put(ctx, t.Cockroach(), "./cockroach", roachNodes)

		startOpts := option.DefaultStartOpts()
		startOpts.RoachprodOpts.StoreCount = opts.ssds
		settings := install.MakeClusterSettings(install.NumRacksOption(racks))
		c.Start(ctx, t.L(), startOpts, settings, roachNodes)

		tpceSpec, err := initTPCESpec(ctx, t, c, loadNode, roachNodes)
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
