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
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type tpceSpec struct {
	loadNode         int
	roachNodes       option.NodeListOption
	roachNodeIPFlags []string
	portFlag         string
}

type tpceCmdOptions struct {
	customers       int
	activeCustomers int
	racks           int
	duration        time.Duration
	threads         int
	skipCleanup     bool
	connectionOpts  tpceConnectionOpts
}

type tpceConnectionOpts struct {
	fixtureBucket string
	user          string
	password      string
}

const (
	defaultFixtureBucket = "gs://cockroach-fixtures-us-east1/tpce-csv"
	defaultUser          = install.DefaultUser
	defaultPassword      = install.DefaultPassword
)

func defaultTPCEConnectionOpts() tpceConnectionOpts {
	return tpceConnectionOpts{
		fixtureBucket: defaultFixtureBucket,
		user:          defaultUser,
		password:      defaultPassword,
	}
}

func (to tpceCmdOptions) AddCommandOptions(cmd *roachtestutil.Command) {
	cmd.MaybeFlag(to.customers != 0, "customers", to.customers)
	cmd.MaybeFlag(to.activeCustomers != 0, "active-customers", to.activeCustomers)
	cmd.MaybeFlag(to.racks != 0, "racks", to.racks)
	cmd.MaybeFlag(to.duration != 0, "duration", to.duration)
	cmd.MaybeFlag(to.threads != 0, "threads", to.threads)
	cmd.MaybeFlag(to.skipCleanup, "skip-cleanup", "")
	cmd.MaybeFlag(to.connectionOpts.fixtureBucket != "", "bucket", to.connectionOpts.fixtureBucket)
	cmd.MaybeFlag(to.connectionOpts.user != "", "pg-user", to.connectionOpts.user)
	cmd.MaybeFlag(to.connectionOpts.password != "", "pg-password", to.connectionOpts.password)
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
	ports, err := c.SQLPorts(ctx, l, roachNodes, "" /* tenant */, 0 /* sqlInstance */)
	if err != nil {
		return nil, err
	}
	port := fmt.Sprintf("--pg-port=%d", ports[0])
	return &tpceSpec{
		loadNode:         loadNode,
		roachNodes:       roachNodes,
		roachNodeIPFlags: roachNodeIPFlags,
		portFlag:         port,
	}, nil
}

func (ts *tpceSpec) newCmd(o tpceCmdOptions) *roachtestutil.Command {
	cmd := roachtestutil.NewCommand(`sudo docker run us-east1-docker.pkg.dev/crl-ci-images/cockroach/tpc-e:master`)
	o.AddCommandOptions(cmd)
	return cmd
}

// init initializes an empty cluster with a tpce database. This includes a bulk
// import of the data and schema creation.
func (ts *tpceSpec) init(ctx context.Context, t test.Test, c cluster.Cluster, o tpceCmdOptions) {
	cmd := ts.newCmd(o).Option("init")
	c.Run(ctx, option.WithNodes(c.Node(ts.loadNode)), fmt.Sprintf("%s %s %s", cmd, ts.portFlag, ts.roachNodeIPFlags[0]))
}

// run runs the tpce workload on cluster that has been initialized with the tpce schema.
func (ts *tpceSpec) run(
	ctx context.Context, t test.Test, c cluster.Cluster, o tpceCmdOptions,
) (install.RunResultDetails, error) {
	cmd := fmt.Sprintf("%s %s %s", ts.newCmd(o), ts.portFlag, strings.Join(ts.roachNodeIPFlags, " "))
	return c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.Node(ts.loadNode)), cmd)
}

type tpceOptions struct {
	// start is called to stage+start cockroach. If not specified, it defaults
	// to uploading the default binary to all nodes, and starting it on all but
	// the last node.
	start func(context.Context, test.Test, cluster.Cluster)

	customers int // --customers
	nodes     int // use to determine where the workload is run from, how data is partitioned, and workload concurrency
	cpus      int // used to determine workload concurrency
	ssds      int // used during cluster init

	// Promethues specific flags.
	//
	prometheusConfig  *prometheus.Config // overwrites the default prometheus config settings
	disablePrometheus bool               // forces prometheus to not start up

	// TPC-E init specific flags.
	//
	setupType          tpceSetupType
	estimatedSetupTime time.Duration // used only for logging

	// Workload specific flags.
	//
	onlySetup        bool                            // doesn't run the workload
	workloadDuration time.Duration                   // how long the workload runs for
	activeCustomers  int                             // --active-customers in the workload
	threads          int                             // overrides the number of threads used
	skipCleanup      bool                            // passes --skip-cleanup to the tpc-e workload
	during           func(ctx context.Context) error // invoked concurrently with the workload
}
type tpceSetupType int

const (
	usingTPCEInit         tpceSetupType = iota
	usingExistingTPCEData               // skips import
)

func runTPCE(ctx context.Context, t test.Test, c cluster.Cluster, opts tpceOptions) {
	crdbNodes := c.Range(1, opts.nodes)
	loadNode := opts.nodes + 1
	racks := opts.nodes

	if opts.start == nil {
		opts.start = func(ctx context.Context, t test.Test, c cluster.Cluster) {
			t.Status("installing cockroach")
			startOpts := option.DefaultStartOpts()
			startOpts.RoachprodOpts.StoreCount = opts.ssds
			settings := install.MakeClusterSettings(install.NumRacksOption(racks))
			c.Start(ctx, t.L(), startOpts, settings, crdbNodes)
		}
	}
	opts.start(ctx, t, c)

	tpceSpec, err := initTPCESpec(ctx, t.L(), c, loadNode, crdbNodes)
	require.NoError(t, err)

	// Configure to increase the speed of the import.
	{
		db := c.Conn(ctx, t.L(), 1)
		defer db.Close()
		if _, err := db.ExecContext(
			ctx, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false",
		); err != nil {
			t.Fatal(err)
		}
	}

	if !opts.disablePrometheus {
		// TODO(irfansharif): Move this after the import step? The statistics
		// during import itself is uninteresting and pollutes actual workload
		// data.
		var cleanupFunc func()
		_, cleanupFunc = setupPrometheusForRoachtest(ctx, t, c, opts.prometheusConfig, nil)
		defer cleanupFunc()
	}

	if opts.setupType == usingTPCEInit && !t.SkipInit() {
		m := c.NewMonitor(ctx, crdbNodes)
		m.Go(func(ctx context.Context) error {
			estimatedSetupTimeStr := ""
			if opts.estimatedSetupTime != 0 {
				estimatedSetupTimeStr = fmt.Sprintf(" (<%s)", opts.estimatedSetupTime)
			}
			t.Status(fmt.Sprintf("initializing %d tpc-e customers%s", opts.customers, estimatedSetupTimeStr))
			tpceSpec.init(ctx, t, c, tpceCmdOptions{
				customers:      opts.customers,
				racks:          racks,
				connectionOpts: defaultTPCEConnectionOpts(),
			})
			return nil
		})
		m.Wait() // for init
	} else {
		t.Status("skipping tpc-e init")
	}
	if opts.onlySetup {
		return
	}

	m := c.NewMonitor(ctx, crdbNodes)
	m.Go(func(ctx context.Context) error {
		t.Status("running workload")
		workloadDuration := opts.workloadDuration
		if workloadDuration == 0 {
			workloadDuration = 2 * time.Hour
		}
		runOptions := tpceCmdOptions{
			customers:      opts.customers,
			racks:          racks,
			duration:       workloadDuration,
			threads:        opts.nodes * opts.cpus,
			connectionOpts: defaultTPCEConnectionOpts(),
		}
		if opts.activeCustomers != 0 {
			runOptions.activeCustomers = opts.activeCustomers
		}
		if opts.threads != 0 {
			runOptions.threads = opts.threads
		}
		if opts.skipCleanup {
			runOptions.skipCleanup = opts.skipCleanup
		}
		result, err := tpceSpec.run(ctx, t, c, runOptions)
		if err != nil {
			t.Fatal(err.Error())
		}
		t.L().Printf("workload output:\n%s\n", result.Stdout)
		if strings.Contains(result.Stdout, "Reported tpsE :    --   (not between 80% and 100%)") {
			return errors.New("invalid tpsE fraction")
		}
		return nil
	})
	if opts.during != nil {
		m.Go(opts.during)
	}
	m.Wait()
}

func registerTPCE(r registry.Registry) {
	// Nightly, small scale configuration.
	smallNightly := tpceOptions{
		customers: 5_000,
		nodes:     3,
		cpus:      4,
		ssds:      1,
	}
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("tpce/c=%d/nodes=%d", smallNightly.customers, smallNightly.nodes),
		Owner:            registry.OwnerTestEng,
		Timeout:          4 * time.Hour,
		Cluster:          r.MakeClusterSpec(smallNightly.nodes+1, spec.CPU(smallNightly.cpus), spec.SSD(smallNightly.ssds)),
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Nightly),
		// Never run with runtime assertions as this makes this test take
		// too long to complete.
		CockroachBinary: registry.StandardCockroach,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCE(ctx, t, c, smallNightly)
		},
	})

	// Weekly, large scale configuration.
	largeWeekly := tpceOptions{
		customers: 100_000,
		nodes:     5,
		cpus:      32,
		ssds:      2,
	}
	r.Add(registry.TestSpec{
		Name:             fmt.Sprintf("tpce/c=%d/nodes=%d", largeWeekly.customers, largeWeekly.nodes),
		Owner:            registry.OwnerTestEng,
		Benchmark:        true,
		CompatibleClouds: registry.OnlyGCE,
		Suites:           registry.Suites(registry.Weekly),
		Timeout:          36 * time.Hour,
		Cluster:          r.MakeClusterSpec(largeWeekly.nodes+1, spec.CPU(largeWeekly.cpus), spec.SSD(largeWeekly.ssds)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCE(ctx, t, c, largeWeekly)
		},
	})
}
