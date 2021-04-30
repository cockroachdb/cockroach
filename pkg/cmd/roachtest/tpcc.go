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
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/search"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/ttycolor"
	"github.com/lib/pq"
)

type tpccSetupType int

const (
	usingImport tpccSetupType = iota
	usingInit
	usingExistingData // skips import
)

type tpccOptions struct {
	Warehouses     int
	ExtraRunArgs   string
	ExtraSetupArgs string
	Chaos          func() Chaos                // for late binding of stopper
	During         func(context.Context) error // for running a function during the test
	Duration       time.Duration               // if zero, TPCC is not invoked
	SetupType      tpccSetupType
	// If specified, called to stage+start cockroach. If not
	// specified, defaults to uploading the default binary to
	// all nodes, and starting it on all but the last node.
	//
	// TODO(tbg): for better coverage at scale of the migration process, we should
	// also be doing a rolling-restart into the new binary while the cluster
	// is running, but that feels like jamming too much into the tpcc setup.
	Start func(context.Context, *test, *cluster)
}

// tpccImportCmd generates the command string to load tpcc data for the
// specified warehouse count into a cluster.
//
// The command uses `cockroach workload` instead of `workload` so the tpcc
// workload-versions match on release branches. Similarly, the command does not
// specify pgurl to ensure that it is run on a node with a running cockroach
// instance to ensure that the workload version matches the gateway version in a
// mixed version cluster.
func tpccImportCmd(warehouses int, extraArgs ...string) string {
	return tpccImportCmdWithCockroachBinary("./cockroach", warehouses, extraArgs...)
}

func tpccImportCmdWithCockroachBinary(
	crdbBinary string, warehouses int, extraArgs ...string,
) string {
	return fmt.Sprintf("./%s workload fixtures import tpcc --warehouses=%d %s",
		crdbBinary, warehouses, strings.Join(extraArgs, " "))
}

func setupTPCC(
	ctx context.Context, t *test, c *cluster, opts tpccOptions,
) (crdbNodes, workloadNode nodeListOption) {
	// Randomize starting with encryption-at-rest enabled.
	c.encryptAtRandom = true
	crdbNodes = c.Range(1, c.spec.NodeCount-1)
	workloadNode = c.Node(c.spec.NodeCount)
	if c.isLocal() {
		opts.Warehouses = 1
	}

	if opts.Start == nil {
		opts.Start = func(ctx context.Context, t *test, c *cluster) {
			// NB: workloadNode also needs ./cockroach because
			// of `./cockroach workload` for usingImport.
			c.Put(ctx, cockroach, "./cockroach", c.All())
			// We still use bare workload, though we could likely replace
			// those with ./cockroach workload as well.
			c.Put(ctx, workload, "./workload", workloadNode)
			c.Start(ctx, t, crdbNodes)
		}
	}

	func() {
		opts.Start(ctx, t, c)
		db := c.Conn(ctx, 1)
		defer db.Close()
		waitForFullReplication(t, c.Conn(ctx, crdbNodes[0]))
		switch opts.SetupType {
		case usingExistingData:
			// Do nothing.
		case usingImport:
			t.Status("loading fixture")
			c.Run(ctx, crdbNodes[:1], tpccImportCmd(opts.Warehouses, opts.ExtraSetupArgs))
		case usingInit:
			t.Status("initializing tables")
			extraArgs := opts.ExtraSetupArgs
			if !t.buildVersion.AtLeast(version.MustParse("v20.2.0")) {
				extraArgs += " --deprecated-fk-indexes"
			}
			cmd := fmt.Sprintf(
				"./workload init tpcc --warehouses=%d %s {pgurl:1}",
				opts.Warehouses, extraArgs,
			)
			c.Run(ctx, workloadNode, cmd)
		default:
			t.Fatal("unknown tpcc setup type")
		}
		t.Status("")
	}()
	return crdbNodes, workloadNode
}

func runTPCC(ctx context.Context, t *test, c *cluster, opts tpccOptions) {
	rampDuration := 5 * time.Minute
	if c.isLocal() {
		opts.Warehouses = 1
		if opts.Duration > time.Minute {
			opts.Duration = time.Minute
		}
		rampDuration = 30 * time.Second
	}
	crdbNodes, workloadNode := setupTPCC(ctx, t, c, opts)
	t.Status("waiting")
	m := newMonitor(ctx, c, crdbNodes)
	m.Go(func(ctx context.Context) error {
		t.WorkerStatus("running tpcc")
		cmd := fmt.Sprintf(
			"./cockroach workload run tpcc --warehouses=%d --histograms="+perfArtifactsDir+"/stats.json "+
				opts.ExtraRunArgs+" --ramp=%s --duration=%s {pgurl:1-%d}",
			opts.Warehouses, rampDuration, opts.Duration, c.spec.NodeCount-1)
		c.Run(ctx, workloadNode, cmd)
		return nil
	})
	if opts.Chaos != nil {
		chaos := opts.Chaos()
		m.Go(chaos.Runner(c, m))
	}
	if opts.During != nil {
		m.Go(opts.During)
	}
	m.Wait()

	c.Run(ctx, workloadNode, fmt.Sprintf(
		"./cockroach workload check tpcc --warehouses=%d {pgurl:1}", opts.Warehouses))
}

// tpccSupportedWarehouses returns our claim for the maximum number of tpcc
// warehouses we support for a given hardware configuration.
//
// These should be added to periodically. Ideally when tpccbench finds major
// performance movement, but at the least for every major release.
var tpccSupportedWarehouses = []struct {
	hardware   string
	v          *version.Version
	warehouses int
}{
	// We append "-0" to the version so that we capture all prereleases of the
	// specified version. Otherwise, "v2.1.0" would compare greater than
	// "v2.1.0-alpha.x".
	{hardware: "gce-n4cpu16", v: version.MustParse(`v2.1.0-0`), warehouses: 1300},
	{hardware: "gce-n4cpu16", v: version.MustParse(`v19.1.0-0`), warehouses: 1250},
	{hardware: "aws-n4cpu16", v: version.MustParse(`v19.1.0-0`), warehouses: 2100},

	// TODO(tbg): this number is copied from gce-n4cpu16. The real number should be a
	// little higher, find out what it is.
	{hardware: "gce-n5cpu16", v: version.MustParse(`v19.1.0-0`), warehouses: 1300},
	// Ditto.
	{hardware: "gce-n5cpu16", v: version.MustParse(`v2.1.0-0`), warehouses: 1300},
}

func maxSupportedTPCCWarehouses(buildVersion version.Version, cloud string, nodes clusterSpec) int {
	var v *version.Version
	var warehouses int
	hardware := fmt.Sprintf(`%s-%s`, cloud, &nodes)
	for _, x := range tpccSupportedWarehouses {
		if x.hardware != hardware {
			continue
		}
		if buildVersion.AtLeast(x.v) && (v == nil || buildVersion.AtLeast(v)) {
			v = x.v
			warehouses = x.warehouses
		}
	}
	if v == nil {
		panic(fmt.Sprintf(`could not find max tpcc warehouses for %s`, hardware))
	}
	return warehouses
}

func registerTPCC(r *testRegistry) {
	headroomSpec := makeClusterSpec(4, cpu(16))
	r.Add(testSpec{
		// w=headroom runs tpcc for a semi-extended period with some amount of
		// headroom, more closely mirroring a real production deployment than
		// running with the max supported warehouses.
		Name:       "tpcc/headroom/" + headroomSpec.String(),
		Owner:      OwnerKV,
		MinVersion: "v19.1.0",
		Tags:       []string{`default`, `release_qualification`},
		Cluster:    headroomSpec,
		Run: func(ctx context.Context, t *test, c *cluster) {
			maxWarehouses := maxSupportedTPCCWarehouses(r.buildVersion, cloud, t.spec.Cluster)
			headroomWarehouses := int(float64(maxWarehouses) * 0.7)
			t.l.Printf("computed headroom warehouses of %d\n", headroomWarehouses)
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses: headroomWarehouses,
				Duration:   120 * time.Minute,
				SetupType:  usingImport,
			})
		},
	})
	mixedHeadroomSpec := makeClusterSpec(5, cpu(16))

	r.Add(testSpec{
		// mixed-headroom is similar to w=headroom, but with an additional
		// node and on a mixed version cluster which runs its long-running
		// migrations while TPCC runs. It simulates a real production
		// deployment in the middle of the migration into a new cluster version.
		Name:  "tpcc/mixed-headroom/" + mixedHeadroomSpec.String(),
		Owner: OwnerKV,
		// TODO(tbg): add release_qualification tag once we know the test isn't
		// buggy.
		Tags:    []string{`default`},
		Cluster: mixedHeadroomSpec,
		Run: func(ctx context.Context, t *test, c *cluster) {
			crdbNodes := c.Range(1, 4)
			workloadNode := c.Node(5)

			maxWarehouses := maxSupportedTPCCWarehouses(r.buildVersion, cloud, t.spec.Cluster)
			headroomWarehouses := int(float64(maxWarehouses) * 0.7)
			if local {
				headroomWarehouses = 10
			}

			// We'll need this below.
			tpccBackgroundStepper := backgroundStepper{
				nodes: crdbNodes,
				run: func(ctx context.Context, u *versionUpgradeTest) error {
					const duration = 120 * time.Minute
					c.l.Printf("running background TPCC workload")
					runTPCC(ctx, t, c, tpccOptions{
						Warehouses: headroomWarehouses,
						Duration:   duration,
						SetupType:  usingExistingData,
						Start: func(ctx context.Context, t *test, c *cluster) {
							// Noop - we don't let tpcc upload or start binaries in this test.
						},
					})
					return nil
				}}
			const (
				mainBinary = ""
				n1         = 1
			)

			// NB: this results in ~100GB of (actual) disk usage per node once things
			// have settled down, and ~7.5k ranges. The import takes ~40 minutes.
			// The full 6.5m import ran into out of disk errors (on 250gb machines),
			// hence division by two.
			bankRows := 65104166 / 2
			if local {
				bankRows = 1000
			}

			oldV, err := PredecessorVersion(r.buildVersion)
			if err != nil {
				t.Fatal(err)
			}

			newVersionUpgradeTest(c,
				uploadAndStartFromCheckpointFixture(crdbNodes, oldV),
				waitForUpgradeStep(crdbNodes), // let predecessor version settle (gossip etc)
				preventAutoUpgradeStep(n1),
				// Load TPCC dataset, don't run TPCC yet. We do this while in the old
				// version to load some data and hopefully create some state that will
				// need work by long-running migrations.
				importTPCCStep(oldV, headroomWarehouses, crdbNodes),
				// Add a lot of cold data to this cluster. This further stresses the version
				// upgrade machinery, in which a) all ranges are touched and b) work proportional
				// to the amount data may be carried out.
				importLargeBankStep(oldV, bankRows, crdbNodes),
				// Upload and restart cluster into the new
				// binary (stays at old cluster version).
				binaryUpgradeStep(crdbNodes, mainBinary),
				uploadVersionStep(workloadNode, mainBinary), // for tpccBackgroundStepper's workload
				// Now start running TPCC in the background.
				tpccBackgroundStepper.launch,
				// While tpcc is running in the background, bump the cluster
				// version manually. We do this over allowing automatic upgrades
				// to get a better idea of what errors come back here, if any.
				// This will block until the long-running migrations have run.
				allowAutoUpgradeStep(n1),
				setClusterSettingVersionStep,
				// Wait until TPCC background run terminates
				// and fail if it reports an error.
				tpccBackgroundStepper.wait,
			).run(ctx, t)
		},
	})
	r.Add(testSpec{
		Name:       "tpcc-nowait/nodes=3/w=1",
		Owner:      OwnerKV,
		MinVersion: "v19.1.0",
		Cluster:    makeClusterSpec(4, cpu(16)),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses:   1,
				Duration:     10 * time.Minute,
				ExtraRunArgs: "--wait=false",
				SetupType:    usingImport,
			})
		},
	})
	r.Add(testSpec{
		Name:       "weekly/tpcc/headroom",
		Owner:      OwnerKV,
		MinVersion: "v19.1.0",
		Tags:       []string{`weekly`},
		Cluster:    makeClusterSpec(4, cpu(16)),
		Timeout:    time.Duration(6*24)*time.Hour + time.Duration(10)*time.Minute,
		Run: func(ctx context.Context, t *test, c *cluster) {
			warehouses := 1000
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses: warehouses,
				Duration:   6 * 24 * time.Hour,
				SetupType:  usingImport,
			})
		},
	})

	r.Add(testSpec{
		Name:       "tpcc/w=100/nodes=3/chaos=true",
		Owner:      OwnerKV,
		MinVersion: "v19.1.0",
		Cluster:    makeClusterSpec(4),
		Run: func(ctx context.Context, t *test, c *cluster) {
			duration := 30 * time.Minute
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses: 100,
				Duration:   duration,
				// For chaos tests, we don't want to use the default method because it
				// involves preparing statements on all connections (see #51785).
				ExtraRunArgs: "--method=simple --wait=false --tolerate-errors",
				Chaos: func() Chaos {
					return Chaos{
						Timer: Periodic{
							Period:   45 * time.Second,
							DownTime: 10 * time.Second,
						},
						Target:       func() nodeListOption { return c.Node(1 + rand.Intn(c.spec.NodeCount-1)) },
						Stopper:      time.After(duration),
						DrainAndQuit: false,
					}
				},
				SetupType: usingImport,
			})
		},
	})
	r.Add(testSpec{
		Name:       "tpcc/interleaved/nodes=3/cpu=16/w=500",
		Owner:      OwnerSQLQueries,
		MinVersion: "v20.1.0",
		Cluster:    makeClusterSpec(4, cpu(16)),
		Timeout:    6 * time.Hour,
		Run: func(ctx context.Context, t *test, c *cluster) {
			skip.WithIssue(t, 53886)
			runTPCC(ctx, t, c, tpccOptions{
				// Currently, we do not support import on interleaved tables which
				// prohibits loading/importing a fixture. If/when this is supported the
				// number of warehouses should be increased as we would no longer
				// bottleneck on initialization which is significantly slower than import.
				Warehouses:     500,
				Duration:       time.Minute * 15,
				ExtraSetupArgs: "--interleaved=true",
				SetupType:      usingInit,
			})
		},
	})

	// Run a few representative tpccbench specs in CI.
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  4,

		LoadWarehouses: 1000,
		EstimatedMax:   gceOrAws(cloud, 650, 800),
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  16,

		LoadWarehouses: gceOrAws(cloud, 2500, 3000),
		EstimatedMax:   gceOrAws(cloud, 2100, 2500),
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 12,
		CPUs:  16,

		LoadWarehouses: gceOrAws(cloud, 8000, 10000),
		EstimatedMax:   gceOrAws(cloud, 7000, 8000),

		Tags: []string{`weekly`},
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes:        6,
		CPUs:         16,
		Distribution: multiZone,

		LoadWarehouses: 5000,
		EstimatedMax:   2500,
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes:        9,
		CPUs:         4,
		Distribution: multiRegion,
		LoadConfig:   multiLoadgen,

		LoadWarehouses: 3000,
		EstimatedMax:   2000,
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes:      9,
		CPUs:       4,
		Chaos:      true,
		LoadConfig: singlePartitionedLoadgen,

		LoadWarehouses: 2000,
		EstimatedMax:   900,
	})
}

func gceOrAws(cloud string, gce, aws int) int {
	if cloud == "aws" {
		return aws
	}
	return gce
}

// tpccBenchDistribution represents a distribution of nodes in a tpccbench
// cluster.
type tpccBenchDistribution int

const (
	// All nodes are within the same zone.
	singleZone tpccBenchDistribution = iota
	// Nodes are distributed across 3 zones, all in the same region.
	multiZone
	// Nodes are distributed across 3 regions.
	multiRegion
)

func (d tpccBenchDistribution) zones() []string {
	switch d {
	case singleZone:
		return []string{"us-central1-b"}
	case multiZone:
		return []string{"us-central1-a", "us-central1-b", "us-central1-c"}
	case multiRegion:
		return []string{"us-east1-b", "us-west1-b", "europe-west2-b"}
	default:
		panic("unexpected")
	}
}

// tpccBenchLoadConfig represents configurations of load generators in a
// tpccbench spec.
type tpccBenchLoadConfig int

const (
	// A single load generator is run.
	singleLoadgen tpccBenchLoadConfig = iota
	// A single load generator is run with partitioning enabled.
	singlePartitionedLoadgen
	// A load generator is run in each zone.
	multiLoadgen
)

// numLoadNodes returns the number of load generator nodes that the load
// configuration requires for the given node distribution.
func (l tpccBenchLoadConfig) numLoadNodes(d tpccBenchDistribution) int {
	switch l {
	case singleLoadgen:
		return 1
	case singlePartitionedLoadgen:
		return 1
	case multiLoadgen:
		return len(d.zones())
	default:
		panic("unexpected")
	}
}

type tpccBenchSpec struct {
	Nodes        int
	CPUs         int
	Chaos        bool
	Distribution tpccBenchDistribution
	LoadConfig   tpccBenchLoadConfig

	// The number of warehouses to load into the cluster before beginning
	// benchmarking. Should be larger than EstimatedMax and should be a
	// value that is unlikely to be achievable.
	LoadWarehouses int
	// An estimate of the maximum number of warehouses achievable in the
	// cluster config. The closer this is to the actual max achievable
	// warehouse count, the faster the benchmark will be in producing a
	// result. This can be adjusted over time as performance characteristics
	// change (i.e. CockroachDB gets faster!).
	EstimatedMax int

	// MinVersion to pass to testRegistry.Add.
	MinVersion string
	// Tags to pass to testRegistry.Add.
	Tags []string
}

// partitions returns the number of partitions specified to the load generator.
func (s tpccBenchSpec) partitions() int {
	switch s.LoadConfig {
	case singleLoadgen:
		return 0
	case singlePartitionedLoadgen:
		return s.Nodes / 3
	case multiLoadgen:
		return len(s.Distribution.zones())
	default:
		panic("unexpected")
	}
}

// startOpts returns any extra start options that the spec requires.
func (s tpccBenchSpec) startOpts() []option {
	opts := []option{startArgsDontEncrypt}
	if s.LoadConfig == singlePartitionedLoadgen {
		opts = append(opts, racks(s.partitions()))
	}
	return opts
}

func registerTPCCBenchSpec(r *testRegistry, b tpccBenchSpec) {
	nameParts := []string{
		"tpccbench",
		fmt.Sprintf("nodes=%d", b.Nodes),
		fmt.Sprintf("cpu=%d", b.CPUs),
	}
	if b.Chaos {
		nameParts = append(nameParts, "chaos")
	}

	opts := []createOption{cpu(b.CPUs)}
	switch b.Distribution {
	case singleZone:
		// No specifier.
	case multiZone:
		nameParts = append(nameParts, "multi-az")
		opts = append(opts, geo(), zones(strings.Join(b.Distribution.zones(), ",")))
	case multiRegion:
		nameParts = append(nameParts, "multi-region")
		opts = append(opts, geo(), zones(strings.Join(b.Distribution.zones(), ",")))
	default:
		panic("unexpected")
	}

	switch b.LoadConfig {
	case singleLoadgen:
		// No specifier.
	case singlePartitionedLoadgen:
		nameParts = append(nameParts, "partition")
	case multiLoadgen:
		// No specifier.
	default:
		panic("unexpected")
	}

	name := strings.Join(nameParts, "/")

	numNodes := b.Nodes + b.LoadConfig.numLoadNodes(b.Distribution)
	nodes := makeClusterSpec(numNodes, opts...)

	minVersion := b.MinVersion
	if minVersion == "" {
		minVersion = "v19.1.0" // needed for import
	}

	r.Add(testSpec{
		Name:       name,
		Owner:      OwnerKV,
		Cluster:    nodes,
		MinVersion: minVersion,
		Tags:       b.Tags,
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCCBench(ctx, t, c, b)
		},
	})
}

// loadTPCCBench loads a TPCC dataset for the specific benchmark spec. The
// function is idempotent and first checks whether a compatible dataset exists,
// performing an expensive dataset restore only if it doesn't.
func loadTPCCBench(
	ctx context.Context, t *test, c *cluster, b tpccBenchSpec, roachNodes, loadNode nodeListOption,
) error {
	db := c.Conn(ctx, 1)
	defer db.Close()

	// Check if the dataset already exists and is already large enough to
	// accommodate this benchmarking. If so, we can skip the fixture RESTORE.
	if _, err := db.ExecContext(ctx, `USE tpcc`); err == nil {
		t.l.Printf("found existing tpcc database\n")

		var curWarehouses int
		if err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM tpcc.warehouse`,
		).Scan(&curWarehouses); err != nil {
			return err
		}
		if curWarehouses >= b.LoadWarehouses {
			// The cluster has enough warehouses. Nothing to do.
			return nil
		}

		// If the dataset exists but is not large enough, wipe the cluster
		// before restoring.
		c.Wipe(ctx, roachNodes)
		c.Start(ctx, t, append(b.startOpts(), roachNodes)...)
	} else if pqErr := (*pq.Error)(nil); !(errors.As(err, &pqErr) &&
		pgcode.MakeCode(string(pqErr.Code)) == pgcode.InvalidCatalogName) {
		return err
	}

	// Increase job leniency to prevent restarts due to node liveness.
	if _, err := db.Exec(`
		SET CLUSTER SETTING jobs.registry.leniency = '5m';
	`); err != nil {
		t.Fatal(err)
	}

	var loadArgs string
	var rebalanceWait time.Duration
	switch b.LoadConfig {
	case singleLoadgen:
		loadArgs = `--checks=false`
		rebalanceWait = time.Duration(b.LoadWarehouses/250) * time.Minute
	case singlePartitionedLoadgen:
		loadArgs = fmt.Sprintf(`--checks=false --partitions=%d`, b.partitions())
		rebalanceWait = time.Duration(b.LoadWarehouses/125) * time.Minute
	case multiLoadgen:
		loadArgs = fmt.Sprintf(`--checks=false --partitions=%d --zones="%s"`,
			b.partitions(), strings.Join(b.Distribution.zones(), ","))
		rebalanceWait = time.Duration(b.LoadWarehouses/50) * time.Minute
	default:
		panic("unexpected")
	}

	// Load the corresponding fixture.
	t.l.Printf("restoring tpcc fixture\n")
	waitForFullReplication(t, db)
	cmd := tpccImportCmd(b.LoadWarehouses, loadArgs)
	if err := c.RunE(ctx, roachNodes[:1], cmd); err != nil {
		return err
	}
	if rebalanceWait == 0 || len(roachNodes) <= 3 {
		return nil
	}

	t.l.Printf("waiting %v for rebalancing\n", rebalanceWait)
	_, err := db.ExecContext(ctx, `SET CLUSTER SETTING kv.snapshot_rebalance.max_rate='128MiB'`)
	if err != nil {
		return err
	}

	// Split and scatter the tables. Ramp up to the half of the expected load in
	// the desired distribution. This should allow for load-based rebalancing to
	// help distribute load. Optionally pass some load configuration-specific
	// flags.
	const txnsPerWarehousePerSecond = 12.8 * (23.0 / 10.0) * (1.0 / 60.0) // max_tpmC/warehouse * all_txns/new_order_txns * minutes/seconds
	rateAtExpected := txnsPerWarehousePerSecond * float64(b.EstimatedMax)
	maxRate := int(rateAtExpected / 2)
	rampTime := (1 * rebalanceWait) / 4
	loadTime := (3 * rebalanceWait) / 4
	cmd = fmt.Sprintf("./cockroach workload run tpcc --warehouses=%d --workers=%d --max-rate=%d "+
		"--wait=false --ramp=%s --duration=%s --scatter --tolerate-errors {pgurl%s}",
		b.LoadWarehouses, b.LoadWarehouses, maxRate, rampTime, loadTime, roachNodes)
	if out, err := c.RunWithBuffer(ctx, c.l, loadNode, cmd); err != nil {
		return errors.Wrapf(err, "failed with output %q", string(out))
	}

	_, err = db.ExecContext(ctx, `SET CLUSTER SETTING kv.snapshot_rebalance.max_rate='2MiB'`)
	return err
}

// tpccbench is a suite of benchmarking tools that run TPC-C against CockroachDB
// clusters in different configurations. The tools search for the maximum number
// of warehouses that a load generator can run TPC-C against while still
// maintaining a minimum acceptable throughput. This maximum warehouse value is
// directly comparable to other runs of the tool in the same cluster config, and
// expresses how well CockroachDB performance scales.
//
// In order to run a benchmark spec, the tool must first load a TPC-C dataset
// large enough to accommodate it. This can take a while, so it is recommended
// to use a combination of `--cluster=<cluster>` and `--wipe=false` flags to
// limit the loading phase to the first run of the tool. Subsequent runs will be
// able to avoid the dataset restore as long as they are not wiped. This allows
// for quick iteration on experimental changes.
//
// It can also be useful to omit the `--cluster` flag during the first run of
// the tool to allow roachtest to create the correct set of VMs required by the
// test. The `--wipe` flag will prevent this cluster from being destroyed, so it
// can then be used during future runs.
func runTPCCBench(ctx context.Context, t *test, c *cluster, b tpccBenchSpec) {
	// Determine the nodes in each load group. A load group consists of a set of
	// Cockroach nodes and a single load generator.
	numLoadGroups := b.LoadConfig.numLoadNodes(b.Distribution)
	numZones := len(b.Distribution.zones())
	loadGroups := makeLoadGroups(c, numZones, b.Nodes, numLoadGroups)
	roachNodes := loadGroups.roachNodes()
	loadNodes := loadGroups.loadNodes()
	c.Put(ctx, cockroach, "./cockroach", roachNodes)
	// Fixture import needs './cockroach workload' on loadNodes[0],
	// and if we use haproxy (see below) we need it on the others
	// as well.
	c.Put(ctx, cockroach, "./cockroach", loadNodes)
	// Don't encrypt in tpccbench tests.
	c.encryptDefault = false
	c.encryptAtRandom = false
	c.Start(ctx, t, append(b.startOpts(), roachNodes)...)

	useHAProxy := b.Chaos
	const restartWait = 15 * time.Second
	{
		// Wait after restarting nodes before applying load. This lets
		// things settle down to avoid unexpected cluster states.
		time.Sleep(restartWait)
		if useHAProxy {
			if len(loadNodes) > 1 {
				t.Fatal("distributed chaos benchmarking not supported")
			}
			t.Status("installing haproxy")
			if err := c.Install(ctx, t.l, loadNodes, "haproxy"); err != nil {
				t.Fatal(err)
			}
			c.Run(ctx, loadNodes, "./cockroach gen haproxy --insecure --url {pgurl:1}")
			// Increase the maximum connection limit to ensure that no TPC-C
			// load gen workers get stuck during connection initialization.
			// 10k warehouses requires at least 20,000 connections, so add a
			// bit of breathing room and check the warehouse count.
			c.Run(ctx, loadNodes, "sed -i 's/maxconn [0-9]\\+/maxconn 21000/' haproxy.cfg")
			if b.LoadWarehouses > 1e4 {
				t.Fatal("HAProxy config supports up to 10k warehouses")
			}
			c.Run(ctx, loadNodes, "haproxy -f haproxy.cfg -D")
		}

		m := newMonitor(ctx, c, roachNodes)
		m.Go(func(ctx context.Context) error {
			t.Status("setting up dataset")
			return loadTPCCBench(ctx, t, c, b, roachNodes, c.Node(loadNodes[0]))
		})
		m.Wait()
	}

	// Search between 1 and b.LoadWarehouses for the largest number of
	// warehouses that can be operated on while sustaining a throughput
	// threshold, set to a fraction of max tpmC.
	precision := int(math.Max(1.0, float64(b.LoadWarehouses/200)))
	initStepSize := precision

	// Create a temp directory to store the local copy of results from the
	// workloads.
	resultsDir, err := ioutil.TempDir("", "roachtest-tpcc")
	if err != nil {
		t.Fatal(errors.Wrap(err, "failed to create temp dir"))
	}
	defer func() { _ = os.RemoveAll(resultsDir) }()

	restart := func() {
		// We overload the clusters in tpccbench, which can lead to transient infra
		// failures. These are a) really annoying to debug and b) hide the actual
		// passing warehouse count, making the line search sensitive to the choice
		// of starting warehouses. Do a best-effort at waiting for the cloud VM(s)
		// to recover without failing the line search.
		if err := c.Reset(ctx); err != nil {
			// Reset() can flake sometimes, see for example:
			// https://github.com/cockroachdb/cockroach/issues/61981#issuecomment-826838740
			c.l.Printf("failed to reset VMs, proceeding anyway: %s", err)
			_ = err // intentionally continuing
		}
		var ok bool
		for i := 0; i < 10; i++ {
			if err := ctx.Err(); err != nil {
				t.Fatal(err)
			}
			shortCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			if err := c.StopE(shortCtx, roachNodes); err != nil {
				cancel()
				t.l.Printf("unable to stop cluster; retrying to allow vm to recover: %s", err)
				// We usually spend a long time blocking in StopE anyway, but just in case
				// of a fast-failure mode, we still want to spend a little bit of time over
				// the course of 10 retries to maximize the chances of things going back to
				// working.
				select {
				case <-time.After(30 * time.Second):
				case <-ctx.Done():
				}
				continue
			}
			cancel()
			ok = true
			break
		}
		if !ok {
			t.Fatalf("VM is hosed; giving up")
		}

		c.Start(ctx, t, append(b.startOpts(), roachNodes)...)
	}

	s := search.NewLineSearcher(1, b.LoadWarehouses, b.EstimatedMax, initStepSize, precision)
	iteration := 0
	if res, err := s.Search(func(warehouses int) (bool, error) {
		iteration++
		t.l.Printf("initializing cluster for %d warehouses (search attempt: %d)", warehouses, iteration)

		restart()

		time.Sleep(restartWait)

		// Set up the load generation configuration.
		rampDur := 5 * time.Minute
		loadDur := 10 * time.Minute
		loadDone := make(chan time.Time, numLoadGroups)

		// NB: for goroutines in this monitor, handle errors via `t.Fatal` to
		// *abort* the line search and whole tpccbench run. Return the errors
		// to indicate that the specific warehouse count failed, but that the
		// line search ought to continue.
		m := newMonitor(ctx, c, roachNodes)

		// If we're running chaos in this configuration, modify this config.
		if b.Chaos {
			// Kill one node at a time.
			ch := Chaos{
				Timer:   Periodic{Period: 90 * time.Second, DownTime: 5 * time.Second},
				Target:  roachNodes.randNode,
				Stopper: loadDone,
			}
			m.Go(ch.Runner(c, m))
		}
		if b.Distribution == multiRegion {
			rampDur = 3 * time.Minute
			loadDur = 15 * time.Minute
		}

		// If we're running multiple load generators, run them in parallel and then
		// aggregate resultChan. In order to process the results we need to copy
		// over the histograms. Create a temp dir which will contain the fetched
		// data.
		resultChan := make(chan *tpcc.Result, numLoadGroups)
		for groupIdx, group := range loadGroups {
			// Copy for goroutine
			groupIdx := groupIdx
			group := group
			m.Go(func(ctx context.Context) error {
				sqlGateways := group.roachNodes
				if useHAProxy {
					sqlGateways = group.loadNodes
				}

				extraFlags := ""
				switch b.LoadConfig {
				case singleLoadgen:
					// Nothing.
				case singlePartitionedLoadgen:
					extraFlags = fmt.Sprintf(` --partitions=%d`, b.partitions())
				case multiLoadgen:
					extraFlags = fmt.Sprintf(` --partitions=%d --partition-affinity=%d`,
						b.partitions(), groupIdx)
				default:
					// Abort the whole test.
					t.Fatalf("unimplemented LoadConfig %v", b.LoadConfig)
				}
				if b.Chaos {
					// For chaos tests, we don't want to use the default method because it
					// involves preparing statements on all connections (see #51785).
					extraFlags += " --method=simple"
				}
				t.Status(fmt.Sprintf("running benchmark, warehouses=%d", warehouses))
				histogramsPath := fmt.Sprintf("%s/warehouses=%d/stats.json", perfArtifactsDir, warehouses)
				cmd := fmt.Sprintf("./cockroach workload run tpcc --warehouses=%d --active-warehouses=%d "+
					"--tolerate-errors --ramp=%s --duration=%s%s --histograms=%s {pgurl%s}",
					b.LoadWarehouses, warehouses, rampDur,
					loadDur, extraFlags, histogramsPath, sqlGateways)
				err := c.RunE(ctx, group.loadNodes, cmd)
				loadDone <- timeutil.Now()
				if err != nil {
					// NB: this will let the line search continue at a lower warehouse
					// count.
					return errors.Wrapf(err, "error running tpcc load generator")
				}
				roachtestHistogramsPath := filepath.Join(resultsDir, fmt.Sprintf("%d.%d-stats.json", warehouses, groupIdx))
				if err := c.Get(
					ctx, t.l, histogramsPath, roachtestHistogramsPath, group.loadNodes,
				); err != nil {
					// NB: this will let the line search continue. The reason we do this
					// is because it's conceivable that we made it here, but a VM just
					// froze up on us. The next search iteration will handle this state.
					return err
				}
				snapshots, err := histogram.DecodeSnapshots(roachtestHistogramsPath)
				if err != nil {
					// If we got this far, and can't decode data, it's not a case of
					// overload but something that deserves failing the whole test.
					t.Fatal(err)
				}
				result := tpcc.NewResultWithSnapshots(warehouses, 0, snapshots)
				resultChan <- result
				return nil
			})
		}
		failErr := m.WaitE()
		close(resultChan)

		var res *tpcc.Result
		if failErr != nil {
			if t.Failed() {
				// Someone called `t.Fatal` in a monitored goroutine,
				// meaning that something went sideways in a way that
				// indicates a general problem (i.e. not just that the
				// current warehouse count overloaded the cluster.
				// Abort the whole test.
				return false, err
			}
			// A goroutine returned an error, but this means only
			// that the given warehouse count did not run to completion,
			// presumably because it overloaded the cluster. We thus
			// "achieved" zero TpmC, but will continue the search.
			//
			// Note that it's also possible that we get here due to an
			// actual bug in CRDB (for example a node crashing due to
			// getting into an invalid state); we cannot distinguish
			// those here and so tpccbench isn't a good test to rely
			// on to catch crash-causing bugs.
			res = &tpcc.Result{
				ActiveWarehouses: warehouses,
			}
		} else {
			// We managed to run TPCC, which means that we may or may
			// not have "passed" TPCC.
			var results []*tpcc.Result
			for partial := range resultChan {
				results = append(results, partial)
			}
			res = tpcc.MergeResults(results...)
			failErr = res.FailureError()
		}

		// Print the result.
		if failErr == nil {
			ttycolor.Stdout(ttycolor.Green)
			t.l.Printf("--- SEARCH ITER PASS: TPCC %d resulted in %.1f tpmC (%.1f%% of max tpmC)\n\n",
				warehouses, res.TpmC(), res.Efficiency())
		} else {
			ttycolor.Stdout(ttycolor.Red)
			t.l.Printf("--- SEARCH ITER FAIL: TPCC %d resulted in %.1f tpmC and failed due to %v",
				warehouses, res.TpmC(), failErr)
		}
		ttycolor.Stdout(ttycolor.Reset)
		return failErr == nil, nil
	}); err != nil {
		t.Fatal(err)
	} else {
		// The last iteration may have been a failing run that overloaded
		// nodes to the point of them crashing. Make roachtest happy by
		// restarting the cluster so that it can run consistency checks.
		restart()
		ttycolor.Stdout(ttycolor.Green)
		t.l.Printf("------\nMAX WAREHOUSES = %d\n------\n\n", res)
		ttycolor.Stdout(ttycolor.Reset)
	}
}

func registerTPCCBench(r *testRegistry) {
	specs := []tpccBenchSpec{
		{
			Nodes: 3,
			CPUs:  4,

			LoadWarehouses: 1000,
			EstimatedMax:   325,
		},
		{
			Nodes: 3,
			CPUs:  16,

			LoadWarehouses: 2000,
			EstimatedMax:   1300,
		},
		// objective 1, key result 1.
		{
			Nodes: 30,
			CPUs:  16,

			LoadWarehouses: 10000,
			EstimatedMax:   5300,
		},
		// objective 1, key result 2.
		{
			Nodes:      18,
			CPUs:       16,
			LoadConfig: singlePartitionedLoadgen,

			LoadWarehouses: 10000,
			EstimatedMax:   8000,
		},
		// objective 2, key result 1.
		{
			Nodes: 7,
			CPUs:  16,
			Chaos: true,

			LoadWarehouses: 5000,
			EstimatedMax:   2000,
		},
		// objective 3, key result 1.
		{
			Nodes:        3,
			CPUs:         16,
			Distribution: multiZone,

			LoadWarehouses: 2000,
			EstimatedMax:   1000,
		},
		// objective 3, key result 2.
		{
			Nodes:        9,
			CPUs:         16,
			Distribution: multiRegion,
			LoadConfig:   multiLoadgen,

			LoadWarehouses: 12000,
			EstimatedMax:   8000,

			MinVersion: "v19.1.0",
		},
		// objective 4, key result 2.
		{
			Nodes: 64,
			CPUs:  16,

			LoadWarehouses: 50000,
			EstimatedMax:   40000,
		},

		// See https://github.com/cockroachdb/cockroach/issues/31409 for the next three specs.
		{
			Nodes: 6,
			CPUs:  16,

			LoadWarehouses: 5000,
			EstimatedMax:   3000,
			LoadConfig:     singlePartitionedLoadgen,
		},
		{
			Nodes: 12,
			CPUs:  16,

			LoadWarehouses: 10000,
			EstimatedMax:   6000,
			LoadConfig:     singlePartitionedLoadgen,
		},
		{
			Nodes: 24,
			CPUs:  16,

			LoadWarehouses: 20000,
			EstimatedMax:   12000,
			LoadConfig:     singlePartitionedLoadgen,
		},

		// Requested by @awoods87.
		{
			Nodes: 11,
			CPUs:  32,

			LoadWarehouses: 10000,
			EstimatedMax:   8000,
		},
	}

	for _, b := range specs {
		registerTPCCBenchSpec(r, b)
	}
}
