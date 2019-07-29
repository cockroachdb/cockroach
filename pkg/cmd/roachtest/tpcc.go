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
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/util/binfetcher"
	"github.com/cockroachdb/cockroach/pkg/util/search"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/cockroachdb/ttycolor"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type tpccOptions struct {
	Warehouses int
	Extra      string
	Chaos      func() Chaos                // for late binding of stopper
	During     func(context.Context) error // for running a function during the test
	Duration   time.Duration

	// ZFS, if set, will make the cluster use a ZFS volume.
	// Be careful with ClusterReusePolicy when using this.
	//
	// TODO(andrei): move this to the test's cluster spec.
	ZFS bool

	// The CockroachDB versions to deploy. The first one indicates the first node,
	// etc. To use the main binary, specify "". When Versions is nil, it defaults
	// to "" for all nodes. When it is specified, len(Versions) needs to match the
	// number of CRDB nodes in the cluster.
	//
	// TODO(tbg): for better coverage at scale of the migration process, we
	// should also be doing a rolling-restart into the new binary while the
	// cluster is running, but that feels like jamming too much into the tpcc
	// setup.
	Versions []string
}

// tpccFixturesCmd generates the command string to load tpcc data for the
// specified warehouse count into a cluster using either `fixtures import`
// or `fixtures load` depending on the cloud.
func tpccFixturesCmd(t *test, cloud string, warehouses int, extraArgs string) string {
	var command string
	switch cloud {
	case "gce":
		// TODO(nvanbenschoten): We could switch to import for both clouds.
		// At the moment, import is still a little unstable and load is still
		// marginally faster.
		command = "./workload fixtures load"
		fixtureWarehouses := -1
		for _, w := range []int{1, 10, 100, 1000, 2000, 5000, 10000} {
			if w >= warehouses {
				fixtureWarehouses = w
				break
			}
		}
		if fixtureWarehouses == -1 {
			t.Fatalf("could not find fixture big enough for %d warehouses", warehouses)
		}
		warehouses = fixtureWarehouses
	case "aws":
		// For fixtures import, use the version built into the cockroach binary
		// so the tpcc workload-versions match on release branches.
		command = "./cockroach workload fixtures import"
	default:
		t.Fatalf("unknown cloud: %q", cloud)
	}
	return fmt.Sprintf("%s tpcc --warehouses=%d %s {pgurl:1}",
		command, warehouses, extraArgs)
}

func runTPCC(ctx context.Context, t *test, c *cluster, opts tpccOptions) {
	crdbNodes := c.Range(1, c.spec.NodeCount-1)
	workloadNode := c.Node(c.spec.NodeCount)
	rampDuration := 5 * time.Minute
	if c.isLocal() {
		opts.Warehouses = 1
		opts.Duration = 1 * time.Minute
		rampDuration = 30 * time.Second
	}

	if n := len(opts.Versions); n == 0 {
		opts.Versions = make([]string, c.spec.NodeCount-1)
	} else if n != c.spec.NodeCount-1 {
		t.Fatalf("must specify Versions for all %d nodes: %v", c.spec.NodeCount-1, opts.Versions)
	}

	{
		var regularNodes []option
		for i, v := range opts.Versions {
			if v == "" {
				regularNodes = append(regularNodes, c.Node(i+1))
			} else {
				// NB: binfetcher caches the downloaded files.
				binary, err := binfetcher.Download(ctx, binfetcher.Options{
					Binary:  "cockroach",
					Version: v,
					GOOS:    ifLocal(runtime.GOOS, "linux"),
					GOARCH:  "amd64",
				})
				if err != nil {
					t.Fatalf("while fetching %s: %s", v, err)
				}
				c.Put(ctx, binary, "./cockroach", c.Node(i+1))
			}
		}
		c.Put(ctx, cockroach, "./cockroach", regularNodes...)
	}

	// Fixture import needs ./cockroach workload on workloadNode.
	c.Put(ctx, cockroach, "./cockroach", workloadNode)
	c.Put(ctx, workload, "./workload", workloadNode)

	t.Status("loading fixture")
	func() {
		db := c.Conn(ctx, 1)
		defer db.Close()
		if opts.ZFS {
			if err := c.RunE(ctx, c.Node(1), "test -d /mnt/data1/.zfs/snapshot/pristine"); err != nil {
				// Use ZFS so the initial store dumps can be instantly rolled back to their
				// pristine state. Useful for iterating quickly on the test, especially when
				// used in a repro.
				c.Reformat(ctx, crdbNodes, "zfs")

				t.Status("loading dataset")
				c.Start(ctx, t, crdbNodes, startArgsDontEncrypt)

				c.Run(ctx, workloadNode, tpccFixturesCmd(t, cloud, opts.Warehouses, ""))
				c.Stop(ctx, crdbNodes)

				c.Run(ctx, crdbNodes, "test -e /sbin/zfs && sudo zfs snapshot data1@pristine")
			}
			t.Status(`restoring store dumps`)
			c.Run(ctx, crdbNodes, "sudo zfs rollback data1@pristine")
			c.Start(ctx, t, crdbNodes, startArgsDontEncrypt)
		} else {
			c.Start(ctx, t, crdbNodes, startArgsDontEncrypt)
			c.Run(ctx, workloadNode, tpccFixturesCmd(t, cloud, opts.Warehouses, ""))
		}
	}()
	t.Status("waiting")
	m := newMonitor(ctx, c, crdbNodes)
	m.Go(func(ctx context.Context) error {
		t.WorkerStatus("running tpcc")
		cmd := fmt.Sprintf(
			"./workload run tpcc --warehouses=%d --histograms="+perfArtifactsDir+"/stats.json "+
				opts.Extra+" --ramp=%s --duration=%s {pgurl:1-%d}",
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
		"./workload check tpcc --warehouses=%d {pgurl:1}", opts.Warehouses))
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
		Name: "tpcc/headroom/" + headroomSpec.String(),
		// TODO(dan): Backfill tpccSupportedWarehouses and remove this "v2.1.0"
		// minimum on gce.
		MinVersion: maxVersion("v2.1.0", maybeMinVersionForFixturesImport(cloud)),
		Tags:       []string{`default`, `release_qualification`},
		Cluster:    headroomSpec,
		Run: func(ctx context.Context, t *test, c *cluster) {
			maxWarehouses := maxSupportedTPCCWarehouses(r.buildVersion, cloud, t.spec.Cluster)
			headroomWarehouses := int(float64(maxWarehouses) * 0.7)
			t.l.Printf("computed headroom warehouses of %d\n", headroomWarehouses)
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses: headroomWarehouses,
				Duration:   120 * time.Minute,
			})
		},
	})
	mixedHeadroomSpec := makeClusterSpec(5, cpu(16))
	r.Add(testSpec{
		// mixed-headroom is similar to w=headroom, but with an additional node
		// and on a mixed version cluster. It simulates a real production
		// deployment in the middle of the migration into a new cluster version.
		Name: "tpcc/mixed-headroom/" + mixedHeadroomSpec.String(),
		// TODO(dan): Backfill tpccSupportedWarehouses and remove this "v2.1.0"
		// minimum on gce.
		MinVersion: maxVersion("v2.1.0", maybeMinVersionForFixturesImport(cloud)),
		// TODO(tbg): add release_qualification tag once we know the test isn't
		// buggy.
		Tags:    []string{`default`},
		Cluster: mixedHeadroomSpec,
		Run: func(ctx context.Context, t *test, c *cluster) {
			maxWarehouses := maxSupportedTPCCWarehouses(r.buildVersion, cloud, t.spec.Cluster)
			headroomWarehouses := int(float64(maxWarehouses) * 0.7)
			oldV, err := PredecessorVersion(r.buildVersion)
			if err != nil {
				t.Fatal(err)
			}
			// Make a git tag out of the version.
			oldV = "v" + oldV
			t.l.Printf("computed headroom warehouses of %d; running mixed with %s\n", headroomWarehouses, oldV)
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses: headroomWarehouses,
				Duration:   120 * time.Minute,
				Versions:   []string{oldV, "", oldV, ""},
			})
			// TODO(tbg): run another TPCC with the final binaries here and
			// teach TPCC to re-use the dataset (seems easy enough) to at least
			// somewhat test the full migration at scale?
		},
	})
	r.Add(testSpec{
		Name:       "tpcc-nowait/nodes=3/w=1",
		MinVersion: maybeMinVersionForFixturesImport(cloud),
		Cluster:    makeClusterSpec(4, cpu(16)),
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses: 1,
				Duration:   10 * time.Minute,
				Extra:      "--wait=false",
			})
		},
	})
	r.Add(testSpec{
		Name:       "weekly/tpcc-max",
		MinVersion: maybeMinVersionForFixturesImport(cloud),
		Tags:       []string{`weekly`},
		Cluster:    makeClusterSpec(4, cpu(16)),
		Run: func(ctx context.Context, t *test, c *cluster) {
			warehouses := 1350
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses: warehouses,
				Duration:   6 * 24 * time.Hour,
			})
		},
	})

	r.Add(testSpec{
		Name:       "tpcc/w=100/nodes=3/chaos=true",
		Cluster:    makeClusterSpec(4),
		MinVersion: maybeMinVersionForFixturesImport(cloud),
		Run: func(ctx context.Context, t *test, c *cluster) {
			duration := 30 * time.Minute
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses: 100,
				Duration:   duration,
				Extra:      "--wait=false --tolerate-errors",
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
				ZFS: false, // change to true during debugging/development
			})
		},
	})

	// Run a few representative tpccbench specs in CI.
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  4,

		LoadWarehouses: 1000,
		EstimatedMax:   gceOrAws(cloud, 400, 600),
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 3,
		CPUs:  16,

		LoadWarehouses: gceOrAws(cloud, 2000, 2500),
		EstimatedMax:   gceOrAws(cloud, 1600, 2300),
	})
	registerTPCCBenchSpec(r, tpccBenchSpec{
		Nodes: 12,
		CPUs:  16,

		LoadWarehouses: gceOrAws(cloud, 8000, 10000),
		EstimatedMax:   gceOrAws(cloud, 7000, 7000),

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
		Nodes:      9,
		CPUs:       4,
		Chaos:      true,
		LoadConfig: singlePartitionedLoadgen,

		LoadWarehouses: 2000,
		EstimatedMax:   600,
	})
}

func maxVersion(vers ...string) string {
	var max *version.Version
	for _, v := range vers {
		v, err := version.Parse(v)
		if err != nil {
			continue
		}
		if max == nil || v.AtLeast(max) {
			max = v
		}
	}
	if max == nil {
		return ""
	}
	return max.String()
}

func gceOrAws(cloud string, gce, aws int) int {
	if cloud == "aws" {
		return aws
	}
	return gce
}

func maybeMinVersionForFixturesImport(cloud string) string {
	const minVersionForFixturesImport = "v19.1.0"
	if cloud == "aws" {
		return minVersionForFixturesImport
	}
	return ""
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

	r.Add(testSpec{
		Name:       name,
		Cluster:    nodes,
		MinVersion: maybeMinVersionForFixturesImport(cloud),
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
	} else if pqErr, ok := err.(*pq.Error); !ok ||
		string(pqErr.Code) != pgcode.InvalidCatalogName {
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
		loadArgs = `--split --scatter --checks=false`
		rebalanceWait = time.Duration(b.LoadWarehouses/100) * time.Minute
	case singlePartitionedLoadgen:
		loadArgs = fmt.Sprintf(`--split --scatter --checks=false --partitions=%d`, b.partitions())
		rebalanceWait = time.Duration(b.LoadWarehouses/50) * time.Minute
	case multiLoadgen:
		loadArgs = fmt.Sprintf(`--split --scatter --checks=false --partitions=%d --zones="%s"`,
			b.partitions(), strings.Join(b.Distribution.zones(), ","))
		rebalanceWait = time.Duration(b.LoadWarehouses/20) * time.Minute
	default:
		panic("unexpected")
	}

	// Load the corresponding fixture.
	t.l.Printf("restoring tpcc fixture\n")
	cmd := tpccFixturesCmd(t, cloud, b.LoadWarehouses, loadArgs)
	if err := c.RunE(ctx, loadNode, cmd); err != nil {
		return err
	}
	if rebalanceWait == 0 {
		return nil
	}

	t.l.Printf("waiting %v for rebalancing\n", rebalanceWait)
	_, err := db.ExecContext(ctx, `SET CLUSTER SETTING kv.snapshot_rebalance.max_rate='64MiB'`)
	if err != nil {
		return err
	}

	// Split and scatter the tables. Ramp up to the expected load in the desired
	// distribution. This should allow for load-based rebalancing to help
	// distribute load. Optionally pass some load configuration-specific flags.
	cmd = fmt.Sprintf("./workload run tpcc --warehouses=%d --workers=%d "+
		"--wait=false --duration=%s --tolerate-errors {pgurl%s}",
		b.LoadWarehouses, b.LoadWarehouses, rebalanceWait, roachNodes)
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
	// Fixture import needs ./cockroach workload on loadNodes[0],
	// and if we use haproxy (see below) we need it on the others
	// as well.
	c.Put(ctx, cockroach, "./cockroach", loadNodes)
	c.Put(ctx, workload, "./workload", loadNodes)
	c.Start(ctx, t, append(b.startOpts(), roachNodes)...)

	useHAProxy := b.Chaos
	if useHAProxy {
		if len(loadNodes) > 1 {
			t.Fatal("distributed chaos benchmarking not supported")
		}
		t.Status("installing haproxy")
		if err := c.Install(ctx, t.l, loadNodes, "haproxy"); err != nil {
			t.Fatal(err)
		}
		c.Run(ctx, loadNodes, "./cockroach gen haproxy --insecure --url {pgurl:1}")
		c.Run(ctx, loadNodes, "haproxy -f haproxy.cfg -D")
	}

	m := newMonitor(ctx, c, roachNodes)
	m.Go(func(ctx context.Context) error {
		t.Status("setting up dataset")
		err := loadTPCCBench(ctx, t, c, b, roachNodes, c.Node(loadNodes[0]))
		if err != nil {
			return err
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
			return errors.Wrap(err, "failed to create temp dir")
		}
		defer func() { _ = os.RemoveAll(resultsDir) }()
		s := search.NewLineSearcher(1, b.LoadWarehouses, b.EstimatedMax, initStepSize, precision)
		res, err := s.Search(func(warehouses int) (bool, error) {
			// Restart the cluster before each iteration to help eliminate
			// inter-trial interactions.
			m.ExpectDeaths(int32(len(roachNodes)))
			c.Stop(ctx, roachNodes)
			c.Start(ctx, t, append(b.startOpts(), roachNodes)...)
			time.Sleep(10 * time.Second)

			// Set up the load generation configuration.
			rampDur := 5 * time.Minute
			loadDur := 10 * time.Minute
			loadDone := make(chan time.Time, numLoadGroups)

			// If we're running chaos in this configuration, modify this config.
			if b.Chaos {
				// Increase the load generation duration.
				loadDur = 10 * time.Minute

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
			var eg errgroup.Group
			resultChan := make(chan *tpcc.Result, numLoadGroups)
			for groupIdx, group := range loadGroups {
				// Copy for goroutine
				groupIdx := groupIdx
				group := group
				eg.Go(func() error {
					sqlGateways := group.roachNodes
					if useHAProxy {
						sqlGateways = group.loadNodes
					}

					extraFlags := ""
					activeWarehouses := warehouses
					switch b.LoadConfig {
					case singleLoadgen:
						// Nothing.
					case singlePartitionedLoadgen:
						extraFlags = fmt.Sprintf(` --partitions=%d`, b.partitions())
					case multiLoadgen:
						extraFlags = fmt.Sprintf(` --partitions=%d --partition-affinity=%d`,
							b.partitions(), groupIdx)
						activeWarehouses = warehouses / numLoadGroups
					default:
						panic("unexpected")
					}

					t.Status(fmt.Sprintf("running benchmark, warehouses=%d", warehouses))
					histogramsPath := fmt.Sprintf("%s/warehouses=%d/stats.json", perfArtifactsDir, activeWarehouses)
					cmd := fmt.Sprintf("./workload run tpcc --warehouses=%d --active-warehouses=%d "+
						"--tolerate-errors --ramp=%s --duration=%s%s {pgurl%s} "+
						"--histograms=%s",
						b.LoadWarehouses, activeWarehouses, rampDur,
						loadDur, extraFlags, sqlGateways, histogramsPath)
					err := c.RunE(ctx, group.loadNodes, cmd)
					loadDone <- timeutil.Now()
					if err != nil {
						return errors.Wrapf(err, "error running tpcc load generator")
					}
					roachtestHistogramsPath := filepath.Join(resultsDir, fmt.Sprintf("%d.%d-stats.json", warehouses, groupIdx))
					if err := c.Get(
						ctx, t.l, histogramsPath, roachtestHistogramsPath, group.loadNodes,
					); err != nil {
						t.Fatal(err)
					}
					snapshots, err := histogram.DecodeSnapshots(roachtestHistogramsPath)
					if err != nil {
						return errors.Wrapf(err, "failed to decode histogram snapshots")
					}
					result := tpcc.NewResultWithSnapshots(activeWarehouses, 0, snapshots)
					resultChan <- result
					return nil
				})
			}
			if err = eg.Wait(); err != nil {
				return false, err
			}
			close(resultChan)
			var results []*tpcc.Result
			for partial := range resultChan {
				results = append(results, partial)
			}
			res := tpcc.MergeResults(results...)
			failErr := res.FailureError()
			// Print the result.
			if failErr == nil {
				ttycolor.Stdout(ttycolor.Green)
				t.l.Printf("--- PASS: tpcc %d resulted in %.1f tpmC (%.1f%% of max tpmC)\n\n",
					warehouses, res.TpmC(), res.Efficiency())
			} else {
				ttycolor.Stdout(ttycolor.Red)
				t.l.Printf("--- FAIL: tpcc %d resulted in %.1f tpmC and failed due to %v",
					warehouses, res.TpmC(), failErr)
			}
			ttycolor.Stdout(ttycolor.Reset)
			return failErr == nil, nil
		})
		if err != nil {
			return err
		}

		ttycolor.Stdout(ttycolor.Green)
		t.l.Printf("------\nMAX WAREHOUSES = %d\n------\n\n", res)
		ttycolor.Stdout(ttycolor.Reset)
		return nil
	})
	m.Wait()
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

			LoadWarehouses: 5000,
			EstimatedMax:   2200,
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
