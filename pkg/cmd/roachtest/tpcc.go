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
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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
}

// tpccFixturesCmd generates the command string to load tpcc data for the
// specified warehouse count into a cluster using either `fixtures import`
// or `fixtures load` depending on the cloud.
func tpccFixturesCmd(t *test, cloud string, warehouses int, checks bool) string {
	var action string
	switch cloud {
	case "gce":
		// TODO(nvanbenschoten): We could switch to import for both clouds.
		// At the moment, import is still a little unstable and load is still
		// marginally faster.
		action = "load"
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
		action = "import"
	default:
		t.Fatalf("unknown cloud: %q", cloud)
	}
	return fmt.Sprintf("./workload fixtures %s tpcc --checks=%v --warehouses=%d {pgurl:1}",
		action, checks, warehouses)
}

func runTPCC(ctx context.Context, t *test, c *cluster, opts tpccOptions) {
	crdbNodes := c.Range(1, c.nodes-1)
	workloadNode := c.Node(c.nodes)
	rampDuration := 5 * time.Minute
	if c.isLocal() {
		opts.Warehouses = 1
		opts.Duration = 1 * time.Minute
		rampDuration = 30 * time.Second
	}

	c.Put(ctx, cockroach, "./cockroach", crdbNodes)
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
				c.Start(ctx, t, crdbNodes)

				c.Run(ctx, workloadNode, tpccFixturesCmd(t, cloud, opts.Warehouses, true /* checks */))
				c.Stop(ctx, crdbNodes)

				c.Run(ctx, crdbNodes, "test -e /sbin/zfs && sudo zfs snapshot data1@pristine")
			}
			t.Status(`restoring store dumps`)
			c.Run(ctx, crdbNodes, "sudo zfs rollback data1@pristine")
			c.Start(ctx, t, crdbNodes)
		} else {
			c.Start(ctx, t, crdbNodes)
			c.Run(ctx, workloadNode, tpccFixturesCmd(t, cloud, opts.Warehouses, true /* checks */))
		}
	}()
	t.Status("waiting")
	m := newMonitor(ctx, c, crdbNodes)
	m.Go(func(ctx context.Context) error {
		t.WorkerStatus("running tpcc")
		cmd := fmt.Sprintf(
			"./workload run tpcc --warehouses=%d --histograms=logs/stats.json "+
				opts.Extra+" --ramp=%s --duration=%s {pgurl:1-%d}",
			opts.Warehouses, rampDuration, opts.Duration, c.nodes-1)
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

func registerTPCC(r *registry) {
	r.Add(testSpec{
		Name: "tpcc/nodes=3/w=max",
		// TODO(dan): Instead of MinVersion, adjust the warehouses below to
		// match our expectation for the max tpcc warehouses that previous
		// releases will support on this hardware.
		MinVersion: maxVersion("v2.1.0", maybeMinVersionForFixturesImport(cloud)),
		Cluster:    makeClusterSpec(4, cpu(16)),
		Run: func(ctx context.Context, t *test, c *cluster) {
			var warehouses int
			switch cloud {
			case "gce":
				warehouses = 1350
			case "aws":
				warehouses = 2300
			default:
				t.Fatalf("unknown cloud: %q", cloud)
			}
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses: warehouses,
				Duration:   120 * time.Minute,
			})
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
						Target:       func() nodeListOption { return c.Node(1 + rand.Intn(c.nodes-1)) },
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
		EstimatedMax:   300,
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

func maybeMinVersionForFixturesImport(cloud string) string {
	const minVersionForFixturesImport = "v2.2.0"
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
	// An optional version that is part of a URL pointing at a pre-generated
	// store dump directory. Can be used to speed up dataset loading on fresh
	// clusters.
	StoreDirVersion string
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
	var opts []option
	if s.LoadConfig == singlePartitionedLoadgen {
		opts = append(opts, racks(s.partitions()))
	}
	return opts
}

func registerTPCCBenchSpec(r *registry, b tpccBenchSpec) {
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
		string(pqErr.Code) != pgerror.CodeInvalidCatalogNameError {
		return err
	}

	// If the fixture has a corresponding store dump, use it.
	if b.StoreDirVersion != "" {
		t.l.Printf("ingesting existing tpcc store dump\n")

		urlBase, err := c.RunWithBuffer(ctx, t.l, c.Node(loadNode[0]),
			fmt.Sprintf(`./workload fixtures url tpcc --warehouses=%d`, b.LoadWarehouses))
		if err != nil {
			return err
		}

		fixtureURL := string(bytes.TrimSpace(urlBase))
		storeDirsPath := storeDirURL(fixtureURL, len(roachNodes), b.StoreDirVersion)
		return downloadStoreDumps(ctx, c, storeDirsPath, len(roachNodes))
	}

	// Load the corresponding fixture.
	t.l.Printf("restoring tpcc fixture\n")
	cmd := tpccFixturesCmd(t, cloud, b.LoadWarehouses, false /* checks */)
	if err := c.RunE(ctx, loadNode, cmd); err != nil {
		return err
	}

	partArgs := ""
	rebalanceWait := time.Duration(b.LoadWarehouses/100) * time.Minute
	switch b.LoadConfig {
	case singleLoadgen:
		t.l.Printf("splitting and scattering\n")
	case singlePartitionedLoadgen:
		t.l.Printf("splitting, scattering, and partitioning\n")
		partArgs = fmt.Sprintf(`--partitions=%d`, b.partitions())
		rebalanceWait = time.Duration(b.LoadWarehouses/50) * time.Minute
	case multiLoadgen:
		t.l.Printf("splitting, scattering, and partitioning\n")
		partArgs = fmt.Sprintf(`--partitions=%d --zones="%s" --partition-affinity=0`,
			b.partitions(), strings.Join(b.Distribution.zones(), ","))
		rebalanceWait = time.Duration(b.LoadWarehouses/20) * time.Minute
	default:
		panic("unexpected")
	}

	t.l.Printf("waiting %v for rebalancing\n", rebalanceWait)
	_, err := db.ExecContext(ctx, `SET CLUSTER SETTING kv.snapshot_rebalance.max_rate='64MiB'`)
	if err != nil {
		return err
	}

	// Split and scatter the tables. Ramp up to the expected load in the desired
	// distribution. This should allow for load-based rebalancing to help
	// distribute load. Optionally pass some load configuration-specific flags.
	cmd = fmt.Sprintf("./workload run tpcc --warehouses=%d --workers=%d --split --scatter "+
		"--wait=false --duration=%s --tolerate-errors %s {pgurl%s}",
		b.LoadWarehouses, b.LoadWarehouses, rebalanceWait, partArgs, roachNodes)
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
	numRoachNodes := b.Nodes
	numLoadGroups := b.LoadConfig.numLoadNodes(b.Distribution)
	numZones := len(b.Distribution.zones())
	roachNodesPerGroup := numRoachNodes / numLoadGroups
	nodesPerGroup := roachNodesPerGroup + 1 // nodesPerGroup * numLoadGroups == c.nodes
	zonesPerGroup := numZones / numLoadGroups
	// Roachprod round-robins the allocation of nodes across zones. When running
	// a single load group across zones, the first zone will always have one more
	// node than the others. This extra node is the load node.
	loadNodePerGroup := roachNodesPerGroup / zonesPerGroup
	loadGroups := make([]struct{ roachNodes, loadNodes nodeListOption }, numLoadGroups)
	for i := range loadGroups {
		for j := 0; j < nodesPerGroup; j++ {
			n := c.Node(i*nodesPerGroup + j + 1)
			if j == loadNodePerGroup {
				loadGroups[i].loadNodes = loadGroups[i].loadNodes.merge(n)
			} else {
				loadGroups[i].roachNodes = loadGroups[i].roachNodes.merge(n)
			}
		}
	}

	// Aggregate nodes across load groups.
	var roachNodes nodeListOption
	var loadNodes nodeListOption
	for _, g := range loadGroups {
		roachNodes = roachNodes.merge(g.roachNodes)
		loadNodes = loadNodes.merge(g.loadNodes)
	}

	c.Put(ctx, cockroach, "./cockroach", roachNodes)
	c.Put(ctx, workload, "./workload", loadNodes)
	c.Start(ctx, t, append(b.startOpts(), roachNodes)...)

	useHAProxy := b.Chaos
	if useHAProxy {
		if len(loadNodes) > 1 {
			t.Fatal("distributed chaos benchmarking not supported")
		}
		t.Status("installing haproxy")
		c.Install(ctx, loadNodes, "haproxy")
		c.Put(ctx, cockroach, "./cockroach", loadNodes)
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
						extraFlags = fmt.Sprintf(` --partitions=%d --split`, b.partitions())
					case multiLoadgen:
						extraFlags = fmt.Sprintf(" --partitions=%d --partition-affinity=%d --split",
							b.partitions(), groupIdx)
						activeWarehouses = warehouses / numLoadGroups
					default:
						panic("unexpected")
					}

					t.Status(fmt.Sprintf("running benchmark, warehouses=%d", warehouses))
					histogramsPath := fmt.Sprintf("logs/warehouses=%d/stats.json", activeWarehouses)
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
					c.Get(ctx, histogramsPath, roachtestHistogramsPath, group.loadNodes)
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
	c.Stop(ctx, c.All())
}

func registerTPCCBench(r *registry) {
	specs := []tpccBenchSpec{
		{
			Nodes: 3,
			CPUs:  4,

			LoadWarehouses: 1000,
			EstimatedMax:   325,
			// TODO(nvanbenschoten): Need to regenerate.
			// StoreDirVersion: "2.0-5",
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
			// TODO(nvanbenschoten): Need to regenerate.
			// StoreDirVersion: "2.0-5",
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
