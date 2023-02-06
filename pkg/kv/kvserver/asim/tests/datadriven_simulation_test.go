// Copyright 2023 The Cockroach Authors.
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
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/guptarohit/asciigraph"
	"github.com/stretchr/testify/require"
)

// TestDataDriven is a data-driven test for the allocation system using the
// simulator. It gives contributors a way to understand how a cluster reacts to
// different settings, load and configuration. In addition, it may be used for
// regression and exploratory testing with parameterized random generation and
// assertions. The following syntax is provided.
//
//   - "gen_load" [rw_ratio=<float>] [rate=<float>] [access_skew=<bool>]
//     [min_block=<int>] [max_block=<int>] [min_key=<int>] [max_key=<int>]
//     Initialize the load generator with parameters. On the next call to eval,
//     the load generator is called to create the workload used in the
//     simulation. The default values are: rw_ratio=0 rate=0 min_block=1
//     max_block=1 min_key=1 max_key=10_000 access_skew=false.
//
//   - "gen_state" [stores=<int>] [ranges=<int>] [placement_skew=<bool>]
//     [repl_factor=<int>] [keyspace=<int>]
//     Initialize the state generator parameters. On the next call to eval, the
//     state generator is called to create the initial state used in the
//     simulation. The default values are: stores=3 ranges=1 repl_factor=3
//     placement_skew=false keyspace=10000.
//
//   - "assertion" type=<string> stat=<string> ticks=<int> threshold=<float>
//     Add an assertion to the list of assertions that run against each
//     sample on subsequent calls to eval. When every assertion holds during eval,
//     OK is printed, otherwise the reason the assertion(s) failed is printed.
//     There are two types of assertions available, type=balance and type=steady.
//     Both assertions only look at the last 'ticks' duration of the simulation
//     run.
//
//     For type=balance assertions, the max stat (e.g. stat=qps) value of each
//     store is taken and divided by the mean store stat value. If the max/mean
//     exceeds the threshold provided (e.g. threshold=1.15) during any of the
//     last ticks (e.g. ticks=5), then the assertion fails. This assertion
//     applies over all stores, at each tick, for the last 'ticks' duration.
//
//     For type=steady assertions, if the max or min stat (e.g. stat=replicas)
//     value over the last ticks (e.g. ticks=6) duration is greater than
//     threshold (e.g. threshold=0.05) % of the mean, the assertion fails. This
//     assertion applies per-store, over 'ticks' duration.
//
//   - "setting" [rebalance_mode=<int>] [rebalance_interval=<duration>]
//     [rebalance_qps_threshold=<float>] [split_qps_threshold=<float>]
//     [rebalance_range_threshold=<float>] [gossip_delay=<duration>]
//     Configure the simulation's various settings. The default values are:
//     rebalance_mode=2 (leases and replicas) rebalance_interval=1m (1 minute)
//     rebalance_qps_threshold=0.1 split_qps_threshold=2500
//     rebalance_range_threshold=0.05 gossip_delay=500ms.
//
//   - "eval" [duration=<string>] [samples=<int>] [seed=<int>]
//     Run samples (e.g. samples=5) number of simulations for duration (e.g.
//     duration=10m) time and evaluate every prior declared assertion against
//     each sample. The seed (e.g. seed=42) given will be used to create a new
//     random number generator that creates the seed used to generate each
//     simulation sample. The default values are: duration=30m (30 minutes)
//     samples=1 seed=random.
//
//   - "plot" stat=<string> [sample=<int>] [height=<int>] [width=<int>]
//     Visually renders the stat (e.g. stat=qps) as a series where the x axis
//     is the simulated time and the y axis is the stat value. A series is
//     rendered per-store, so if there are 10 stores, 10 series will be
//     rendered.
func TestDataDriven(t *testing.T) {
	ctx := context.Background()
	dir := datapathutils.TestDataPath(t, ".")
	datadriven.Walk(t, dir, func(t *testing.T, path string) {
		const defaultKeyspace = 10000
		loadGen := gen.BasicLoad{}
		stateGen := gen.BasicState{}
		settingsGen := gen.StaticSettings{Settings: config.DefaultSimulationSettings()}
		assertions := []SimulationAssertion{}
		runs := []asim.History{}
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "gen_load":
				var rwRatio, rate = 0.0, 0.0
				var minBlock, maxBlock = 1, 1
				var minKey, maxKey = int64(1), int64(defaultKeyspace)
				var accessSkew bool

				scanIfExists(t, d, "rw_ratio", &rate)
				scanIfExists(t, d, "rate", &rwRatio)
				scanIfExists(t, d, "access_skew", &accessSkew)
				scanIfExists(t, d, "min_block", &minBlock)
				scanIfExists(t, d, "max_block", &maxBlock)
				scanIfExists(t, d, "min_key", &minKey)
				scanIfExists(t, d, "max_key", &maxKey)

				loadGen.SkewedAccess = accessSkew
				loadGen.MinKey = minKey
				loadGen.MaxKey = maxKey
				loadGen.RWRatio = rwRatio
				loadGen.Rate = rate
				loadGen.MaxBlockSize = maxBlock
				loadGen.MinBlockSize = minBlock
				return ""
			case "gen_state":
				var stores, ranges, replFactor, keyspace = 3, 1, 3, defaultKeyspace
				var placementSkew bool

				scanIfExists(t, d, "stores", &stores)
				scanIfExists(t, d, "ranges", &ranges)
				scanIfExists(t, d, "repl_factor", &replFactor)
				scanIfExists(t, d, "placement_skew", &placementSkew)
				scanIfExists(t, d, "keyspace", &keyspace)

				stateGen.Stores = stores
				stateGen.ReplicationFactor = replFactor
				stateGen.KeySpace = keyspace
				stateGen.Ranges = ranges
				stateGen.SkewedPlacement = placementSkew
				return ""
			case "eval":
				samples := 1
				seed := rand.Int63()
				duration := 30 * time.Minute
				failureExists := false

				scanIfExists(t, d, "duration", &duration)
				scanIfExists(t, d, "samples", &samples)
				scanIfExists(t, d, "seed", &seed)

				seedGen := rand.New(rand.NewSource(seed))
				sampleAssertFailures := make([]string, samples)
				// TODO(kvoli): Samples are evaluated sequentially (no
				// concurrency). Add a evaluator component which concurrently
				// evaluates samples with the option to stop evaluation early
				// if an assertion fails.
				for sample := 0; sample < samples; sample++ {
					assertionFailures := []string{}
					simulator := gen.GenerateSimulation(
						duration, stateGen, loadGen, settingsGen, seedGen.Int63(),
					)
					simulator.RunSim(ctx)
					history := simulator.History()
					runs = append(runs, history)
					for _, assertion := range assertions {
						if holds, reason := assertion.Assert(ctx, history); !holds {
							failureExists = true
							assertionFailures = append(assertionFailures, reason)
						}
					}
					sampleAssertFailures[sample] = strings.Join(assertionFailures, "")
				}

				// Every sample passed every assertion.
				if !failureExists {
					return "OK"
				}

				// There exists a sample where some assertion didn't hold. For
				// each sample that had at least one failing assertion, report
				// the sample and every failing assertion.
				buf := strings.Builder{}
				for sample, failString := range sampleAssertFailures {
					if failString != "" {
						fmt.Fprintf(&buf, "failed assertion sample %d\n%s",
							sample+1, failString)
					}
				}
				return buf.String()
			case "assertion":
				var stat string
				var typ string
				var ticks int
				var threshold float64

				scanArg(t, d, "type", &typ)
				scanArg(t, d, "stat", &stat)
				scanArg(t, d, "ticks", &ticks)
				scanArg(t, d, "threshold", &threshold)

				switch typ {
				case "balance":
					assertions = append(assertions, balanceAssertion{
						ticks:     ticks,
						stat:      stat,
						threshold: threshold,
					})
				case "steady":
					assertions = append(assertions, steadyStateAssertion{
						ticks:     ticks,
						stat:      stat,
						threshold: threshold,
					})
				}
				return ""
			case "setting":
				scanIfExists(t, d, "rebalance_mode", &settingsGen.Settings.LBRebalancingMode)
				scanIfExists(t, d, "rebalance_interval", &settingsGen.Settings.LBRebalancingInterval)
				scanIfExists(t, d, "rebalance_qps_threshold", &settingsGen.Settings.LBRebalanceQPSThreshold)
				scanIfExists(t, d, "split_qps_threshold", &settingsGen.Settings.SplitQPSThreshold)
				scanIfExists(t, d, "rebalance_range_threshold", &settingsGen.Settings.RangeRebalanceThreshold)
				scanIfExists(t, d, "gossip_delay", &settingsGen.Settings.StateExchangeDelay)
				return ""
			case "plot":
				var stat string
				var height, width, sample = 15, 80, 1
				var buf strings.Builder

				scanArg(t, d, "stat", &stat)
				scanArg(t, d, "sample", &sample)
				scanIfExists(t, d, "height", &height)
				scanIfExists(t, d, "width", &width)

				require.GreaterOrEqual(t, len(runs), sample)

				history := runs[sample-1]
				ts := metrics.MakeTS(history.Recorded)
				statTS := ts[stat]
				buf.WriteString("\n")
				buf.WriteString(asciigraph.PlotMany(
					statTS,
					asciigraph.Caption(stat),
					asciigraph.Height(height),
					asciigraph.Width(width),
				))
				buf.WriteString("\n")
				return buf.String()
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

// TODO(kvoli): Upstream the scan implementations for the float64 and
// time.Duration types to the datadriven testing repository.
func scanArg(t *testing.T, d *datadriven.TestData, key string, dest interface{}) {
	var tmp string
	var err error
	switch dest := dest.(type) {
	case *time.Duration:
		d.ScanArgs(t, key, &tmp)
		*dest, err = time.ParseDuration(tmp)
		require.NoError(t, err)
	case *float64:
		d.ScanArgs(t, key, &tmp)
		*dest, err = strconv.ParseFloat(tmp, 64)
		require.NoError(t, err)
	case *string, *int, *int64, *uint64, *bool:
		d.ScanArgs(t, key, dest)
	default:
		require.Fail(t, "unsupported type %T", dest)
	}
}

func scanIfExists(t *testing.T, d *datadriven.TestData, key string, dest interface{}) {
	if d.HasArg(key) {
		scanArg(t, d, key, dest)
	}
}
