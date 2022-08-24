// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package admission

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/guptarohit/asciigraph"
	"github.com/stretchr/testify/require"
)

// TestSchedulerLatencyListener is a data-driven test for the component in
// admission control that reacts to scheduling latencies. It gives package
// authors a way to understand how the elastic CPU % gets adjusted in response
// to changes in scheduler latencies. The following syntax is provided:
//
// - "init" [limit=<percent>]
//   Initialize the scheduler latency listener and adjuster with optional
//   initial limit (defaulting to 25%).
//
// - "params" [ewma_c=<float>] [target_p99=<duration>] [min_util=<float>] \
//    [max_util=<float>] [delta=<float>] [factor=<float>] \
//    [min_util_fraction=<float>]
//   Configure the listener's various parameters.
//
// - "tick"
//    p99=<duration> [util-fraction=[+|-]<float>|util-lag=<int>] [ticks=<int>]
//    ....
//   Invoke the listener with the specified p99 latency a specific number of
//   times (default = 1). Optionally control the utilization fraction (of the
//   configured limit) over that tick, and also specify a "lag" term -- pick the
//   limit from the specified number of ticks ago if available. To increase or
//   decrease utilization gradually (within [0.0, 1.0]), use the +/- sign.
//
// - "plot" [height=<int>] [width=<int>]
//   Visually renders what the controller output (elastic CPU utilization limit
//   %) looks like over time, given the controller inputs (scheduling latencies,
//   observed utilization).
//
// - "auto" ticks=<int> set-point=<percentage> [m=<int>] [c=<int>]
//   Mode where you can specify a "set-point" elastic CPU limit; if utilization
//   is higher than the set-point, scheduling latency is higher than the target
//   threshold, and vice versa. The tick count determines how long to simulate
//   for. The latency function is linear with some randomization mixed in, and
//   can be controlled by the "m" and "c" respectively.
//
//       Y = mx + C
//
//   Where x is absolute difference between the set-point and observed
//   utilization and m the multiplier applied to it. C is a random variable
//   with expected value of 0, by can go as high as +/- the c provided. The Y
//   term here is the delta we apply to the latency target we're parametrized
//   to meet.
//
func TestSchedulerLatencyListener(t *testing.T) {
	ambientCtx := log.MakeTestingAmbientCtxWithNewTracer()
	st := cluster.MakeTestingClusterSettings()
	metrics := makeSchedulerLatencyListenerMetrics()
	dir := testutils.TestDataPath(t, "scheduler_latency_listener")
	const period = time.Second
	datadriven.Walk(t, dir, func(t *testing.T, path string) {
		var (
			adjuster        *testElasticCPUUtilizationAdjuster
			latencyListener *schedulerLatencyListener
			params          schedulerLatencyListenerParams
		)
		var ( // plotted data points
			utilLimitPercents, utilPercents []float64
			p99Latencies, p99LatencyTargets []float64
		)
		utilFrac, utilDelta, utilLag := 0.0, 0.0, 0
		rnd := rand.New(rand.NewSource(42))

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				limitUtilPercent := 25.0
				if d.HasArg("limit") {
					var limitUtilStr string
					d.ScanArgs(t, "limit", &limitUtilStr)
					limitUtilStr = strings.TrimSuffix(limitUtilStr, "%")
					var err error
					limitUtilPercent, err = strconv.ParseFloat(limitUtilStr, 64)
					require.NoError(t, err)
				}
				utilPercent := limitUtilPercent * utilFrac

				adjuster = &testElasticCPUUtilizationAdjuster{}
				adjuster.setUtilizationLimit(limitUtilPercent / 100)
				adjuster.setUtilization(utilPercent / 100)

				latencyListener = newSchedulerLatencyListener(ambientCtx, st, metrics, adjuster)
				params = latencyListener.getParams(period)
				params.enabled = true
				latencyListener.testingParams = &params

			case "params":
				if d.HasArg("target_p99") {
					var targetP99Str string
					d.ScanArgs(t, "target_p99", &targetP99Str)
					targetP99, err := time.ParseDuration(targetP99Str)
					require.NoError(t, err)
					params.targetP99 = targetP99
				}

				for _, floatArgKey := range []string{
					"ewma-c", "min-util", "max-util", "delta", "factor", "min-util-fraction",
				} {
					if !d.HasArg(floatArgKey) {
						continue
					}
					var floatStr string
					d.ScanArgs(t, floatArgKey, &floatStr)
					isPercentage := strings.HasSuffix(floatStr, "%")
					if isPercentage {
						floatStr = strings.TrimSuffix(floatStr, "%")
					}
					floatVal, err := strconv.ParseFloat(floatStr, 64)
					require.NoError(t, err)
					if isPercentage {
						floatVal = floatVal / 100
					}

					switch floatArgKey {
					case "ewma-c":
						params.ewmaConstant = floatVal
					case "min-util":
						params.minUtilization = floatVal
					case "max-util":
						params.maxUtilization = floatVal
					case "delta":
						params.additiveDelta = floatVal
					case "factor":
						params.multiplicativeFactorOnDecrease = floatVal
					case "min-util-fraction":
						params.utilizationFractionForAdditionalCPU = floatVal
					}
				}
				return params.String()

			case "tick":
				require.NotNilf(t, latencyListener, "uninitialized latency listener (did you use 'init'?)")

				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)
					p99, err := time.ParseDuration(strings.TrimPrefix(strings.TrimSpace(parts[0]), "p99="))
					require.NoError(t, err)

					ticks := 1
					for _, part := range parts[1:] {
						part = strings.TrimSpace(part)

						if strings.HasPrefix(part, "ticks=") {
							part = strings.TrimPrefix(part, "ticks=")
							var err error
							ticks, err = strconv.Atoi(part)
							require.NoError(t, err)
							continue
						}

						if strings.HasPrefix(part, "util-lag=") {
							part = strings.TrimPrefix(part, "util-lag=")
							var err error
							utilLag, err = strconv.Atoi(part)
							require.NoError(t, err)
							continue
						}

						if strings.HasPrefix(part, "util-fraction=") {
							part = strings.TrimPrefix(part, "util-fraction=")

							isDelta := strings.HasPrefix(part, "+") || strings.HasPrefix(part, "-")
							isPositive := strings.Contains(part, "+")
							if isDelta {
								part = strings.TrimPrefix(part, "+")
								part = strings.TrimPrefix(part, "-")
							}

							floatVal, err := strconv.ParseFloat(part, 64)
							require.NoError(t, err)

							if isDelta {
								if isPositive {
									utilDelta = floatVal
								} else {
									utilDelta = -floatVal
								}
							} else {
								utilDelta = 0.0
								utilFrac = floatVal
							}
							continue
						}

					}

					for i := 0; i < ticks; i++ {
						if len(utilLimitPercents) > utilLag {
							limitUtilPercent := utilLimitPercents[len(utilLimitPercents)-1-utilLag]

							utilFrac += utilDelta
							if utilFrac > 1.0 {
								utilFrac = 1.0
							}
							if utilFrac < 0.0 {
								utilFrac = 0.0
							}
							utilPercent := limitUtilPercent * utilFrac

							adjuster.setUtilization(utilPercent / 100)
						}

						latencyListener.SchedulerLatency(p99, period)
						utilLimitPercents = append(utilLimitPercents, 100*adjuster.getUtilizationLimit())
						utilPercents = append(utilPercents, 100*adjuster.getUtilization())
						p99Latencies = append(p99Latencies, float64(p99.Microseconds()))
						p99LatencyTargets = append(p99LatencyTargets, float64(params.targetP99.Microseconds()))
					}
				}
				return ""

			case "auto":
				var ticks int
				d.ScanArgs(t, "ticks", &ticks)

				var setPointUtilStr string
				d.ScanArgs(t, "set-point", &setPointUtilStr)
				setPointUtilStr = strings.TrimSuffix(setPointUtilStr, "%")
				steadyStateUtilPercent, err := strconv.ParseFloat(setPointUtilStr, 64)
				require.NoError(t, err)

				m, c := 20, 200
				if d.HasArg("m") {
					d.ScanArgs(t, "m", &m)
				}
				if d.HasArg("c") {
					d.ScanArgs(t, "c", &c)
				}

				p99 := params.targetP99
				// We're simulating the specified number of ticks. We "remember"
				// the utilDelta, utilFrac, utilLag values from earlier, and
				// they're maintained per-iteration below. This allows the auto
				// mode to "take over" after specific values for utilDelta,
				// utilFrac, etc. have been set manually through the "tick"
				// directive.
				for i := 0; i < ticks; i++ {
					if len(utilLimitPercents) > utilLag {
						limitUtilPercent := utilLimitPercents[len(utilLimitPercents)-1-utilLag]

						utilFrac += utilDelta
						if utilFrac > 1.0 {
							utilFrac = 1.0
						}
						if utilFrac < 0.0 {
							utilFrac = 0.0
						}

						utilPercent := limitUtilPercent * utilFrac
						adjuster.setUtilization(utilPercent / 100)
					}

					latencyListener.SchedulerLatency(p99, period)
					utilLimitPercents = append(utilLimitPercents, 100*adjuster.getUtilizationLimit())
					utilPercents = append(utilPercents, 100*adjuster.getUtilization())
					p99Latencies = append(p99Latencies, float64(p99.Microseconds()))
					p99LatencyTargets = append(p99LatencyTargets, float64(params.targetP99.Microseconds()))

					diff := 100*adjuster.getUtilizationLimit() - steadyStateUtilPercent
					y := math.RoundToEven(float64(m)*math.Abs(diff) + float64(rnd.Intn(2*c)-c))
					if diff >= 0.1 {
						p99 = params.targetP99 + time.Duration(y)*time.Microsecond
					} else {
						p99 = params.targetP99 - time.Duration(y)*time.Microsecond
					}
					if p99.Microseconds() < 50 {
						p99 = 50 * time.Microsecond // floor
					}
				}

			case "plot":
				if len(utilPercents) == 0 || len(utilLimitPercents) == 0 || len(p99Latencies) == 0 {
					return "error: can't plot empty {limits,observed,p99Latencies}"
				}
				var h, w, p = 10, 30, 1
				var options []asciigraph.Option
				if d.HasArg("height") {
					d.ScanArgs(t, "height", &h)
				}
				if d.HasArg("width") {
					d.ScanArgs(t, "width", &w)
				}
				if d.HasArg("precision") {
					d.ScanArgs(t, "precision", &p)
				}
				options = append(options, asciigraph.Height(h))
				options = append(options, asciigraph.Width(w))
				options = append(options, asciigraph.Precision(uint(p)))
				var buf strings.Builder
				buf.WriteString(asciigraph.PlotMany([][]float64{p99Latencies, p99LatencyTargets},
					append(options, asciigraph.Caption("p99 scheduler latencies (μs)"))...,
				))
				buf.WriteString("\n\n\n")
				buf.WriteString(asciigraph.PlotMany([][]float64{utilLimitPercents, utilPercents},
					append(options, asciigraph.Caption("elastic cpu utilization and limit (%)"))...,
				))
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
			return ""
		})
	})
}

type testElasticCPUUtilizationAdjuster struct {
	utilizationLimit, observedUtilization float64
}

var _ elasticCPUUtilizationAdjuster = &testElasticCPUUtilizationAdjuster{}

func (t *testElasticCPUUtilizationAdjuster) getUtilizationLimit() float64 {
	return t.utilizationLimit
}

func (t *testElasticCPUUtilizationAdjuster) setUtilizationLimit(limit float64) {
	t.utilizationLimit = limit
}

func (t *testElasticCPUUtilizationAdjuster) getUtilization() float64 {
	return t.observedUtilization
}

func (t *testElasticCPUUtilizationAdjuster) setUtilization(observed float64) {
	t.observedUtilization = observed
}

func (p schedulerLatencyListenerParams) String() string {
	return fmt.Sprintf(
		"ewma-c            = %0.2f\n"+
			"target-p99        = %s\n"+
			"min-util          = %0.2f%%\n"+
			"max-util          = %0.2f%%\n"+
			"delta             = %0.2f%%\n"+
			"factor            = %0.2f\n"+
			"min-util-fraction = %0.2f%%",
		p.ewmaConstant, p.targetP99, p.minUtilization*100, p.maxUtilization*100, p.additiveDelta*100,
		p.multiplicativeFactorOnDecrease, p.utilizationFractionForAdditionalCPU*100,
	)
}
