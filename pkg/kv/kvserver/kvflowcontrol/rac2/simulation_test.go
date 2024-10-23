// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rac2

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/asciitsdb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/dustin/go-humanize"
	"github.com/guptarohit/asciigraph"
	"github.com/mkungla/bexp/v3"
	"github.com/stretchr/testify/require"
)

// TestUsingSimulation is a data-driven test for the RangeController. It allows
// package authors to understand how flow tokens are maintained for individual
// replication streams, how write bandwidth is shaped by these tokens, and how
// requests queue/dequeue internally.
//
// NOTE: This test is largely taken from kvflowcontrol/kvflowsimulator and
// attempts to conform to the same interface, so that the outputs can be
// compared. It operates without ever calling WaitForEval, as it's not
// deterministic, instead testingNonBlockingAdmit is used to simulate
// pre-evaluation admission. The following is a list of  notable divergences
// from how rac2 is used in practice:
//
//   - WaitForEval is replaced with testingNonBlockingAdmit.
//   - testingNonBlockingAdmit waits for flow tokens to be available on all
//     connected streams, regardless of work class.
//   - eval_wait metrics are handled via testing functions, rather than updated
//     via WaitForEval.
//   - Tokens can be deducted/returned to a stream independently of the
//     underlying RangeController, directly against a stream token counter,
//     when using form A of the timeline directive.
//
// The test provides the following commands:
//
//   - "init"
//     [handle=<string>]
//     ...
//     Initialize the test simulator. Optionally, initialize range controllers
//     for a given handle (mapping to a range_id), for subsequent use.
//
//   - "timeline"
//     t=[<duration>,<duration>) class={regular,elastic} \
//     stream=t<int>/s<int> adjust={+,-}<bytes>/s rate=<int>/s \
//     [deduction-delay=<duration>]                                          (A)
//     ....
//     t=[<duration>,<duration>) handle=<string> class={regular,elastic} \
//     adjust={+,-}<bytes>/s rate=<int>/s [stream=t<int>/s<int>] \
//     [deduction-delay=<duration>]                                          (B)
//     ....
//     t=<duration> handle=<string> op=connect stream=t<int>/s<int>          (C)
//     ....
//     t=<duration> handle=<string> op=disconnect stream=t<int>/s<int>       (D)
//     ....
//     t=<duration> handle=<string> op={snapshot,close}                      (E)
//     ....
//
//     Set up timelines to simulate. There are a few forms:
//
//     A. Creates a "thread" that operates during the given time range
//     t=[tstart,tend), issuing the specified 'rate' of requests of the given
//     work 'class', over the given 'stream', where the flow tokens are
//     {deducted,returned} with the given bandwidth. The 'rate' controls the
//     granularity of token adjustment, i.e. if adjust=+100bytes/s and
//     rate=5/s, then each return adjusts by +100/5 = +20bytes. If flow tokens
//     are being deducted (-ve 'adjust'), they go through
//     testingNonBlockingAdmit() followed by testingDeductTokens(). If they're
//     being returned (+ve 'adjust'), they simply go through
//     testingReturnTokens(). The optional 'deduction-delay' parameter controls
//     the number of ticks between each request being granted admission and it
//     deducting the corresponding flow tokens.
//
//     B. Similar to A except using a given handle corresponding to an
//     underlying range's RangeController instead, which internally deducts
//     tokens from all connected streams or if returning tokens, does so for
//     the named stream. Token deductions from a range's RangeController are
//     tied to monotonically increasing raft log positions starting from the
//     position the underlying stream was connected to (using C). When
//     returning tokens, we translate the byte token value to the corresponding
//     raft log prefix (token returns with handle are in terms of raft log
//     positions).
//
//     C. Connects the handle's RangeController to the specific stream,
//     starting at the given log position. Subsequent token deductions using
//     the RangeController will deduct from the given stream. Underneath, this
//     ensures a replica send stream is created for a replica on the given
//     stream.
//
//     D. Disconnects the specific stream from the named handle's
//     RangeController. All deducted flow tokens (using the given handle) from
//     that specific stream are released. Future token deductions/returns (when
//     using the given handle) don't deduct from/return to the stream.
//
//     E. Close or snapshot the named handle, corresponding to a
//     RangeController. When closing a RangeController, all deducted flow
//     tokens are released and subsequent operations are noops. Snapshots
//     record the internal state (what tokens have been
//     deducted-but-not-returned, and for what log positions). This can later
//     be rendered using the "snapshot" directive.
//
//   - "simulate" [t=[<duration>,<duration>)]
//     Simulate timelines until the optionally specified timestamp.
//
//   - "plot" [height=<int>] [width=<int>] [precision=<int>] \
//     [t=[<duration>,<duration>)]
//     <metric-selector> unit=<string> [rate=true]
//     ....
//     Plot the flow controller specified metrics (and optionally its rate of
//     change) with the specified units. The following metrics are supported:
//     a. kvflowcontrol.tokens.{eval,send}.{regular,elastic}.{available,deducted,returned}
//     b. kvflowcontrol.streams.{eval,send}.{regular,elastic}.{blocked,total}_count
//     c. kvflowcontrol.eval_wait.{regular,elastic}.requests.{waiting,admitted,errored}
//     d. kvflowcontrol.eval_wait.{regular,elastic}.duration
//     To overlay metrics onto the same plot, the selector supports curly brace
//     expansion. If the unit is one of {MiB,MB,KB,KiB,s,ms,us,μs}, or the
//     bandwidth equivalents (<byte size>/s), the y-axis is automatically
//     converted.
//
//   - "snapshots" handle=<string>
//     Render any captured "snapshots" for the given handle's RangeController.
//
// This test uses a non-blocking interface for the RangeController to support
// deterministic simulation.
func TestUsingSimulation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, datapathutils.TestDataPath(t, "simulation"), func(t *testing.T, path string) {
		var (
			sim *simulator
			// Used to map named handles to their respective range IDs.
			rangeIDSeq      roachpb.RangeID
			handleToRangeID map[string]roachpb.RangeID
		)
		ctx := context.Background()

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				sim = &simulator{}
				sim.init(t, ctx)
				defer sim.state.stopper.Stop(ctx)
				handleToRangeID = make(map[string]roachpb.RangeID)

				for _, line := range strings.Split(d.Input, "\n") {
					handle := strings.TrimPrefix(line, "handle=")
					rangeIDSeq++
					rangeID := rangeIDSeq
					handleToRangeID[handle] = rangeID
					sim.state.getOrInitRange(t, makeSingleVoterTestingRange(
						rangeID, testingLocalTenantID, testingLocalNodeID, testingLocalStoreID), MsgAppPush)
				}

			case "timeline":
				require.NotNil(t, sim, "unintialized simulator (did you use 'init'?)")
				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)

					var tl timeline
					for i := range parts {
						parts[i] = strings.TrimSpace(parts[i])
						inner := strings.Split(parts[i], "=")
						require.Len(t, inner, 2)
						arg := strings.TrimSpace(inner[1])

						switch {
						case strings.HasPrefix(parts[i], "t="):
							// Parse t={<duration>,[<duration>,<duration>)}.
							ranged := strings.HasPrefix(arg, "[")
							if ranged {
								arg = strings.TrimSuffix(strings.TrimPrefix(arg, "["), ")")
								args := strings.Split(arg, ",")
								dur, err := time.ParseDuration(args[0])
								require.NoError(t, err)
								tl.tstart = sim.state.ts.Now().Add(dur)
								dur, err = time.ParseDuration(args[1])
								require.NoError(t, err)
								tl.tend = sim.state.ts.Now().Add(dur)
							} else {
								dur, err := time.ParseDuration(arg)
								require.NoError(t, err)
								tl.tstart = sim.state.ts.Now().Add(dur)
							}

						case strings.HasPrefix(parts[i], "class="):
							// Parse class={regular,elastic}.
							switch arg {
							case "regular":
								tl.pri = admissionpb.NormalPri
							case "elastic":
								tl.pri = admissionpb.BulkNormalPri
							default:
								t.Fatalf("unexpected class: %s", parts[i])
							}

						case strings.HasPrefix(parts[i], "stream="):
							// Parse stream=t<int>/s<int>.
							inner := strings.Split(arg, "/")
							require.Len(t, inner, 2)
							ti, err := strconv.Atoi(strings.TrimPrefix(inner[0], "t"))
							require.NoError(t, err)
							si, err := strconv.Atoi(strings.TrimPrefix(inner[1], "s"))
							require.NoError(t, err)
							tl.stream = kvflowcontrol.Stream{
								TenantID: roachpb.MustMakeTenantID(uint64(ti)),
								StoreID:  roachpb.StoreID(si),
							}

						case strings.HasPrefix(parts[i], "adjust="):
							// Parse adjust={+,-}<bytes>/s.
							isPositive := strings.Contains(arg, "+")
							arg = strings.TrimPrefix(arg, "+")
							arg = strings.TrimPrefix(arg, "-")
							bytes, err := humanize.ParseBytes(strings.TrimSuffix(arg, "/s"))
							require.NoError(t, err)
							tl.delta = kvflowcontrol.Tokens(int64(bytes))
							if !isPositive {
								tl.delta = -tl.delta
							}

						case strings.HasPrefix(parts[i], "rate="):
							// Parse rate=<int>/s.
							var err error
							tl.rate, err = strconv.Atoi(strings.TrimSuffix(arg, "/s"))
							require.NoError(t, err)

						case strings.HasPrefix(parts[i], "deduction-delay="):
							// Parse deduction-delay=<duration>.
							dur, err := time.ParseDuration(arg)
							require.NoError(t, err)
							tl.deductionDelay = int(dur.Nanoseconds() / testingTick.Nanoseconds())

						case strings.HasPrefix(parts[i], "handle="):
							// Parse handle=<string>.
							var ok bool
							rangeID, ok := handleToRangeID[arg]
							require.True(t, ok, "expected to find handle %q, was it initialized?", arg)
							tl.handle = sim.state.ranges[rangeID]

						case strings.HasPrefix(parts[i], "op="):
							// Parse op=<string>.
							require.True(t, arg == "connect" || arg == "disconnect" ||
								arg == "close" || arg == "snapshot")
							tl.rcOp = arg

						default:
							t.Fatalf("unrecognized prefix: %s", parts[i])
						}
					}

					sim.timeline(tl)
				}

			case "simulate":
				require.NotNilf(t, sim, "uninitialized simulator (did you use 'init'?)")
				var end time.Time
				if d.HasArg("t") {
					// Parse t=[<duration>,<duration>), but ignoring the
					// start time.
					var tstr string
					d.ScanArgs(t, "t", &tstr)
					tstr = strings.TrimSuffix(strings.TrimPrefix(tstr, "["), ")")
					args := strings.Split(tstr, ",")
					dur, err := time.ParseDuration(args[1])
					require.NoError(t, err)
					end = sim.state.ts.Now().Add(dur)
				}
				sim.simulate(ctx, end)
				return ""

			case "metric_names":
				var buf strings.Builder
				for _, name := range sim.tsdb.RegisteredMetricNames() {
					buf.WriteString(fmt.Sprintf("%s\n", name))
				}
				return buf.String()

			case "plot":
				var h, w, p = 15, 40, 1
				if d.HasArg("height") {
					d.ScanArgs(t, "height", &h)
				}
				if d.HasArg("width") {
					d.ScanArgs(t, "width", &w)
				}
				if d.HasArg("precision") {
					d.ScanArgs(t, "precision", &p)
				}

				var buf strings.Builder
				for i, line := range strings.Split(d.Input, "\n") {
					var (
						selector, unit string
						rated          bool
					)
					parts := strings.Fields(line)
					for i, part := range parts {
						part = strings.TrimSpace(part)
						if i == 0 {
							selector = part
							continue
						}

						if strings.HasPrefix(part, "rate=") {
							var err error
							rated, err = strconv.ParseBool(strings.TrimPrefix(part, "rate="))
							require.NoError(t, err)
						}

						if strings.HasPrefix(part, "unit=") {
							unit = strings.TrimPrefix(part, "unit=")
						}
					}

					caption := strings.TrimPrefix(selector, "kvflowcontrol.")
					if rated {
						caption = fmt.Sprintf("rate(%s)", caption)
					}
					caption = fmt.Sprintf("%s (%s)", caption, unit)

					options := []asciitsdb.Option{
						asciitsdb.WithGraphOptions(
							asciigraph.Height(h),
							asciigraph.Width(w),
							asciigraph.Precision(uint(p)),
							asciigraph.Caption(caption),
						),
					}
					if rated {
						options = append(options, asciitsdb.WithRate(int(time.Second/testingMetricTick)))
					}
					switch unit {
					case "μs", "us", "microseconds":
						options = append(options, asciitsdb.WithDivisor(float64(time.Microsecond.Nanoseconds())) /* ns => μs conversion  */)
					case "ms", "milliseconds":
						options = append(options, asciitsdb.WithDivisor(float64(time.Millisecond.Nanoseconds())) /* ns => μs conversion  */)
					case "s", "seconds":
						options = append(options, asciitsdb.WithDivisor(float64(time.Second.Nanoseconds())) /* ns => μs conversion  */)
					case "MiB", "MiB/s":
						options = append(options, asciitsdb.WithDivisor(humanize.MiByte) /* 1 MiB */)
					case "MB", "MB/s":
						options = append(options, asciitsdb.WithDivisor(humanize.MByte) /* 1 MB */)
					case "KiB", "KiB/s":
						options = append(options, asciitsdb.WithDivisor(humanize.KiByte) /* 1 KiB */)
					case "KB", "KB/s":
						options = append(options, asciitsdb.WithDivisor(humanize.KByte) /* 1 KB */)
					default:
					}

					if d.HasArg("t") {
						// Parse t=[<duration>,<duration>).
						var tstr string
						d.ScanArgs(t, "t", &tstr)
						tstr = strings.TrimSuffix(strings.TrimPrefix(tstr, "["), ")")
						args := strings.Split(tstr, ",")

						dur, err := time.ParseDuration(args[0])
						require.NoError(t, err)
						start := tzero.Add(dur)
						options = append(options, asciitsdb.WithOffset(start.Sub(tzero).Nanoseconds()/testingMetricTick.Nanoseconds()))

						dur, err = time.ParseDuration(args[1])
						require.NoError(t, err)
						end := tzero.Add(dur)
						options = append(options, asciitsdb.WithLimit(end.Sub(start).Nanoseconds()/testingMetricTick.Nanoseconds()))
					}
					if i > 0 {
						buf.WriteString("\n\n\n")
					}
					metrics := bexp.Parse(strings.TrimSpace(selector))
					buf.WriteString(sim.tsdb.Plot(metrics, options...))
				}
				return buf.String()

			case "snapshots":
				var name string
				d.ScanArgs(t, "handle", &name)
				rangeID, ok := handleToRangeID[name]
				require.True(t, ok, "expected to find handle %q, was it initialized?", name)
				handle := sim.state.ranges[rangeID]
				require.True(t, ok, "expected to find named handle %q, was it initialized?", name)
				var buf strings.Builder
				for i, s := range handle.snapshots {
					if i > 0 {
						buf.WriteString("\n")
					}
					buf.WriteString(fmt.Sprintf("t=%s stream=%s\n", s.time.Sub(tzero), s.stream))
					buf.WriteString(fmt.Sprintf(" %s", s.data))
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
			return ""
		})
	})
}

const (
	// testingLocalNodeID is the default node ID used for testing when none is
	// specified.
	testingLocalNodeID = roachpb.NodeID(1)
	// testingLocalStoreID is the default store ID used for testing when none is
	// specified.
	testingLocalStoreID = roachpb.StoreID(1)
	// testingTick is the smallest time interval that we simulate (1ms).
	testingTick = time.Millisecond
	// testingMetricTick is the time interval over which we scrape metrics for
	// ASCII plots.
	testingMetricTick = 100 * testingTick
)

var (
	// tzero represents the t=0, the earliest possible time. All other
	// t={<duration>,[<duration>,<duration>)) is relative to this time.
	tzero = timeutil.Unix(0, 0)
	// testingLocalTenantID is the default tenant ID used for testing when none is specified.
	testingLocalTenantID = roachpb.TenantID{InternalValue: 1}
)

// simulator for various flow control components. It can be annotated with
// various timelines. The two main components being tested are:
//   - RangeController: via testingRCState and associated testing* methods. See
//     the `testdata/range_controller.*` tests for usage.
//   - tokenCounter: via StreamTokenCounterProvider and associated testing*
//     methods. See the `testdata/streams_.*` tests for usage.
type simulator struct {
	state    *testingRCState
	registry *metric.Registry
	tsdb     *asciitsdb.TSDB
	ticker   []ticker
}

func (s *simulator) init(t *testing.T, ctx context.Context) {
	s.state = &testingRCState{}
	s.state.init(t, ctx)
	s.state.initialRegularTokens = kvflowcontrol.Tokens(kvflowcontrol.RegularTokensPerStream.Get(&s.state.settings.SV))
	s.state.initialElasticTokens = kvflowcontrol.Tokens(kvflowcontrol.ElasticTokensPerStream.Get(&s.state.settings.SV))
	s.registry = metric.NewRegistry()
	s.tsdb = asciitsdb.New(t, s.registry)
	s.registry.AddMetricStruct(s.state.evalMetrics)
	s.registry.AddMetricStruct(s.state.ssTokenCounter.Metrics())
	s.registry.AddMetricStruct(s.state.rcMetrics)
	s.tsdb.Register(s.state.evalMetrics)
	s.tsdb.Register(s.state.ssTokenCounter.Metrics())
	s.tsdb.Register(s.state.rcMetrics)
}

func (s *simulator) timeline(tl timeline) {
	// See commentary on the timeline type for its various forms; we do the
	// validation here, in-line. For each timeline, we construct an appropriate
	// ticker that's ticked during the simulation.

	if tl.handle == nil {
		// Form A, interacting with the TokenCounters directly via
		// StreamTokenCounterProvider.

		if tl.rate == 0 {
			return // nothing to do
		}

		require.NotZero(s.state.t, tl.tend)
		require.NotZero(s.state.t, tl.stream)
		require.LessOrEqual(s.state.t, tl.rate, 1000)

		s.ticker = append(s.ticker, &streamsTicker{
			t:           s.state.t,
			evalTC:      s.state.ssTokenCounter.Eval(tl.stream),
			sendTC:      s.state.ssTokenCounter.Send(tl.stream),
			evalMetrics: s.state.evalMetrics,

			tstart: tl.tstart,
			tend:   tl.tend,
			pri:    tl.pri,
			stream: tl.stream,

			deductionDelay:  tl.deductionDelay,
			deduct:          make(map[time.Time][]func()),
			waitingRequests: make(map[time.Time]waitingStreamRequest),
			// Using the parameters above, we figure out two things:
			// - On which ticks do we adjust flow tokens?
			// - How much by, each time?
			//
			// If the request rate we're simulating is:
			// - 1000/sec, we adjust flow tokens every tick(=1ms).
			// - 500/sec, we adjust flow tokens every 2 ticks (=2ms).
			// - ....
			//
			// How much do we adjust by each time? Given we're making 'rate' requests
			// per second, and have to deduct 'delta' tokens per second, each request
			// just deducts delta/rate.
			mod:   int(time.Second/testingTick) / tl.rate,
			delta: kvflowcontrol.Tokens(int(tl.delta) / tl.rate),
		})
		return
	}

	// Forms B-E, using the RangeController instead.
	require.NotNil(s.state.t, tl.handle)

	if tl.rcOp != "" {
		// Forms C-E, where we're either connecting/disconnecting a named
		// stream, or closing/snapshotting a RangeController.
		require.Zero(s.state.t, tl.tend)
		if tl.rcOp == "connect" {
			// Form C.
			require.NotZero(s.state.t, tl.stream)
		}
		if tl.rcOp == "disconnect" {
			// Form D.
			require.NotZero(s.state.t, tl.stream)
		}

		s.ticker = append(s.ticker, &rcOpTicker{
			t:      s.state.t,
			tstart: tl.tstart,
			handle: tl.handle,
			op:     tl.rcOp,
			stream: tl.stream,
		})
		return
	}

	// Form B, where we're deducting/returning flow tokens using RangeController.
	if tl.rate == 0 {
		return // nothing to do
	}

	require.NotZero(s.state.t, tl.tend)
	s.ticker = append(s.ticker, &rcTicker{
		t: s.state.t,

		tstart: tl.tstart,
		tend:   tl.tend,
		pri:    tl.pri,
		handle: tl.handle,
		stream: tl.stream,

		deductionDelay: tl.deductionDelay,
		deduct:         make(map[time.Time][]func()),

		// See commentary on the streamsTicker construction above.
		mod:   int(time.Second/testingTick) / tl.rate,
		delta: kvflowcontrol.Tokens(int(tl.delta) / tl.rate),
	})
}

func (s *simulator) simulate(ctx context.Context, tend time.Time) {
	s.state.ts.Backwards(s.state.ts.Since(tzero)) // reset to tzero
	s.tsdb.Clear()
	for i := range s.ticker {
		s.ticker[i].reset()
		if s.ticker[i].end().After(tend) {
			tend = s.ticker[i].end()
		}
	}

	for {
		t := s.state.ts.Now()
		if t.After(tend) || t.Equal(tend) {
			break
		}
		for i := range s.ticker {
			s.ticker[i].tick(ctx, t)
		}
		if t.UnixNano()%testingMetricTick.Nanoseconds() == 0 {
			s.state.ssTokenCounter.UpdateMetricGauges()
			s.tsdb.Scrape(ctx)
		}
		s.state.ts.Advance(testingTick)
	}
}

// timeline is a sequence of events being simulated. It comes in the following
// forms:
//
//	A. t=[<duration>,<duration>) class={regular,elastic} \
//	   stream=t<int>/s<int> adjust={+,-}<bytes>/s rate=<int>/s \
//	   [deduction-delay=<duration>]
//	B. t=[<duration>,<duration>) class={regular,elastic} handle=<string> \
//	   adjust={+,-}<bytes>/s rate=<int>/s [deduction-delay=<duration>]
//	C. t=<duration> handle=<string> op=connect stream=t<int>/s<int> \
//	   log-position=<int>/<int>
//	D. t=<duration> handle=<string> op=disconnect stream=t<int>/s<int>
//	E. t=<duration> handle=<string> op={snapshot,close}
type timeline struct {
	// Start and (optional) end time for action being simulated.
	tstart, tend time.Time
	// Priority (if applicable) of work on behalf of which we're
	// deducting/returning flow tokens through tokenCounter or RangeController.
	pri admissionpb.WorkPriority
	// Stream over which we're deducting/returning flow tokens (form A) or the
	// stream we're connecting to/disconnecting from a given RangeController.
	stream kvflowcontrol.Stream
	// The number of tokens either being deducted or returned over
	// [tstart,tend). Only applicable to forms A and B.
	delta kvflowcontrol.Tokens
	// The rate at which we adjust flow tokens, controlling the granularity at
	// which 'delta' is adjusted. Only applicable to forms A and B.
	rate int
	// The # of ticks post-Admit() when we actually deduct tokens. Only
	// applicable to forms A and B.
	deductionDelay int
	// Scoped RangeController handle. Only applicable to forms B-E, when we're
	// not dealing with tokenCounters (streams) directly.
	handle *testingRCRange
	// The specific operation to run on a RangeController. Only applicable to
	// forms C-E.
	rcOp string
}

type ticker interface {
	tick(ctx context.Context, t time.Time)
	reset()
	end() time.Time
}

// streamsTicker is a concrete implementation of the ticker interface, used to
// simulate token adjustments to a stream's tokenCounter directly.
type streamsTicker struct {
	t              *testing.T
	tstart, tend   time.Time
	pri            admissionpb.WorkPriority
	stream         kvflowcontrol.Stream
	delta          kvflowcontrol.Tokens
	evalMetrics    *EvalWaitMetrics
	evalTC, sendTC *tokenCounter
	// mod is used to control the number of ticks at which we interact with the
	// the tokenCounter.
	mod, ticks     int
	deductionDelay int

	deduct          map[time.Time][]func()
	waitingRequests map[time.Time]waitingStreamRequest
}

var _ ticker = &streamsTicker{}

// tick is part of the ticker interface.
func (st *streamsTicker) tick(ctx context.Context, t time.Time) {
	wc := admissionpb.WorkClassFromPri(st.pri)
	if ds, ok := st.deduct[t]; ok {
		for _, deduct := range ds {
			deduct()
		}
		delete(st.deduct, t)
	}
	for key, waitingRequest := range st.waitingRequests {
		// Process all waiting requests from earlier. Do this even if t >
		// ct.tend since these requests could've been generated earlier.
		if waitingRequest.ctx.Err() != nil {
			delete(st.waitingRequests, key)
			continue
		}
		if !waitingRequest.signaled() {
			continue
		}
		if waitingRequest.admit() {
			// Request admitted; proceed with token deductions.
			if st.deductionDelay == 0 {
				st.evalTC.adjust(ctx, wc, st.delta, AdjNormal)
				st.sendTC.adjust(ctx, wc, st.delta, AdjNormal)
			} else {
				future := t.Add(testingTick * time.Duration(st.deductionDelay))
				st.deduct[future] = append(st.deduct[future], func() {
					st.evalTC.adjust(ctx, wc, st.delta, AdjNormal)
					st.sendTC.adjust(ctx, wc, st.delta, AdjNormal)
				})
			}
			delete(st.waitingRequests, key)
			return
		}
	}

	if t.Before(st.tstart) || (t.After(st.tend) || t.Equal(st.tend)) {
		// We're outside our [ct.tstart, ct.tend), there's nothing left to do.
		return
	}

	defer func() { st.ticks += 1 }()
	if st.ticks%st.mod != 0 {
		// Nothing to do in this tick.
		return
	}

	if st.delta >= 0 {
		// This timeline is just returning tokens.
		st.evalTC.adjust(ctx, wc, st.delta, AdjNormal)
		st.sendTC.adjust(ctx, wc, st.delta, AdjNormal)
		return
	}

	admitted, signaled, admit := st.evalTC.testingNonBlockingAdmit(ctx, st.pri, st.evalMetrics)
	if admitted {
		// Request admitted; proceed with token deductions.
		if st.deductionDelay == 0 {
			// TODO(kvoli): We are assuming here (and throughout) that send tokens
			// are deducted immediately regardless of their availability. When the
			// send queue is added, we should extend this test to accurately exercise
			// the send queue.
			st.evalTC.adjust(ctx, wc, st.delta, AdjNormal)
			st.sendTC.adjust(ctx, wc, st.delta, AdjNormal)
		} else {
			future := t.Add(testingTick * time.Duration(st.deductionDelay))
			st.deduct[future] = append(st.deduct[future], func() {
				st.evalTC.adjust(ctx, wc, st.delta, AdjNormal)
				st.sendTC.adjust(ctx, wc, st.delta, AdjNormal)
			})
		}
		return
	}
	// Track waiting request.
	st.waitingRequests[t] = waitingStreamRequest{
		ctx:      ctx,
		signaled: signaled,
		admit:    admit,
	}
}

// reset is part of the ticker interface.
func (st *streamsTicker) reset() {
	st.ticks = 0
	st.deduct = make(map[time.Time][]func())
	st.waitingRequests = make(map[time.Time]waitingStreamRequest)
}

// end is part of the ticker interface.
func (st *streamsTicker) end() time.Time {
	return st.tend
}

// waitingStreamRequest represents a request waiting for admission (due to
// unavailable flow tokens) when interacting directly with a stream
// (tokenCounter).
type waitingStreamRequest struct {
	ctx context.Context
	// signaled is a function that returns whether the request has been signaled.
	signaled func() bool
	// admit is a function that returns whether the request has been admitted,
	// invoked once signaled.
	admit func() bool
}

// rcTicker is a concrete implementation of the ticker interface, used to
// simulate token adjustments using a RangeController.
type rcTicker struct {
	t            *testing.T
	tstart, tend time.Time
	pri          admissionpb.WorkPriority
	delta        kvflowcontrol.Tokens
	handle       *testingRCRange
	stream       kvflowcontrol.Stream
	// mod is used to control the number of ticks at which we interact with the
	// the tokenCounter.
	mod, ticks     int
	deductionDelay int

	deduct          map[time.Time][]func()
	waitingRequests map[time.Time]waitingRCRequest
}

var _ ticker = &rcTicker{}

// tick is part of the ticker interface.
func (rt *rcTicker) tick(ctx context.Context, t time.Time) {
	if ds, ok := rt.deduct[t]; ok {
		for _, deduct := range ds {
			deduct()
		}
		delete(rt.deduct, t)
	}
	for key, waitingRequest := range rt.waitingRequests {
		// Process all waiting requests from earlier. Do this even if t >
		// rt.tend since these requests could've been generated earlier.
		if waitingRequest.ctx.Err() != nil {
			delete(rt.waitingRequests, key)
			continue
		}
		for i := range waitingRequest.signaled {
			if !waitingRequest.signaled[i]() {
				continue
			}
			if !waitingRequest.admit[i]() {
				continue
			}

			// Specific stream is unblocked (either because tokens were
			// available, or it disconnected). Stop tracking it.
			waitingRequest.remove(i)
			break
		}

		if len(waitingRequest.signaled) == 0 {
			// All underlying streams have been unblocked; proceed with token
			// deductions.
			if rt.deductionDelay == 0 {
				rt.handle.testingDeductTokens(rt.t, ctx, rt.pri, -rt.delta)
			} else {
				future := t.Add(testingTick * time.Duration(rt.deductionDelay))
				rt.deduct[future] = append(rt.deduct[future], func() {
					rt.handle.testingDeductTokens(rt.t, ctx, rt.pri, -rt.delta)
				})
			}
			delete(rt.waitingRequests, key)
		}
	}

	if t.Before(rt.tstart) || (t.After(rt.tend) || t.Equal(rt.tend)) {
		// We're outside our [rt.tstart, rt.tend), there's nothing left to do
		return
	}

	defer func() { rt.ticks += 1 }()
	if rt.ticks%rt.mod != 0 {
		// Nothing to do in this tick.
		return
	}

	if rt.delta >= 0 {
		// This timeline is just returning tokens.
		rt.handle.testingReturnTokens(rt.t, ctx, rt.pri, rt.delta, rt.stream)
		return
	}

	admitted, signaled, admit := rt.handle.rc.testingNonBlockingAdmit(ctx, rt.pri)
	if admitted {
		// Request admitted; proceed with token deductions.
		if rt.deductionDelay == 0 {
			rt.handle.testingDeductTokens(rt.t, ctx, rt.pri, -rt.delta)
		} else {
			future := t.Add(testingTick * time.Duration(rt.deductionDelay))
			rt.deduct[future] = append(rt.deduct[future], func() {
				rt.handle.testingDeductTokens(rt.t, ctx, rt.pri, -rt.delta)
			})
		}
		return
	}

	// Track waiting request.
	rt.waitingRequests[t] = waitingRCRequest{
		ctx:      ctx,
		signaled: signaled,
		admit:    admit,
	}
}

// reset is part of the ticker interface.
func (rt *rcTicker) reset() {
	rt.ticks = 0
	rt.deduct = make(map[time.Time][]func())
	rt.waitingRequests = make(map[time.Time]waitingRCRequest)
}

// end is part of the ticker interface.
func (rt *rcTicker) end() time.Time {
	return rt.tend
}

// waitingRCRequest represents a request waiting for admission (due to
// unavailable flow tokens) when interacting directly with the RangeController.
type waitingRCRequest struct {
	ctx context.Context
	// signaled is a slice of functions that return whether the request has been
	// signaled (for each underlying replica stream).
	signaled []func() bool
	// admit is a slice of functions that return whether the request has been
	// admitted (for each underlying replica stream), invoked once signaled.
	admit []func() bool
}

func (w *waitingRCRequest) remove(i int) {
	w.signaled = append(w.signaled[:i], w.signaled[i+1:]...)
	w.admit = append(w.admit[:i], w.admit[i+1:]...)
}

// rcOpTicker is a concrete implementation of the ticker interface, used to
// simulate timed operations on a RangeController (taking snapshots,
// connecting/disconnected replica streams, closing it entirely).
type rcOpTicker struct {
	t      *testing.T
	tstart time.Time
	handle *testingRCRange
	stream kvflowcontrol.Stream
	op     string

	done bool
}

var _ ticker = &rcOpTicker{}

// tick is part of the ticker interface.
func (rot *rcOpTicker) tick(ctx context.Context, t time.Time) {
	if rot.done || t.Before(rot.tstart) {
		return // nothing to do
	}
	switch rot.op {
	case "close":
		func() {
			rot.handle.rc.opts.ReplicaMutexAsserter.RaftMu.Lock()
			defer rot.handle.rc.opts.ReplicaMutexAsserter.RaftMu.Unlock()
			rot.handle.rc.CloseRaftMuLocked(ctx)
		}()
		rot.handle.rc = nil
	case "disconnect":
		rot.handle.testingDisconnectStream(rot.t, ctx, rot.stream)
	case "connect":
		rot.handle.testingConnectStream(rot.t, ctx, rot.stream)
	case "snapshot":
		var data string
		rid := rot.handle.testingFindReplStreamOrFatal(ctx, rot.stream)
		rs := rot.handle.rc.replicaMap[rid]
		if rs.sendStream != nil {
			data = rs.sendStream.raftMu.tracker.testingString()
		}
		rot.handle.snapshots = append(rot.handle.snapshots, testingTrackerSnapshot{
			time:   t,
			stream: rot.stream,
			data:   data,
		})
	}
	rot.done = true
}

// reset is part of the ticker interface.
func (rot *rcOpTicker) reset() {}

// end is part of the ticker interface.
func (rot *rcOpTicker) end() time.Time {
	return rot.tstart
}

// testingTrackerSnapshot is a snapshot of a stream's tracker at a given time,
// used in testing.
type testingTrackerSnapshot struct {
	time   time.Time
	stream kvflowcontrol.Stream
	data   string
}

func (t *tokenCounter) testingSignalChannel(wc admissionpb.WorkClass) {
	t.mu.counters[wc].signal()
}

func (t *tokenCounter) testingSignaled(wc admissionpb.WorkClass) func() bool {
	return func() bool {
		select {
		case <-t.mu.counters[wc].signalCh:
			return true
		default:
			return false
		}
	}
}

// testingNonBlockingAdmit is a non-blocking version of a single tokenCounter
// handle WaitForEval() for use in tests.
// - it checks if we have a non-zero number of flow tokens
// - if we do, we return immediately with admitted=true
// - if we don't, we return admitted=false and two callbacks:
//   - signaled, which can be polled to check whether we're ready to try and
//     admitting again;
//   - admit, which can be used to try and admit again. If still not admitted,
//     callers are to wait until they're signaled again.
func (t *tokenCounter) testingNonBlockingAdmit(
	ctx context.Context, pri admissionpb.WorkPriority, metrics *EvalWaitMetrics,
) (admitted bool, signaled func() bool, admit func() bool) {
	wc := admissionpb.WorkClassFromPri(pri)
	tstart := t.clock.PhysicalTime()
	if metrics != nil {
		metrics.OnWaiting(wc)
	}

	admit = func() bool {
		tokens := t.tokens(wc)
		if tokens <= 0 {
			return false
		}
		// Signal a waiter, if any, since we may have removed an entry from the
		// channel via testingSignaled.
		t.testingSignalChannel(wc)
		if metrics != nil {
			metrics.OnAdmitted(wc, t.clock.PhysicalTime().Sub(tstart))
		}
		return true
	}

	if admit() {
		return true, nil, nil
	}

	return false, t.testingSignaled(wc), admit
}

// testingNonBlockingAdmit is a non-blocking alternative to
// RangeController.WaitForEval() for use in tests.
//   - it checks if we have a non-zero number of flow tokens for all connected
//     streams;
//   - if we do, we return immediately with admitted=true;
//   - if we don't, we return admitted=false and two sets of callbacks:
//     (i) signaled, which can be polled to check whether we're ready to try and
//     admitting again. There's one per underlying stream.
//     (ii) admit, which can be used to try and admit again. If still not
//     admitted, callers are to wait until they're signaled again. There's one
//     per underlying stream.
func (rc *rangeController) testingNonBlockingAdmit(
	ctx context.Context, pri admissionpb.WorkPriority,
) (admitted bool, signaled []func() bool, admit []func() bool) {
	vss := rc.mu.voterSets
	// We are matching the behavior of the existing kvflowsimulator test here,
	// where we are abstractly dealing with streams representing replicas,
	// connected to the leader. Expect there to be at least one voter set and no
	// joint configuration (multiple voter sets).
	if len(vss) != 1 {
		log.Fatalf(ctx, "expected exactly one voter set, found %d", len(vss))
	}
	// Similar to the token counter non-blocking admit, we also don't care about
	// replica types, or waiting for only a quorum. We just wait for all
	// non-closed streams to have available tokens, regardless of work class.
	//
	// TODO(kvoli): When we introduce the send queue, we will want to extend the
	// simulation testing. However, we will need to diverge from the existing
	// kvflowsimulator test to do so. For now, match the testing behavior as
	// close as possible.
	vs := vss[0]
	tstart := rc.opts.Clock.PhysicalTime()
	wc := admissionpb.WorkClassFromPri(pri)
	rc.opts.EvalWaitMetrics.OnWaiting(wc)

	admitted = true
	for _, v := range vs {
		// We don't pass in metrics to avoid duplicate updates to eval_wait
		// metrics, done per stream in tokenCounter.testingNonBlockingAdmit.
		vAdmitted, vSignaled, vAdmit := v.evalTokenCounter.testingNonBlockingAdmit(ctx, pri, nil /* metrics */)
		if vAdmitted {
			continue
		}
		admit = append(admit, func() bool {
			if vAdmit() {
				// TODO(kvoli): This is in the original simulation test, but it's not
				// clear why, as it is duplicating onAdmitted calls for each stream
				// that was blocked. See kvflowsimulator/simulation_test.go:438.
				rc.opts.EvalWaitMetrics.OnAdmitted(wc, rc.opts.Clock.PhysicalTime().Sub(tstart))
				return true
			}
			return false
		})
		signaled = append(signaled, vSignaled)
		admitted = false
	}
	if admitted {
		rc.opts.EvalWaitMetrics.OnAdmitted(wc, rc.opts.Clock.PhysicalTime().Sub(tstart))
	}
	return admitted, signaled, admit
}

// testingDeductTokens deducts tokens via the RangeController for a given work
// priority. It constructs a mock Raft entry of the correct size and term, and
// passes it to the RangeController to track, deduct etc, via
// HandleRaftEventRaftMuLocked.
func (r *testingRCRange) testingDeductTokens(
	t *testing.T, ctx context.Context, pri admissionpb.WorkPriority, tokens kvflowcontrol.Tokens,
) {
	if r.rc == nil {
		log.Fatal(ctx, "operating on a closed RangeController")
	}
	r.mu.quorumPosition.Index++
	entry := testingCreateEntry(t, entryInfo{
		term:   r.mu.quorumPosition.Term,
		index:  r.mu.quorumPosition.Index,
		enc:    raftlog.EntryEncodingStandardWithACAndPriority,
		tokens: tokens,
		pri:    AdmissionToRaftPriority(pri),
	})
	msgApps := map[roachpb.ReplicaID][]raftpb.Message{}
	msgApp := raftpb.Message{
		Type:    raftpb.MsgApp,
		To:      0,
		Entries: []raftpb.Entry{entry},
	}
	for k, testR := range r.mu.r.replicaSet {
		msgApp.To = raftpb.PeerID(k)
		msgApps[k] = append([]raftpb.Message(nil), msgApp)
		testR.info.Next = r.mu.quorumPosition.Index + 1
		r.mu.r.replicaSet[k] = testR
	}
	func() {
		r.rc.opts.ReplicaMutexAsserter.RaftMu.Lock()
		defer r.rc.opts.ReplicaMutexAsserter.RaftMu.Unlock()
		require.NoError(t, r.rc.HandleRaftEventRaftMuLocked(ctx, RaftEvent{
			MsgAppMode:        MsgAppPush,
			ReplicasStateInfo: r.replicasStateInfo(),
			Term:              r.mu.quorumPosition.Term,
			Entries:           []raftpb.Entry{entry},
			MsgApps:           msgApps,
		}))
	}()
}

// testingReturnTokens returns tokens via the RangeController for a given work
// priority. It iterates over the tracked deductions for the given (replica
// send) stream, and returns tokens via AdmitRaftMuLocked, if possible.
func (r *testingRCRange) testingReturnTokens(
	t *testing.T,
	ctx context.Context,
	pri admissionpb.WorkPriority,
	tokens kvflowcontrol.Tokens,
	stream kvflowcontrol.Stream,
) {
	if r.rc == nil {
		return
	}
	// Find the replica corresponding to the given stream.
	rid := r.testingFindReplStreamOrFatal(ctx, stream)
	rs := r.rc.replicaMap[rid]
	if rs == nil || rs.sendStream == nil {
		log.Fatalf(ctx, "expected to find non-closed replica send stream for %v", stream)
	}

	// We need to determine the index at which we're returning tokens via
	// AdmittedVector. We do this by iterating over the ountstanding returns
	raftPri := AdmissionToRaftPriority(pri)
	returnIndex := uint64(0)
	r.mu.outstandingReturns[rid] += tokens
	n := rs.sendStream.raftMu.tracker.tracked[raftPri].Length()
	for i := 0; i < n; i++ {
		deduction := rs.sendStream.raftMu.tracker.tracked[raftPri].At(i)
		if r.mu.outstandingReturns[rid]-deduction.tokens >= 0 {
			r.mu.outstandingReturns[rid] -= deduction.tokens
			returnIndex = deduction.id.index
		}
	}
	if returnIndex != 0 {
		// Found at least one deduction to return tokens for.
		av := AdmittedVector{Term: r.mu.quorumPosition.Term}
		av.Admitted[raftPri] = returnIndex
		repl := r.mu.r.replicaSet[rid]
		repl.info.Match = r.mu.quorumPosition.Index
		repl.info.Next = r.mu.quorumPosition.Index + 1
		r.mu.r.replicaSet[rid] = repl
		func() {
			r.rc.opts.ReplicaMutexAsserter.RaftMu.Lock()
			defer r.rc.opts.ReplicaMutexAsserter.RaftMu.Unlock()
			r.rc.AdmitRaftMuLocked(ctx, rs.desc.ReplicaID, av)
			require.NoError(t, r.rc.HandleRaftEventRaftMuLocked(ctx, r.makeRaftEventWithReplicasState()))
		}()
	}
}

func (r *testingRCRange) testingFindReplStreamOrFatal(
	ctx context.Context, stream kvflowcontrol.Stream,
) roachpb.ReplicaID {
	for _, rs := range r.rc.replicaMap {
		if rs.desc.StoreID == stream.StoreID {
			return rs.desc.ReplicaID
		}
	}
	log.Fatalf(ctx, "expected to find replica for stream %v", stream)
	panic("unreachable")
}

// testingConnectStream adds a voter in StateRelicate to the RangeController,
// thereby connecting a replication stream. If a voter for the given stream
// already exists, it is overwritten and forced to StateReplicate.
func (r *testingRCRange) testingConnectStream(
	t *testing.T, ctx context.Context, stream kvflowcontrol.Stream,
) {
	if r.rc == nil {
		log.Fatal(ctx, "operating on a closed RangeController")
	}
	r.mu.r.replicaSet[roachpb.ReplicaID(stream.StoreID)] = testingReplica{
		desc: roachpb.ReplicaDescriptor{
			// We aren't testing multiple stores per node.
			NodeID:    roachpb.NodeID(stream.StoreID),
			StoreID:   stream.StoreID,
			ReplicaID: roachpb.ReplicaID(stream.StoreID),
			Type:      roachpb.VOTER_FULL,
		},
		info: ReplicaStateInfo{
			State: tracker.StateReplicate,
			Match: r.mu.quorumPosition.Index,
			Next:  r.mu.quorumPosition.Index + 1,
		},
	}
	func() {
		r.rc.opts.ReplicaMutexAsserter.RaftMu.Lock()
		defer r.rc.opts.ReplicaMutexAsserter.RaftMu.Unlock()
		// Send an empty raft event in order to trigger any state changes.
		require.NoError(t, r.rc.SetReplicasRaftMuLocked(ctx, r.mu.r.replicas()))
		require.NoError(t, r.rc.HandleRaftEventRaftMuLocked(ctx, r.makeRaftEventWithReplicasState()))
	}()
}

// testingDisconnectStream changes the tracker state of a given stream's
// replica to StateSnapshot, thereby disconnecting it. This will trigger token
// return for any outstanding deductions.
func (r *testingRCRange) testingDisconnectStream(
	t *testing.T, ctx context.Context, stream kvflowcontrol.Stream,
) {
	if r.rc == nil {
		return
	}
	rid := r.testingFindReplStreamOrFatal(ctx, stream)
	rs := r.mu.r.replicaSet[rid]
	rs.info.State = tracker.StateSnapshot
	r.mu.r.replicaSet[rid] = rs
	func() {
		r.rc.opts.ReplicaMutexAsserter.RaftMu.Lock()
		defer r.rc.opts.ReplicaMutexAsserter.RaftMu.Unlock()
		// Send an empty raft event in order to trigger any state changes.
		require.NoError(t, r.rc.HandleRaftEventRaftMuLocked(ctx, r.makeRaftEventWithReplicasState()))
	}()
}

// testingString returns a string representation of the tracker state for use
// in testing.
func (t *Tracker) testingString() string {
	var buf strings.Builder
	for pri, deductions := range t.tracked {
		n := deductions.Length()
		if n == 0 {
			continue
		}
		buf.WriteString(fmt.Sprintf("pri=%s\n", RaftToAdmissionPriority(raftpb.Priority(pri))))
		for i := 0; i < n; i++ {
			deduction := deductions.At(i)
			buf.WriteString(fmt.Sprintf("  tokens=%s log-position=%v/%v\n",
				testingPrintTrimmedTokens(deduction.tokens), deduction.id.term, deduction.id.index))
		}
	}

	return buf.String()
}

func testingPrintTrimmedTokens(t kvflowcontrol.Tokens) string {
	return strings.TrimPrefix(strings.ReplaceAll(t.String(), " ", ""), "+")
}
