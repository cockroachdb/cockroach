// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvflowsimulator

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontroller"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowhandle"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowtokentracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/asciitsdb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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

// TestUsingSimulation is a data-driven test for kvflowcontrol.Controller and
// kvflowcontrol.Handle. It gives package authors a way to understand how flow
// tokens are maintained for individual replication streams, how write bandwidth
// is shaped by these tokens, and how requests queue/dequeue internally. We
// provide the following syntax:
//
//   - "init"
//     [handle=<str>]
//     ....
//     Initialize the flow controller and test simulator. Optionally, initialize
//     named handles for subsequent use.
//
// -----------------------------------------------------------------------------
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
//     t=<duration> handle=<string> op=connect stream=t<int>/s<int> \
//     log-position=<int>/<int>                                              (C)
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
//     rate=5/s, then each return adjusts by +100/5 = +20bytes. If flow
//     tokens are being deducted (-ve 'adjust'), they go through Admit()
//     followed by DeductTokens(). If they're being returned (+ve 'adjust'),
//     they simply go through ReturnTokens(). The optional 'deduction-delay'
//     parameter controls the number of ticks between each request being
//     granted admission and it deducting the corresponding flow tokens.
//
//     B. Similar to A except using a named handle instead, which internally
//     deducts tokens from all connected streams or if returning tokens, does so
//     for the named stream. Token deductions from a handle are tied to
//     monotonically increasing raft log positions starting from position the
//     underlying stream was connected to (using C). When returning tokens, we
//     translate the byte token value to the corresponding raft log prefix
//     (token returns with handles are in terms of raft log positions).
//
//     C. Connects the named handle to the specific stream, starting at the
//     given log position. Subsequent token deductions using the handle will
//     deduct from the given stream.
//
//     D. Disconnects the specific stream from the named handle. All deducted
//     flow tokens (using the named handle) from that specific stream are
//     released. Future token deductions/returns (when using the named handle)
//     don't deduct from/return to the stream.
//
//     E. Close or snapshot the named handle. When closing a handle, all
//     deducted flow tokens are released and subsequent operations are noops.
//     Snapshots record the internal state (what tokens have been
//     deducted-but-not-returned, and for what log positions). This can later be
//     rendered using the "snapshot" directive.
//
// -----------------------------------------------------------------------------
//
//   - "simulate" [t=[<duration>,<duration>)]
//     Simulate timelines until the optionally specified timestamp (the start
//     time is ignored). If no t is specified, the largest end time of all
//     registered timelines is used instead.
//
// -----------------------------------------------------------------------------
//
//   - "plot" [height=<int>] [width=<int>] [precision=<int>] \
//     [t=[<duration>,<duration>)]
//     <metric-selector> unit=<string> [rate=true]
//     ....
//     Plot the flow controller specified metrics (and optionally its rate of
//     change) with the specified units. The following metrics are supported:
//     a. kvadmission.flow_controller.{regular,elastic}_tokens_{available,deducted,returned}
//     b. kvadmission.flow_controller.{regular,elastic}_requests_{waiting,admitted,errored}
//     c. kvadmission.flow_controller.{regular,elastic}{,_blocked}_stream_count
//     d. kvadmission.flow_controller.{regular,elastic}_wait_duration
//     To overlay metrics onto the same plot, the selector supports curly brace
//     expansion. If the unit is one of {MiB,MB,KB,KiB,s,ms,us,μs}, or the
//     bandwidth equivalents (<byte size>/s), the y-axis is automatically
//     converted.
//
// -----------------------------------------------------------------------------
//
//   - "snapshots" handle=<string>
//     Render any captured "snapshots" (what tokens were
//     deducted-but-not-returned as of some timestamp, for what log positions)
//     for the named handle.
//
// Internally we make use of a test-only non-blocking interface for the flow
// controller to support deterministic simulation.
func TestUsingSimulation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		var (
			controller     *kvflowcontroller.Controller
			simulator      *simulator
			tsdb           *asciitsdb.TSDB
			mtime          *timeutil.ManualTime
			replicaHandles map[string]*replicaHandle
		)

		ctx := context.Background()
		replicaHandles = map[string]*replicaHandle{}
		datadriven.RunTest(t, path,
			func(t *testing.T, d *datadriven.TestData) string {
				switch d.Cmd {
				case "init":
					registry := metric.NewRegistry()
					tsdb = asciitsdb.New(t, registry)
					mtime = timeutil.NewManualTime(tzero)
					clock := hlc.NewClockForTesting(mtime)
					controller = kvflowcontroller.New(
						registry,
						cluster.MakeTestingClusterSettings(),
						clock,
					)
					hmetrics := kvflowhandle.NewMetrics(registry)
					tsdb.Register(controller.TestingMetrics())
					tsdb.Register(hmetrics)
					simulator = newSimulator(t, controller, tsdb, mtime)
					for _, line := range strings.Split(d.Input, "\n") {
						name := strings.TrimPrefix(strings.TrimSpace(line), "handle=")
						replicaHandles[name] = &replicaHandle{
							handle:             kvflowhandle.New(controller, hmetrics, clock, roachpb.RangeID(0), roachpb.TenantID{}, nil /* knobs */),
							deductionTracker:   make(map[kvflowcontrol.Stream]*kvflowtokentracker.Tracker),
							outstandingReturns: make(map[kvflowcontrol.Stream]kvflowcontrol.Tokens),
							snapshots:          make([]snapshot, 0),
						}
					}
					return ""

				case "timeline":
					require.NotNilf(t, controller, "uninitialized flow controller (did you use 'init'?)")
					require.NotNilf(t, simulator, "uninitialized simulator (did you use 'init'?)")

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
									tl.tstart = mtime.Now().Add(dur)
									dur, err = time.ParseDuration(args[1])
									require.NoError(t, err)
									tl.tend = mtime.Now().Add(dur)
								} else {
									dur, err := time.ParseDuration(arg)
									require.NoError(t, err)
									tl.tstart = mtime.Now().Add(dur)
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
								tl.deductionDelay = int(dur.Nanoseconds() / tick.Nanoseconds())

							case strings.HasPrefix(parts[i], "handle="):
								// Parse handle=<string>.
								var ok bool
								tl.replicaHandle, ok = replicaHandles[arg]
								require.True(t, ok, "expected to find named handle %q, was it initialized?", arg)

							case strings.HasPrefix(parts[i], "op="):
								// Parse op=<string>.
								require.True(t, arg == "connect" || arg == "disconnect" ||
									arg == "close" || arg == "snapshot")
								tl.handleOp = arg

							case strings.HasPrefix(parts[i], "log-position="):
								// Parse log-position=<int>/<int>.
								inner := strings.Split(arg, "/")
								require.Len(t, inner, 2)
								term, err := strconv.Atoi(inner[0])
								require.NoError(t, err)
								index, err := strconv.Atoi(inner[1])
								require.NoError(t, err)
								tl.position = kvflowcontrolpb.RaftLogPosition{
									Term:  uint64(term),
									Index: uint64(index),
								}

							default:
								t.Fatalf("unrecognized prefix: %s", parts[i])
							}
						}

						simulator.timeline(tl)
					}
					return ""

				case "simulate":
					require.NotNilf(t, simulator, "uninitialized simulator (did you use 'init'?)")
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
						end = mtime.Now().Add(dur)
					}
					simulator.simulate(ctx, end)
					return ""

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

						caption := strings.TrimPrefix(selector, "kvadmission.flow_controller.")
						caption = strings.TrimPrefix(caption, "kvadmission.")
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
							options = append(options, asciitsdb.WithRate(int(time.Second/metricTick)))
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
							options = append(options, asciitsdb.WithOffset(start.Sub(tzero).Nanoseconds()/metricTick.Nanoseconds()))

							dur, err = time.ParseDuration(args[1])
							require.NoError(t, err)
							end := tzero.Add(dur)
							options = append(options, asciitsdb.WithLimit(end.Sub(start).Nanoseconds()/metricTick.Nanoseconds()))
						}
						if i > 0 {
							buf.WriteString("\n\n\n")
						}
						metrics := bexp.Parse(strings.TrimSpace(selector))
						buf.WriteString(tsdb.Plot(metrics, options...))
					}
					return buf.String()

				case "snapshots":
					var name string
					d.ScanArgs(t, "handle", &name)
					replicaHandle, ok := replicaHandles[name]
					require.True(t, ok, "expected to find named handle %q, was it initialized?", name)
					var buf strings.Builder
					for i, s := range replicaHandle.snapshots {
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
			},
		)
	})
}

// tick is the smallest time interval that we simulate (1ms).
const tick = time.Millisecond

// metricTick is the time interval over which we scrape metrics for ASCII plots.
const metricTick = 100 * tick

// simulator for various flow control components. It can be annotated with
// various timelines
type simulator struct {
	t          *testing.T
	controller *kvflowcontroller.Controller
	ticker     []ticker
	tsdb       *asciitsdb.TSDB
	mtime      *timeutil.ManualTime
}

func newSimulator(
	t *testing.T,
	controller *kvflowcontroller.Controller,
	tsdb *asciitsdb.TSDB,
	mtime *timeutil.ManualTime,
) *simulator {
	return &simulator{
		t:          t,
		tsdb:       tsdb,
		controller: controller,
		mtime:      mtime,
	}
}

func (s *simulator) timeline(tl timeline) {
	// See commentary on the timeline type for its various forms; we do the
	// validation here, in-line. For each timeline, we construct an appropriate
	// ticker that's ticked during the simulation.

	if tl.replicaHandle == nil {
		// Form A, interacting with the kvflowcontrol.Controller directly.

		if tl.rate == 0 {
			return // nothing to do
		}

		require.NotZero(s.t, tl.tend)
		require.NotZero(s.t, tl.stream)
		require.LessOrEqual(s.t, tl.rate, 1000)

		s.ticker = append(s.ticker, &controllerTicker{
			t:          s.t,
			controller: s.controller,

			tstart: tl.tstart,
			tend:   tl.tend,
			pri:    tl.pri,
			stream: tl.stream,

			deductionDelay:  tl.deductionDelay,
			deduct:          make(map[time.Time][]func()),
			waitingRequests: make(map[time.Time]waitingRequestInController),

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
			mod:   int(time.Second/tick) / tl.rate,
			delta: kvflowcontrol.Tokens(int(tl.delta) / tl.rate),
		})
		return
	}

	// Forms B-E, using the kvflowcontrol.Handle instead.
	require.NotNil(s.t, tl.replicaHandle)

	if tl.handleOp != "" {
		// Forms C-E, where we're either connecting/disconnecting a named
		// stream, or closing/snapshotting a handle.
		require.Zero(s.t, tl.tend)
		if tl.handleOp == "connect" {
			// Form C.
			require.NotZero(s.t, tl.stream)
			require.NotZero(s.t, tl.position)
		}
		if tl.handleOp == "disconnect" {
			// Form D.
			require.NotZero(s.t, tl.stream)
		}

		s.ticker = append(s.ticker, &handleOpTicker{
			t:             s.t,
			tstart:        tl.tstart,
			replicaHandle: tl.replicaHandle,
			op:            tl.handleOp,
			stream:        tl.stream,
			position:      tl.position,
		})
		return
	}

	// Form B, where we're deducting/returning flow tokens using
	// kvflowcontrol.Handle.
	if tl.rate == 0 {
		return // nothing to do
	}

	require.NotZero(s.t, tl.tend)
	require.Zero(s.t, tl.position)
	s.ticker = append(s.ticker, &handleTicker{
		t: s.t,

		tstart:        tl.tstart,
		tend:          tl.tend,
		pri:           tl.pri,
		replicaHandle: tl.replicaHandle,
		stream:        tl.stream,

		deductionDelay:  tl.deductionDelay,
		deduct:          make(map[time.Time][]func()),
		waitingRequests: make(map[time.Time]waitingRequestInHandle),

		// See commentary on the controllerTicker construction above.
		mod:   int(time.Second/tick) / tl.rate,
		delta: kvflowcontrol.Tokens(int(tl.delta) / tl.rate),
	})
}

// tzero represents the t=0, the earliest possible time. All other
// t={<duration>,[<duration>,<duration>)) is relative to this time.
var tzero = timeutil.Unix(0, 0)

func (s *simulator) simulate(ctx context.Context, tend time.Time) {
	s.mtime.Backwards(s.mtime.Since(tzero)) // reset to tzero
	s.tsdb.Clear()
	for i := range s.ticker {
		s.ticker[i].reset()
		if s.ticker[i].end().After(tend) {
			tend = s.ticker[i].end()
		}
	}

	for {
		t := s.mtime.Now()
		if t.After(tend) || t.Equal(tend) {
			break
		}
		for i := range s.ticker {
			s.ticker[i].tick(ctx, t)
		}
		if t.UnixNano()%metricTick.Nanoseconds() == 0 {
			s.tsdb.Scrape(ctx)
		}
		s.mtime.Advance(tick)
	}
}

// replicaHandle is used to simulate a leaseholder+leader replica that embeds a
// kvflowcontrol.Handle to do flow control.
type replicaHandle struct {
	handle *kvflowhandle.Handle
	closed bool
	// The current raft log position. This is incremented on every flow token
	// deduction, to simulate a command being proposed to raft.
	quorumLogPosition kvflowcontrolpb.RaftLogPosition
	// deductionTracker is used to track flow token deductions and the
	// corresponding quorumLogPosition in the simulator. When returning flow
	// tokens, we specify a bandwidth (+2MiB/s); we use this per-stream tracker
	// to figure out what raft log positions to free up tokens upto.
	deductionTracker   map[kvflowcontrol.Stream]*kvflowtokentracker.Tracker
	outstandingReturns map[kvflowcontrol.Stream]kvflowcontrol.Tokens
	snapshots          []snapshot
}

type snapshot struct {
	time   time.Time
	stream kvflowcontrol.Stream
	data   string
}

func (h *replicaHandle) deductTokens(
	ctx context.Context, pri admissionpb.WorkPriority, tokens kvflowcontrol.Tokens,
) {
	if h.closed {
		return
	}
	// Increment the quorum log position -- all token deductions are bound to
	// incrementing log positions.
	h.quorumLogPosition.Index += 1
	streams := h.handle.TestingDeductTokensForInner(ctx, pri, h.quorumLogPosition, tokens)
	for _, stream := range streams {
		_ = h.deductionTracker[stream].Track(ctx, pri, tokens, h.quorumLogPosition)
	}
}

func (h *replicaHandle) returnTokens(
	ctx context.Context,
	pri admissionpb.WorkPriority,
	tokens kvflowcontrol.Tokens,
	stream kvflowcontrol.Stream,
) {
	if h.closed {
		return
	}

	// Iterate through tracked deductions of the given priority until we've
	// found the log index that accumulates to the # of tokens we want to
	// return. Track any leftovers to consider the next time.
	h.outstandingReturns[stream] += tokens

	returnUpto := kvflowcontrolpb.RaftLogPosition{}
	h.deductionTracker[stream].TestingIter(
		func(priority admissionpb.WorkPriority, tokens kvflowcontrol.Tokens, pos kvflowcontrolpb.RaftLogPosition) bool {
			if pri != priority {
				return true
			}

			if (h.outstandingReturns[stream] - tokens) >= 0 {
				h.outstandingReturns[stream] -= tokens
				returnUpto = pos
				return true
			}
			return false
		},
	)

	if !returnUpto.Equal(kvflowcontrolpb.RaftLogPosition{}) {
		h.handle.ReturnTokensUpto(ctx, pri, returnUpto, stream)
		_ = h.deductionTracker[stream].Untrack(ctx, pri, returnUpto)
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
	// deducting/returning flow tokens through kvflowcontrol.Controller or
	// kvflowcontrol.Handle.
	pri admissionpb.WorkPriority
	// Stream over which we're deducting/returning flow tokens (form A) or the
	// stream we're connecting to/disconnecting from a given
	// kvflowcontrol.Handle.
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
	// Scoped replica handle. Only applicable to forms B-E, when we're not
	// dealing with the kvflowcontrol.Controller directly.
	replicaHandle *replicaHandle
	// The specific operation to run on a kvflowcontrol.Handle. Only applicable
	// to forms C-E.
	handleOp string
	// The log position at which we start issuing writes/deducting tokens (form
	// B) or the position at which we connect a given stream (form C).
	position kvflowcontrolpb.RaftLogPosition
}

type ticker interface {
	tick(ctx context.Context, t time.Time)
	reset()
	end() time.Time
}

// controllerTicker is a concrete implementation of the ticker interface, used
// to simulate token adjustments on the kvflowcontrol.Controller directly.
type controllerTicker struct {
	t              *testing.T
	tstart, tend   time.Time
	pri            admissionpb.WorkPriority
	stream         kvflowcontrol.Stream
	delta          kvflowcontrol.Tokens
	controller     *kvflowcontroller.Controller
	mod, ticks     int // used to control the ticks at which we interact with the controller
	deductionDelay int

	deduct          map[time.Time][]func()
	waitingRequests map[time.Time]waitingRequestInController
}

var _ ticker = &controllerTicker{}

// tick is part of the ticker interface.
func (ct *controllerTicker) tick(ctx context.Context, t time.Time) {
	if ds, ok := ct.deduct[t]; ok {
		for _, deduct := range ds {
			deduct()
		}
		delete(ct.deduct, t)
	}
	for key, waitingRequest := range ct.waitingRequests {
		// Process all waiting requests from earlier. Do this even if t >
		// ct.tend since these requests could've been generated earlier.
		if waitingRequest.ctx.Err() != nil {
			delete(ct.waitingRequests, key)
			continue
		}
		if !waitingRequest.signaled() {
			continue
		}
		if waitingRequest.admit() {
			// Request admitted; proceed with token deductions.
			if ct.deductionDelay == 0 {
				ct.controller.TestingAdjustTokens(ctx, ct.pri, ct.delta, ct.stream)
			} else {
				future := t.Add(tick * time.Duration(ct.deductionDelay))
				ct.deduct[future] = append(ct.deduct[future], func() {
					ct.controller.TestingAdjustTokens(ctx, ct.pri, ct.delta, ct.stream)
				})
			}
			delete(ct.waitingRequests, key)
			return
		}
	}

	if t.Before(ct.tstart) || (t.After(ct.tend) || t.Equal(ct.tend)) {
		return // we're outside our [ct.tstart, ct.tend), there's nothing left to do
	}

	defer func() { ct.ticks += 1 }()
	if ct.ticks%ct.mod != 0 {
		return // nothing to do in this tick
	}

	if ct.delta >= 0 { // return tokens
		ct.controller.TestingAdjustTokens(ctx, ct.pri, ct.delta, ct.stream)
		return
	}

	admitted, signaled, admit := ct.controller.TestingNonBlockingAdmit(ct.pri, &unbreakableStream{ct.stream})
	if admitted {
		// Request admitted; proceed with token deductions.
		if ct.deductionDelay == 0 {
			ct.controller.TestingAdjustTokens(ctx, ct.pri, ct.delta, ct.stream)
		} else {
			future := t.Add(tick * time.Duration(ct.deductionDelay))
			ct.deduct[future] = append(ct.deduct[future], func() {
				ct.controller.TestingAdjustTokens(ctx, ct.pri, ct.delta, ct.stream)
			})
		}
		return
	}
	// Track waiting request.
	ct.waitingRequests[t] = waitingRequestInController{
		ctx:      ctx,
		signaled: signaled,
		admit:    admit,
	}
}

// reset is part of the ticker interface.
func (ct *controllerTicker) reset() {
	ct.ticks = 0
	ct.deduct = make(map[time.Time][]func())
	ct.waitingRequests = make(map[time.Time]waitingRequestInController)
}

// end is part of the ticker interface.
func (ct *controllerTicker) end() time.Time {
	return ct.tend
}

// waitingRequestInController represents a request waiting for admission (due to
// unavailable flow tokens) when interacting directly with the
// kvflowcontrol.Controller.
type waitingRequestInController struct {
	ctx      context.Context
	signaled func() bool // whether the request has been signaled
	admit    func() bool // invoked once signaled, returns whether the request has been admitted
}

// handleTicker is a concrete implementation of the ticker interface, used
// to simulate tokens adjustments using a kvflowcontrol.Handle.
type handleTicker struct {
	t              *testing.T
	tstart, tend   time.Time
	pri            admissionpb.WorkPriority
	delta          kvflowcontrol.Tokens
	replicaHandle  *replicaHandle
	stream         kvflowcontrol.Stream
	mod, ticks     int // used to control the ticks at which we interact with the handle
	deductionDelay int

	deduct          map[time.Time][]func()
	waitingRequests map[time.Time]waitingRequestInHandle
}

var _ ticker = &handleTicker{}

// tick is part of the ticker interface.
func (ht *handleTicker) tick(ctx context.Context, t time.Time) {
	if ds, ok := ht.deduct[t]; ok {
		for _, deduct := range ds {
			deduct()
		}
		delete(ht.deduct, t)
	}
	for key, waitingRequest := range ht.waitingRequests {
		// Process all waiting requests from earlier. Do this even if t >
		// ht.tend since these requests could've been generated earlier.
		if waitingRequest.ctx.Err() != nil {
			delete(ht.waitingRequests, key)
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

			// TODO(irfansharif): Are we introducing non-determinism in this
			// test by potentially allowing multiple (i) streams of a single
			// request, and (ii) requests getting admitted depending on
			// (non-deterministic) channel delivery?
		}

		if len(waitingRequest.signaled) == 0 {
			// All underlying streams have been unblocked; proceed with token
			// deductions.
			if ht.deductionDelay == 0 {
				ht.replicaHandle.deductTokens(ctx, ht.pri, -ht.delta)
			} else {
				future := t.Add(tick * time.Duration(ht.deductionDelay))
				ht.deduct[future] = append(ht.deduct[future], func() {
					ht.replicaHandle.deductTokens(ctx, ht.pri, -ht.delta)
				})
			}
			delete(ht.waitingRequests, key)
		}
	}

	if t.Before(ht.tstart) || (t.After(ht.tend) || t.Equal(ht.tend)) {
		return // we're outside our [ht.tstart, ht.tend), there's nothing left to do
	}

	defer func() { ht.ticks += 1 }()
	if ht.ticks%ht.mod != 0 {
		return // nothing to do in this tick
	}

	if ht.delta >= 0 { // return tokens
		ht.replicaHandle.returnTokens(ctx, ht.pri, ht.delta, ht.stream)
		return
	}

	admitted, signaled, admit := ht.replicaHandle.handle.TestingNonBlockingAdmit(ctx, ht.pri)
	if admitted {
		// Request admitted; proceed with token deductions.
		if ht.deductionDelay == 0 {
			ht.replicaHandle.deductTokens(ctx, ht.pri, -ht.delta)
		} else {
			future := t.Add(tick * time.Duration(ht.deductionDelay))
			ht.deduct[future] = append(ht.deduct[future], func() {
				ht.replicaHandle.deductTokens(ctx, ht.pri, -ht.delta)
			})
		}
		return
	}

	// Track waiting request.
	ht.waitingRequests[t] = waitingRequestInHandle{
		ctx:      ctx,
		signaled: signaled,
		admit:    admit,
	}
}

// reset is part of the ticker interface.
func (ht *handleTicker) reset() {
	ht.ticks = 0
	ht.deduct = make(map[time.Time][]func())
	ht.waitingRequests = make(map[time.Time]waitingRequestInHandle)
}

// end is part of the ticker interface.
func (ht *handleTicker) end() time.Time {
	return ht.tend
}

// waitingRequestInHandle represents a request waiting for admission (due to
// unavailable flow tokens) when interacting directly with the
// kvflowcontrol.Handle.
type waitingRequestInHandle struct {
	ctx      context.Context
	signaled []func() bool // whether the request has been signaled (for each underlying stream)
	admit    []func() bool // invoked once signaled, returns whether the request has been admitted (for each underlying stream)
}

func (w *waitingRequestInHandle) remove(i int) {
	w.signaled = append(w.signaled[:i], w.signaled[i+1:]...)
	w.admit = append(w.admit[:i], w.admit[i+1:]...)
}

// handleOpTicker is a concrete implementation of the ticker interface, used
// to simulate timed operations on a kvflowcontrol.Handle (taking snapshots,
// connecting/disconnecting streams, closing it entirely).
type handleOpTicker struct {
	t             *testing.T
	tstart        time.Time
	replicaHandle *replicaHandle
	position      kvflowcontrolpb.RaftLogPosition
	stream        kvflowcontrol.Stream
	op            string

	done bool
}

var _ ticker = &handleOpTicker{}

// tick is part of the ticker interface.
func (ht *handleOpTicker) tick(ctx context.Context, t time.Time) {
	if ht.done || t.Before(ht.tstart) {
		return // nothing to do
	}
	switch ht.op {
	case "close":
		ht.replicaHandle.handle.Close(ctx)
		ht.replicaHandle.closed = true
	case "disconnect":
		ht.replicaHandle.handle.DisconnectStream(ctx, ht.stream)
	case "connect":
		ht.replicaHandle.quorumLogPosition = ht.position
		ht.replicaHandle.handle.ConnectStream(ctx, ht.position, ht.stream)
		ht.replicaHandle.deductionTracker[ht.stream] = kvflowtokentracker.New(ht.position, ht.stream, nil /* knobs */)
	case "snapshot":
		ht.replicaHandle.snapshots = append(ht.replicaHandle.snapshots, snapshot{
			time:   t,
			stream: ht.stream,
			data:   ht.replicaHandle.deductionTracker[ht.stream].TestingPrintIter(),
		})
	}
	ht.done = true
}

// reset is part of the ticker interface.
func (ht *handleOpTicker) reset() {}

// end is part of the ticker interface.
func (ht *handleOpTicker) end() time.Time {
	return ht.tstart.Add(time.Second)
}

type unbreakableStream struct {
	stream kvflowcontrol.Stream
}

var _ kvflowcontrol.ConnectedStream = &unbreakableStream{}

// Stream is part of the ConnectedStream interface.
func (u *unbreakableStream) Stream() kvflowcontrol.Stream {
	return u.stream
}

// Disconnected is part of the ConnectedStream interface.
func (u *unbreakableStream) Disconnected() <-chan struct{} {
	return nil
}
