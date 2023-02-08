// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowcontroller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
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

// TestUsingSimulation is a data-driven test for kvflowcontrol.Controller.
// It gives package authors a way to understand how flow tokens are maintained
// for individual replication streams, how write bandwidth is shaped by these
// tokens, and how requests queue/dequeue internally. We provide the following
// syntax:
//
//   - "init"
//     Initialize the flow controller and test simulator.
//
//   - "timeline"
//     start=<duration> end=<duration> class={regular,elastic} \
//     stream=t<int>/s<int> adjust={+,-}<bytes>/s rate=<int>/s \
//     [deduction-delay=<duration>]
//     ....
//     Creates a "thread" that operates between t='start' to (non-inclusive)
//     t='end', issuing the specified 'rate' of requests of the given work
//     'class', over the given 'stream', where the flow tokens are
//     {deducted,returned} with the given bandwidth. The 'rate' controls the
//     granularity of token adjustment, i.e. if adjust=+100bytes/s and
//     rate=5/s, then each return adjusts by +100/5 = +20bytes. If flow tokens
//     are being deducted (-ve 'adjust'), they go through Admit() followed by
//     DeductTokens(). If they're being returned (+ve 'adjust'), they simply go
//     through ReturnTokens(). The optional 'deduction-delay' parameter controls
//     the number of ticks between each request being granted admission and it
//     deducting the corresponding flow tokens.
//
//   - "simulate" [end=<duration>]
//     Simulate timelines until the optionally specified timestamp. If no
//     timestamp is specified, the largest end time of all registered timelines
//     is used instead.
//
//   - "plot" [height=<int>] [width=<int>] [precision=<int>] \
//     [start=<duration>] [end=<duration>]
//     <metric selector> unit=<string> [rate=true]
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
// Internally we make use of a test-only non-blocking interface for the flow
// controller in order to enable deterministic simulation.
func TestUsingSimulation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, datapathutils.TestDataPath(t, "simulation"), func(t *testing.T, path string) {
		var (
			controller *Controller
			simulator  *simulator
			tsdb       *asciitsdb.TSDB
			mtime      *timeutil.ManualTime
		)

		ctx := context.Background()
		datadriven.RunTest(t, path,
			func(t *testing.T, d *datadriven.TestData) string {
				switch d.Cmd {
				case "init":
					registry := metric.NewRegistry()
					tsdb = asciitsdb.New(t, registry)
					mtime = timeutil.NewManualTime(tstart)
					controller = New(
						registry,
						cluster.MakeTestingClusterSettings(),
						hlc.NewClock(mtime, time.Nanosecond /* maxOffset */),
					)
					tsdb.Register(controller.metrics)
					simulator = newSimulator(t, controller, tsdb, mtime)
					return ""

				case "timeline":
					require.NotNilf(t, controller, "uninitialized flow controller (did you use 'init'?)")
					require.NotNilf(t, simulator, "uninitialized simulator (did you use 'init'?)")

					for _, line := range strings.Split(d.Input, "\n") {
						parts := strings.Fields(line)
						require.True(t, len(parts) >= 6, `expected form:
	start=<duration> end=<duration> class={regular,elastic} \
	stream=t<int>/s<int> adjust={+,-}<bytes>/s rate=<int>/s \
	[deduction-delay=<duration>]
`)

						var (
							start, end time.Time
							pri        admissionpb.WorkPriority
							stream     kvflowcontrol.Stream
							delta      kvflowcontrol.Tokens
							rate       int
						)

						for i := range parts {
							parts[i] = strings.TrimSpace(parts[i])
						}

						require.True(t, strings.HasPrefix(parts[0], "start="))
						require.True(t, strings.HasPrefix(parts[1], "end="))
						require.True(t, strings.HasPrefix(parts[2], "class="))
						require.True(t, strings.HasPrefix(parts[3], "stream="))
						require.True(t, strings.HasPrefix(parts[4], "adjust="))
						require.True(t, strings.HasPrefix(parts[5], "rate="))

						for i := range parts[:6] {
							inner := strings.Split(parts[i], "=")
							require.Len(t, inner, 2)
							parts[i] = strings.TrimSpace(inner[1])
						}

						// Parse start=<duration>.
						dur, err := time.ParseDuration(parts[0])
						require.NoError(t, err)
						start = mtime.Now().Add(dur)

						// Parse end=<duration>.
						dur, err = time.ParseDuration(parts[1])
						require.NoError(t, err)
						end = mtime.Now().Add(dur)

						// Parse class={regular,elastic}.
						switch parts[2] {
						case "regular":
							pri = admissionpb.NormalPri
						case "elastic":
							pri = admissionpb.BulkNormalPri
						default:
							t.Fatalf("unexpected class: %s", parts[1])
						}

						{ // Parse stream=t<int>/s<int>.
							inner := strings.Split(parts[3], "/")
							require.Len(t, inner, 2)

							ti, err := strconv.Atoi(strings.TrimPrefix(inner[0], "t"))
							require.NoError(t, err)

							si, err := strconv.Atoi(strings.TrimPrefix(inner[1], "s"))
							require.NoError(t, err)

							stream = kvflowcontrol.Stream{
								TenantID: roachpb.MustMakeTenantID(uint64(ti)),
								StoreID:  roachpb.StoreID(si),
							}
						}

						// Parse adjust={+,-}<bytes>/s.
						isPositive := strings.Contains(parts[4], "+")
						parts[4] = strings.TrimPrefix(parts[4], "+")
						parts[4] = strings.TrimPrefix(parts[4], "-")
						bytes, err := humanize.ParseBytes(strings.TrimSuffix(parts[4], "/s"))
						require.NoError(t, err)
						delta = kvflowcontrol.Tokens(int64(bytes))
						if !isPositive {
							delta = -delta
						}

						// Parse rate=<int>/s.
						rate, err = strconv.Atoi(strings.TrimSuffix(parts[5], "/s"))
						require.NoError(t, err)

						deductionDelay := 0
						if len(parts) > 6 {
							for _, part := range parts[6:] {
								part = strings.TrimSpace(part)
								if strings.HasPrefix(part, "deduction-delay=") {
									part = strings.TrimPrefix(part, "deduction-delay=")
									dur, err := time.ParseDuration(part)
									require.NoError(t, err)
									deductionDelay = int(dur.Nanoseconds() / tick.Nanoseconds())
								}
							}
						}
						simulator.timeline(start, end, pri, stream, delta, rate, deductionDelay)
					}
					return ""

				case "simulate":
					require.NotNilf(t, simulator, "uninitialized simulator (did you use 'init'?)")
					var end time.Time
					if d.HasArg("end") {
						// Parse end=<duration>.
						var endStr string
						d.ScanArgs(t, "end", &endStr)
						dur, err := time.ParseDuration(endStr)
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
							time.Microsecond.Nanoseconds()
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

						start := tstart
						if d.HasArg("start") {
							// Parse start=<duration>.
							var startStr string
							d.ScanArgs(t, "start", &startStr)
							dur, err := time.ParseDuration(startStr)
							require.NoError(t, err)
							start = tstart.Add(dur)
							options = append(options, asciitsdb.WithOffset(start.Sub(tstart).Nanoseconds()/metricTick.Nanoseconds()))
						}

						if d.HasArg("end") {
							// Parse end=<duration>.
							var endStr string
							d.ScanArgs(t, "end", &endStr)
							dur, err := time.ParseDuration(endStr)
							require.NoError(t, err)
							end := tstart.Add(dur)
							options = append(options, asciitsdb.WithLimit(end.Sub(start).Nanoseconds()/metricTick.Nanoseconds()))
						}

						if i > 0 {
							buf.WriteString("\n\n\n")
						}
						metrics := bexp.Parse(strings.TrimSpace(selector))
						buf.WriteString(tsdb.Plot(metrics, options...))
					}
					return buf.String()

				default:
					return fmt.Sprintf("unknown command: %s", d.Cmd)
				}
			},
		)
	})
}

var tstart = timeutil.Unix(0, 0)

const tick = time.Millisecond
const metricTick = 100 * tick

type simulator struct {
	t *testing.T

	controller *Controller
	ticker     []ticker
	tsdb       *asciitsdb.TSDB
	mtime      *timeutil.ManualTime
}

func newSimulator(
	t *testing.T, controller *Controller, tsdb *asciitsdb.TSDB, mtime *timeutil.ManualTime,
) *simulator {
	return &simulator{
		t:          t,
		tsdb:       tsdb,
		controller: controller,
		mtime:      mtime,
	}
}

func (s *simulator) timeline(
	start, end time.Time,
	pri admissionpb.WorkPriority,
	stream kvflowcontrol.Stream,
	delta kvflowcontrol.Tokens,
	rate, deductionDelay int,
) {
	if rate == 0 {
		return // nothing to do
	}
	s.ticker = append(s.ticker, ticker{
		t:          s.t,
		controller: s.controller,

		start:  start,
		end:    end,
		pri:    pri,
		stream: stream,

		deductionDelay: deductionDelay,
		deduct:         make(map[time.Time][]func()),
		waitHandles:    make(map[time.Time]waitHandle),

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
		mod:   int(time.Second/tick) / rate,
		delta: kvflowcontrol.Tokens(int(delta) / rate),
	})
}

func (s *simulator) simulate(ctx context.Context, end time.Time) {
	s.mtime.Backwards(s.mtime.Since(tstart)) // reset to tstart
	s.tsdb.Clear()
	for i := range s.ticker {
		s.ticker[i].reset()
		if s.ticker[i].end.After(end) {
			end = s.ticker[i].end
		}
	}

	for {
		t := s.mtime.Now()
		if t.After(end) || t.Equal(end) {
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

type waitHandle struct {
	ctx      context.Context
	signaled func() bool
	admit    func() bool
}

type ticker struct {
	t          *testing.T
	start, end time.Time
	pri        admissionpb.WorkPriority
	stream     kvflowcontrol.Stream
	delta      kvflowcontrol.Tokens
	controller *Controller
	mod, ticks int // used to control the ticks at which we interact with the controller

	deduct      map[time.Time][]func()
	waitHandles map[time.Time]waitHandle

	deductionDelay int
}

func (ti *ticker) tick(ctx context.Context, t time.Time) {
	if ds, ok := ti.deduct[t]; ok {
		for _, deduct := range ds {
			deduct()
		}
		delete(ti.deduct, t)
	}
	for key, handle := range ti.waitHandles {
		// Process all waiting requests from earlier. Do this even if t >
		// ti.end since these requests could've been generated earlier.
		if handle.ctx.Err() != nil {
			delete(ti.waitHandles, key)
			continue
		}
		if !handle.signaled() {
			continue
		}
		if handle.admit() {
			if ti.deductionDelay == 0 {
				ti.controller.adjustTokens(ctx, ti.pri, ti.delta, ti.stream)
			} else {
				future := t.Add(tick * time.Duration(ti.deductionDelay))
				ti.deduct[future] = append(ti.deduct[future], func() {
					ti.controller.adjustTokens(ctx, ti.pri, ti.delta, ti.stream)
				})
			}
			delete(ti.waitHandles, key)
			return
		}
	}

	if t.Before(ti.start) || (t.After(ti.end) || t.Equal(ti.end)) {
		return // we're outside our [ti.start, ti.end), there's nothing left to do
	}

	defer func() { ti.ticks += 1 }()
	if ti.ticks%ti.mod != 0 {
		return // nothing to do in this tick
	}

	if ti.delta >= 0 { // return tokens
		ti.controller.adjustTokens(ctx, ti.pri, ti.delta, ti.stream)
		return
	}

	admitted, signaled, admit := ti.controller.testingNonBlockingAdmit(ti.pri, ti.stream)
	if admitted {
		if ti.deductionDelay == 0 {
			ti.controller.adjustTokens(ctx, ti.pri, ti.delta, ti.stream)
		} else {
			future := t.Add(tick * time.Duration(ti.deductionDelay))
			ti.deduct[future] = append(ti.deduct[future], func() {
				ti.controller.adjustTokens(ctx, ti.pri, ti.delta, ti.stream)
			})
		}
		return
	}
	ti.waitHandles[t] = waitHandle{
		ctx:      ctx,
		signaled: signaled,
		admit:    admit,
	}
}

func (ti *ticker) reset() {
	ti.ticks = 0
	ti.deduct = make(map[time.Time][]func())
	ti.waitHandles = make(map[time.Time]waitHandle)
}
