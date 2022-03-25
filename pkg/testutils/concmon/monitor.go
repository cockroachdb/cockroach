// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package concmon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/maruel/panicparse/v2/stack"
	"github.com/petermattis/goid"
)

// Monitor tracks a set of running goroutines as they execute and captures
// tracing recordings from them. It is capable of watching its set of goroutines
// until they all mutually stall.
//
// It is NOT safe to use multiple monitors concurrently.
type Monitor struct {
	seq int
	gs  map[*MonitoredGoroutine]struct{}
	tr  *tracing.Tracer
	buf []byte // avoids allocations
}

type MonitoredGoroutine struct {
	opSeq    int
	opName   string
	gID      int64
	finished int32

	ctx        context.Context
	collect    func() tracing.Recording
	cancel     func()
	prevEvents int
}

// NewMonitor initializes a Monitor.
func NewMonitor() *Monitor {
	return &Monitor{
		tr: tracing.NewTracer(),
		gs: make(map[*MonitoredGoroutine]struct{}),
	}
}

// RunSync synchronously invokes the provided closure with a context that
// contains a tracing span tracked by the monitor.
func (m *Monitor) RunSync(opName string, fn func(context.Context)) {
	ctx, sp := m.tr.StartSpanCtx(context.Background(), opName, tracing.WithRecording(tracing.RecordingVerbose))
	g := &MonitoredGoroutine{
		opSeq:  0, // synchronous
		opName: opName,
		ctx:    ctx,
		collect: func() tracing.Recording {
			return sp.GetConfiguredRecording()
		},
		cancel: sp.Finish,
	}
	m.gs[g] = struct{}{}
	fn(ctx)
	atomic.StoreInt32(&g.finished, 1)
}

// RunAsync is like RunSync, but invokes the closure in a goroutine, returning a
// function that cancels its context.
func (m *Monitor) RunAsync(opName string, fn func(context.Context)) (cancel func()) {
	m.seq++
	ctx, sp := m.tr.StartSpanCtx(context.Background(), opName, tracing.WithRecording(tracing.RecordingVerbose))
	g := &MonitoredGoroutine{
		opSeq:  m.seq,
		opName: opName,
		ctx:    ctx,
		collect: func() tracing.Recording {
			return sp.GetConfiguredRecording()
		},
		cancel: sp.Finish,
	}
	m.gs[g] = struct{}{}
	go func() {
		atomic.StoreInt64(&g.gID, goid.Get())
		fn(ctx)
		atomic.StoreInt32(&g.finished, 1)
	}()
	return cancel
}

// NumMonitored returns the number of goroutines currently tracked by the
// Monitor.
func (m *Monitor) NumMonitored() int {
	return len(m.gs)
}

// CollectRecordings collects the trace recordings from
// all tracked goroutines.
func (m *Monitor) CollectRecordings() string {
	// Collect trace recordings from each goroutine.
	type logRecord struct {
		g     *MonitoredGoroutine
		value string
	}
	var logs []logRecord
	for g := range m.gs {
		prev := g.prevEvents
		rec := g.collect()
		for _, span := range rec {
			for _, log := range span.Logs {
				if prev > 0 {
					prev--
					continue
				}
				logs = append(logs, logRecord{
					g: g, value: log.Msg().StripMarkers(),
				})
				g.prevEvents++
			}
		}
		if atomic.LoadInt32(&g.finished) == 1 {
			g.cancel()
			delete(m.gs, g)
		}
	}

	// Sort logs by g.opSeq. This will sort synchronous goroutines before
	// asynchronous goroutines. Use a stable sort to break ties within
	// goroutines and keep logs in event order.
	sort.SliceStable(logs, func(i int, j int) bool {
		return logs[i].g.opSeq < logs[j].g.opSeq
	})

	var buf strings.Builder
	for i, log := range logs {
		if i > 0 {
			buf.WriteByte('\n')
		}
		seq := "-"
		if log.g.opSeq != 0 {
			seq = strconv.Itoa(log.g.opSeq)
		}
		logValue := stripFileLinePrefix(log.value)
		fmt.Fprintf(&buf, "[%s] %s: %s", seq, log.g.opName, logValue)
	}
	return buf.String()
}

func stripFileLinePrefix(s string) string {
	return reFileLinePrefix.ReplaceAllString(s, "")
}

var reFileLinePrefix = regexp.MustCompile(`^[^:]+:\d+ `)

func (m *Monitor) hasNewEvents(g *MonitoredGoroutine) bool {
	events := 0
	rec := g.collect()
	for _, span := range rec {
		events += len(span.Logs)
	}
	return events > g.prevEvents
}

// WaitForAsyncGoroutinesToStall waits for all goroutines that were launched by
// the monitor's runAsync method to stall due to cross-goroutine synchronization
// dependencies. For instance, it waits for all goroutines to stall while
// receiving from channels. When the method returns, the caller has exclusive
// access to any memory that it shares only with monitored goroutines until it
// performs an action that may unblock any of the goroutines.
func (m *Monitor) WaitForAsyncGoroutinesToStall(t *testing.T) {
	// Iterate until we see two iterations in a row that both observe all
	// monitored goroutines to be stalled and also both observe the exact same
	// goroutine state. The goroutine dump provides a consistent snapshot of all
	// goroutine states and statuses (runtime.Stack(all=true) stops the world).
	var status []*stack.Goroutine
	filter := funcName((*Monitor).RunAsync)
	testutils.SucceedsSoon(t, func() error {
		// Add a small fixed delay after each iteration. This is sufficient to
		// prevents false detection of stalls in a few cases, like when
		// receiving on a buffered channel that already has an element in it.
		defer time.Sleep(5 * time.Millisecond)

		prevStatus := status
		status = goroutineStatus(t, filter, &m.buf)
		if len(status) == 0 {
			// No monitored goroutines.
			return nil
		}

		if prevStatus == nil {
			// First iteration.
			return errors.Errorf("previous status unset")
		}

		// Check whether all monitored goroutines are stalled. If not, retry.
		for _, stat := range status {
			stalled, ok := goroutineStalledStates[stat.State]
			if !ok {
				// NB: this will help us avoid rotting on Go runtime changes.
				t.Fatalf("unknown goroutine state: %s", stat.State)
			}
			if !stalled {
				return errors.Errorf("goroutine %d is not stalled; status %s", stat.ID, stat.State)
			}
		}

		// Make sure the goroutines haven't changed since the last iteration.
		// This ensures that the goroutines stay stable for some amount of time.
		// NB: status and lastStatus are sorted by goroutine id.
		if !reflect.DeepEqual(status, prevStatus) {
			return errors.Errorf("goroutines rapidly changing")
		}
		return nil
	})

	// Add a trace event indicating where each stalled goroutine is waiting.
	byID := make(map[int64]*MonitoredGoroutine, len(m.gs))
	for g := range m.gs {
		byID[atomic.LoadInt64(&g.gID)] = g
	}
	for _, stat := range status {
		g, ok := byID[int64(stat.ID)]
		if !ok {
			// If we're not tracking this goroutine, just ignore it. This can
			// happen when goroutines from earlier subtests haven't finished
			// yet.
			continue
		}
		// If the goroutine hasn't produced any new events, don't create a new
		// "blocked on" trace event. It likely hasn't moved since the last one.
		if !m.hasNewEvents(g) {
			continue
		}
		stalledCall := firstNonStdlib(stat.Stack.Calls)
		log.Eventf(g.ctx, "blocked on %s in %s.%s",
			stat.State, stalledCall.Func.DirName, stalledCall.Func.Name)
	}
}

func funcName(f interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
}

// goroutineStalledStates maps all goroutine states as reported by runtime.Stack
// to a boolean representing whether that state indicates a stalled goroutine. A
// stalled goroutine is one that is waiting on a change on another goroutine in
// order for it to make forward progress itself. If all goroutines enter stalled
// states simultaneously, a process would encounter a deadlock.
var goroutineStalledStates = map[string]bool{
	// See gStatusStrings in runtime/traceback.go and associated comments about
	// the corresponding G statuses in runtime/runtime2.go.
	"idle":      false,
	"runnable":  false,
	"running":   false,
	"syscall":   false,
	"waiting":   true,
	"dead":      false,
	"copystack": false,
	"???":       false, // catch-all in runtime.goroutineheader

	// runtime.goroutineheader may override these G statuses with a waitReason.
	// See waitReasonStrings in runtime/runtime2.go.
	"GC assist marking":       false,
	"IO wait":                 false,
	"chan receive (nil chan)": true,
	"chan send (nil chan)":    true,
	"dumping heap":            false,
	"garbage collection":      false,
	"garbage collection scan": false,
	"panicwait":               false,
	"select":                  true,
	"select (no cases)":       true,
	"GC assist wait":          false,
	"GC sweep wait":           false,
	"GC scavenge wait":        false,
	"chan receive":            true,
	"chan send":               true,
	"finalizer wait":          false,
	"force gc (idle)":         false,
	// Perhaps surprisingly, we mark "semacquire" as a non-stalled state. This
	// is because it is possible to see goroutines briefly enter this state when
	// performing fine-grained memory synchronization, occasionally in the Go
	// runtime itself. No request-level synchronization points use mutexes to
	// wait for state transitions by other requests, so it is safe to ignore
	// this state and wait for it to exit.
	"semacquire":             false,
	"sleep":                  false,
	"sync.Cond.Wait":         true,
	"timer goroutine (idle)": false,
	"trace reader (blocked)": false,
	"wait for GC cycle":      false,
	"GC worker (idle)":       false,
}

// goroutineStatus returns a stack trace for each goroutine whose stack frame
// matches the provided filter. It uses the provided buffer to avoid repeat
// allocations.
func goroutineStatus(t *testing.T, filter string, buf *[]byte) []*stack.Goroutine {
	b := stacks(buf)
	s, _, err := stack.ScanSnapshot(bytes.NewBuffer(b), ioutil.Discard, stack.DefaultOpts())
	if err != io.EOF {
		t.Fatalf("could not parse goroutine dump: %v", err)
		return nil
	}

	matching := s.Goroutines[:0]
	for _, g := range s.Goroutines {
		for _, call := range g.Stack.Calls {
			if strings.Contains(call.Func.Complete, filter) {
				matching = append(matching, g)
				break
			}
		}
	}
	return matching
}

// stacks is a wrapper for runtime.Stack that attempts to recover the data for
// all goroutines. It uses the provided buffer to avoid repeat allocations.
func stacks(buf *[]byte) []byte {
	// We don't know how big the buffer needs to be to collect all the
	// goroutines. Start with 64 KB and try a few times, doubling each time.
	// NB: This is inspired by runtime/pprof/pprof.go:writeGoroutineStacks.
	if len(*buf) == 0 {
		*buf = make([]byte, 1<<16)
	}
	var truncBuf []byte
	for i := 0; ; i++ {
		n := runtime.Stack(*buf, true /* all */)
		if n < len(*buf) {
			truncBuf = (*buf)[:n]
			break
		}
		*buf = make([]byte, 2*len(*buf))
	}
	return truncBuf
}

func firstNonStdlib(calls []stack.Call) stack.Call {
	for _, call := range calls {
		if call.Location != stack.Stdlib {
			return call
		}
	}
	panic("unexpected")
}
