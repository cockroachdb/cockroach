// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/dd"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func makeSchedulerOpts(maxConcurrency int, deprioritization float64) SchedulerOptions {
	return SchedulerOptions{
		GetMaxConcurrency:              func() int { return maxConcurrency },
		LogEngineDeprioritizationRatio: func() float64 { return deprioritization },
	}
}

// testDBForCompaction is a mock implementation of pebble.DBForCompaction.
type testDBForCompaction struct {
	name    string
	allowed int
	// waiting is the compaction returned by GetWaitingCompaction. nil means not
	// waiting.
	waiting *pebble.WaitingCompaction
	// remainingScheduleAccepts is the number of Schedule accepts before waiting
	// is consumed (set to nil). Decremented on each accept; when it reaches 0,
	// waiting is nilled. Defaults to 1 if not set.
	remainingScheduleAccepts int
	// onScheduleAccepted is called when Schedule accepts a handle, and returns
	// the index of the compaction.
	onScheduleAccepted func(pebble.CompactionGrantHandle) int
	// rejectNext causes the next Schedule call to return false.
	rejectNext bool
	b          *builderWithMu
}

func (d *testDBForCompaction) GetAllowedWithoutPermission() int {
	return d.allowed
}

func (d *testDBForCompaction) GetWaitingCompaction() (bool, pebble.WaitingCompaction) {
	d.b.append(fmt.Sprintf("%s: GetWaitingCompaction", d.name))
	if d.waiting == nil {
		return false, pebble.WaitingCompaction{}
	}
	return true, *d.waiting
}

func (d *testDBForCompaction) Schedule(h pebble.CompactionGrantHandle) bool {
	if d.rejectNext {
		d.rejectNext = false
		d.b.append(fmt.Sprintf("%s: Schedule => rejected", d.name))
		return false
	}
	index := d.onScheduleAccepted(h)
	// Consume the waiting compaction after remainingScheduleAccepts reaches 0,
	// mimicking real Pebble behavior where the cached picked compaction gets
	// consumed.
	d.remainingScheduleAccepts--
	if d.remainingScheduleAccepts <= 0 {
		d.waiting = nil
	}
	d.b.append(fmt.Sprintf("%s: Schedule => accepted: handle=%d", d.name, index))
	return true
}

type builderWithMu struct {
	mu syncutil.Mutex
	b  strings.Builder
}

func (b *builderWithMu) append(s string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	fmt.Fprintf(&b.b, "%s\n", s)
}

func (b *builderWithMu) getStringAndReset() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	str := b.b.String()
	b.b.Reset()
	return str
}

func TestMultiCompactionScheduler(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// handles tracks all grant handles per engine (from both TrySchedule and
	// Schedule). Uses a pointer to allow testDBForCompaction to append.
	type handleList struct {
		h []pebble.CompactionGrantHandle
	}

	datadriven.Walk(t, datapathutils.TestDataPath(t, "multi_compaction_scheduler"),
		func(t *testing.T, path string) {
			var scheduler *MultiEngineCompactionScheduler
			// Map keys are "log" or "state".
			engines := map[string]*engineState{}
			dbs := map[string]*testDBForCompaction{}
			handles := map[string]*handleList{}
			var b builderWithMu

			datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
				getEngine := func(name string) *engineState {
					e, ok := engines[name]
					require.True(t, ok, "engine %s not found", name)
					return e
				}
				getDB := func(name string) *testDBForCompaction {
					db, ok := dbs[name]
					require.True(t, ok, "db %s not found", name)
					return db
				}

				switch d.Cmd {
				case "init":
					// Initializes the scheduler and the engines, including registration.
					maxConc := dd.ScanArg[int](t, d, "max-concurrency")
					depri := dd.ScanArg[float64](t, d, "deprioritization")
					logAllowed := dd.ScanArg[int](t, d, "log-allowed")
					stateAllowed := dd.ScanArg[int](t, d, "state-allowed")

					opts := makeSchedulerOpts(maxConc, depri)
					scheduler = newMultiEngineCompactionScheduler(opts)

					engines = map[string]*engineState{}
					dbs = map[string]*testDBForCompaction{}
					handles = map[string]*handleList{}

					// Create and register both engines.
					for _, spec := range []struct {
						name    string
						et      EngineType
						allowed int
					}{
						{"log", EngineTypeLog, logAllowed},
						{"state", EngineTypeState, stateAllowed},
					} {
						e := scheduler.OpeningEngine(spec.et).(*engineState)
						engines[spec.name] = e
						hl := &handleList{}
						handles[spec.name] = hl
						db := &testDBForCompaction{
							name: spec.name, allowed: spec.allowed, b: &b}
						db.onScheduleAccepted = func(h pebble.CompactionGrantHandle) int {
							n := len(hl.h)
							hl.h = append(hl.h, h)
							return n
						}
						dbs[spec.name] = db
						e.Register(1, db)
					}
					return ""

				case "set-allowed":
					// Sets the value of GetAllowedWithoutPermission. If "inform" is set,
					// also calls UpdateGetAllowedWithoutPermission and triggers granting.
					name := dd.ScanArg[string](t, d, "engine")
					getDB(name).allowed = dd.ScanArg[int](t, d, "allowed")
					if d.HasArg("inform") {
						getEngine(name).UpdateGetAllowedWithoutPermission()
						// Background granter is disabled; directly trigger granting. Pass
						// it a non-empty scratch candidate set to ensure it does re-slice
						// to zero length.
						scheduler.tryGrant(make([]candidate, 3))
					}
					return b.getStringAndReset()

				case "set-waiting":
					// Sets the value returned by GetWaitingCompaction. If "accept-count"
					// is set (defaults to 1), it specifies the number of Schedule accepts
					// before waiting is consumed.
					name := dd.ScanArg[string](t, d, "engine")
					db := getDB(name)
					db.waiting = &pebble.WaitingCompaction{
						Optional: d.HasArg("optional"),
						Priority: dd.ScanArgOr(t, d, "priority", 0),
						Score:    dd.ScanArgOr[float64](t, d, "score", 0),
					}
					db.remainingScheduleAccepts = dd.ScanArgOr(t, d, "accept-count", 1)
					return ""

				case "clear-waiting":
					// Clears the waiting compaction.
					name := dd.ScanArg[string](t, d, "engine")
					getDB(name).waiting = nil
					return ""

				case "try-schedule":
					// Calls TrySchedule.
					name := dd.ScanArg[string](t, d, "engine")
					e := getEngine(name)
					granted, handle := e.TrySchedule()
					logs := b.getStringAndReset()
					var result string
					if granted {
						handles[name].h = append(handles[name].h, handle)
						result = fmt.Sprintf("granted: handle=%d", len(handles[name].h)-1)
					} else {
						result = "not granted"
					}
					return logs + result + "\n"

				case "compaction-done":
					// Indicates that a running compaction is done.
					name := dd.ScanArg[string](t, d, "engine")
					handleIndex := dd.ScanArg[int](t, d, "handle")
					hl := handles[name]
					require.Less(t, handleIndex, len(hl.h), "no handle at index %d for %s", handleIndex, name)
					hl.h[handleIndex].Done()
					// Pass it a non-empty scratch candidate set to ensure it does
					// re-slice to zero length.
					scheduler.tryGrant(make([]candidate, 1))
					return b.getStringAndReset()

				case "tick":
					// Directly triggers granting (background granter is disabled). Pass
					// it an empty scratch candidate set to ensure it allocates.
					scheduler.tryGrant(nil)
					return b.getStringAndReset()

				case "set-reject-next":
					// For an engine that has a waiting compaction, causes the next
					// Schedule call to return false.
					name := dd.ScanArg[string](t, d, "engine")
					getDB(name).rejectNext = true
					return ""

				case "close":
					// Unregisters the engines and closes the scheduler.
					for _, eng := range engines {
						eng.Unregister()
					}
					scheduler.Close()
					return ""

				case "print-state":
					// Prints the internal state of the scheduler.
					var buf strings.Builder
					scheduler.mu.Lock()
					fmt.Fprintf(&buf, "totalRunning: %d\n", scheduler.mu.totalRunning)
					for _, e := range scheduler.mu.engines {
						fmt.Fprintf(&buf, "%s: running=%d waiting=%v registered=%v\n",
							e.engineType, e.running, e.waiting.value, e.registered)
					}
					scheduler.mu.Unlock()
					return buf.String()

				default:
					t.Fatalf("unknown command %s", d.Cmd)
					return ""
				}
			})
		})
}

// TestMultiCompactionSchedulerConcurrency stress tests that the scheduler
// correctly enforces the global and per-engine concurrency limits under
// concurrent TrySchedule and Schedule calls.
func TestMultiCompactionSchedulerConcurrency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const maxConcurrency = 5
	const logAllowed = 3
	const stateAllowed = 4
	const numGoroutines = 20

	opts := makeSchedulerOpts(maxConcurrency, 1.0)
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	scheduler, err := NewMultiEngineCompactionScheduler(opts, stopper)
	require.NoError(t, err)

	var totalRunning atomic.Int32
	var logRunning atomic.Int32
	var stateRunning atomic.Int32
	var globalViolations atomic.Int32
	var perEngineViolations atomic.Int32

	// runCompaction is the shared logic for both TrySchedule and Schedule paths.
	// It increments counters, sleeps briefly, then decrements counters and calls
	// Done. The counters are conservative bounds: incremented after grant,
	// decremented before Done.
	runCompaction := func(h pebble.CompactionGrantHandle, running *atomic.Int32, allowed int32) {
		if tr := totalRunning.Add(1); tr > maxConcurrency {
			globalViolations.Add(1)
		}
		if er := running.Add(1); er > allowed {
			perEngineViolations.Add(1)
		}
		time.Sleep(time.Duration(rand.Intn(3)) * time.Millisecond)
		running.Add(-1)
		totalRunning.Add(-1)
		h.Done()
	}

	// wg tracks all goroutines: the numGoroutines TrySchedule workers plus any
	// spawned by Schedule from the granter.
	var wg sync.WaitGroup

	// scheduleBudget is the approximate number of compactions we want to run on
	// each engine.
	const scheduleBudget = 200
	logDB := &concurrencyTestDB{
		allowed:    logAllowed,
		runCounter: &logRunning,
		run:        runCompaction,
	}
	logDB.remainingScheduled.Store(scheduleBudget)
	stateDB := &concurrencyTestDB{
		allowed:    stateAllowed,
		runCounter: &stateRunning,
		run:        runCompaction,
	}
	stateDB.remainingScheduled.Store(scheduleBudget)

	logEngine := scheduler.OpeningEngine(EngineTypeLog).(*engineState)
	stateEngine := scheduler.OpeningEngine(EngineTypeState).(*engineState)
	logEngine.Register(1, logDB)
	stateEngine.Register(1, stateDB)

	for i := 0; i < numGoroutines; i++ {
		// Alternate between log and state engines.
		engine, db := logEngine, logDB
		if i%2 == 0 {
			engine, db = stateEngine, stateDB
		}
		wg.Go(func() {
			for db.remainingScheduled.Load() > 0 {
				granted, handle := engine.TrySchedule()
				if !granted {
					time.Sleep(time.Duration(rand.Intn(2)) * time.Millisecond)
					continue
				}
				db.remainingScheduled.Add(-1)
				runCompaction(handle, db.runCounter, db.allowed)
			}
			// Wait for any goroutines started by Schedule to finish before we start
			// waiting on the WaitGroup. We know new goroutines won't be started
			// because db.remainingScheduled is no longer > 0.
			for db.inScheduleWork.Load() > 0 {
				time.Sleep(time.Millisecond)
			}
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out — possible deadlock")
	}

	require.Zero(t, globalViolations.Load(), "global concurrency limit violated")
	require.Zero(t, perEngineViolations.Load(), "per-engine concurrency limit violated")

	stateEngine.Unregister()
	logEngine.Unregister()
	scheduler.Close()
}

// concurrencyTestDB is a thread-safe mock of pebble.DBForCompaction for the
// concurrency test. When Schedule is called (by the granter), it spawns a
// goroutine running the same runCompaction logic as the TrySchedule path.
// remainingScheduled limits the total number of Schedule-initiated compactions
// to prevent unbounded feedback (Schedule → Done → tryGrant → Schedule …).
type concurrencyTestDB struct {
	allowed int32
	// remainingScheduled captures the remaining number of compactions that need
	// to run before we stop. This can overshoot slightly since multiple
	// concurrent goroutines can be decrementing it.
	remainingScheduled atomic.Int32
	runCounter         *atomic.Int32
	run                func(pebble.CompactionGrantHandle, *atomic.Int32, int32)
	inScheduleWork     atomic.Int32
}

func (d *concurrencyTestDB) GetAllowedWithoutPermission() int {
	return int(d.allowed)
}

func (d *concurrencyTestDB) GetWaitingCompaction() (bool, pebble.WaitingCompaction) {
	if d.remainingScheduled.Load() <= 0 {
		return false, pebble.WaitingCompaction{}
	}
	return true, pebble.WaitingCompaction{Score: 1.0}
}

func (d *concurrencyTestDB) Schedule(h pebble.CompactionGrantHandle) bool {
	d.inScheduleWork.Add(1)
	if d.remainingScheduled.Add(-1) < 0 {
		d.inScheduleWork.Add(-1)
		return false
	}
	go func() {
		d.run(h, d.runCounter, d.allowed)
		d.inScheduleWork.Add(-1)
	}()
	return true
}

func TestMultiCompactionSchedulerPebble(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test sanity checks that with two real Pebble instances and a scheduler
	// that limits concurrency to 1, we manage to run compactions on both engines.
	opts := makeSchedulerOpts(1, 1.0)
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	scheduler, err := NewMultiEngineCompactionScheduler(opts, stopper)
	require.NoError(t, err)

	fs := vfs.NewMem()
	require.NoError(t, fs.MkdirAll("/db1", 0755))
	require.NoError(t, fs.MkdirAll("/db2", 0755))

	openDB := func(path string, et EngineType) *pebble.DB {
		cs := scheduler.OpeningEngine(et)
		pOpts := &pebble.Options{
			FS: fs,
		}
		pOpts.Experimental.CompactionScheduler = func() pebble.CompactionScheduler {
			return cs
		}
		db, err := pebble.Open(path, pOpts)
		require.NoError(t, err)
		return db
	}

	db1 := openDB("/db1", EngineTypeLog)
	db2 := openDB("/db2", EngineTypeState)

	// writeFlushCompact writes data, flushes (which may trigger automatic
	// compactions through the scheduler), then does a manual compact to ensure
	// compactions run to completion. Does multiple flushes to trigger more
	// compactions.
	writeFlushCompact := func(db *pebble.DB) error {
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key%06d", i))
			if err := db.Set(key, key, pebble.Sync); err != nil {
				return err
			}
		}
		if err := db.Flush(); err != nil {
			return err
		}
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key%06d", i))
			if err := db.Set(key, key, pebble.Sync); err != nil {
				return err
			}
		}
		if err := db.Flush(); err != nil {
			return err
		}
		return db.Compact(context.Background(), []byte("key000000"), []byte("key999999"), false)
	}

	// Run concurrently on both engines, repeated to increase the chance of
	// hitting interesting interleavings.
	for iter := 0; iter < 10; iter++ {
		var eg errgroup.Group
		eg.Go(func() error {
			return errors.Wrapf(writeFlushCompact(db1), "db1")
		})
		eg.Go(func() error {
			return errors.Wrapf(writeFlushCompact(db2), "db2")
		})
		done := make(chan struct{})
		var waitErr error
		go func() {
			waitErr = eg.Wait()
			close(done)
		}()
		select {
		case <-done:
			require.NoError(t, waitErr)
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out on iteration %d — possible deadlock", iter)
		}
	}

	require.NoError(t, db1.Close())
	require.NoError(t, db2.Close())
	scheduler.Close()
}
