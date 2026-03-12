// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"cmp"
	"context"
	"slices"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/redact"
)

// SchedulerOptions provides configuration for MultiEngineCompactionScheduler.
type SchedulerOptions struct {
	// GetMaxConcurrency returns the global maximum number of concurrent
	// compactions across all engines.
	GetMaxConcurrency func() int
	// LogEngineDeprioritizationRatio returns a multiplier (in [0, 1]) applied
	// to log engine compaction scores when comparing against state engine
	// compactions. A value of 0.5 means log engine scores are halved.
	LogEngineDeprioritizationRatio func() float64

	// testingDisableBackgroundGranter, if true, prevents the background granter
	// goroutine from starting. Used in deterministic tests that trigger
	// granting explicitly.
	testingDisableBackgroundGranter bool
}

// EngineType identifies the type of engine.
type EngineType uint8

const (
	// EngineTypeLog is the engine used for the raft log.
	EngineTypeLog EngineType = iota
	// EngineTypeState is the engine used for the state machine.
	EngineTypeState
)

func (e EngineType) String() string {
	return redact.StringWithoutMarkers(e)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (e EngineType) SafeFormat(p redact.SafePrinter, _ rune) {
	switch e {
	case EngineTypeLog:
		p.SafeString("log")
	case EngineTypeState:
		p.SafeString("state")
	default:
		p.SafeString("unknown")
	}
}

// MultiEngineCompactionScheduler enforces a global compaction concurrency
// limit across multiple Pebble engines (e.g. log and state engines in a
// CockroachDB store), while respecting per-engine soft limits from
// GetAllowedWithoutPermission and deprioritizing log engine compactions.
//
// Each engine gets an engineState that implements pebble.CompactionScheduler
// (passed to pebble.Options.CompactionScheduler) and
// pebble.CompactionGrantHandle.
type MultiEngineCompactionScheduler struct {
	opts SchedulerOptions
	mu   struct {
		syncutil.Mutex
		// engines is the list of engines for which OpeningEngine has been called.
		// Note that the engine may not yet be registered. See engineState for
		// details. Once registered, an unregistration will remove the engine from
		// this list.
		engines []*engineState
		// totalRunning is the current number of compactions running across all
		// engines.
		totalRunning int
		// isGranting ensures TrySchedule serializes with granting, and ensures
		// granting is not active before Unregister returns.
		isGranting     bool
		isGrantingCond *sync.Cond
		// closed transitions to true when the scheduler is closed.
		closed bool
	}
	stopBackgroundGranterCh chan struct{}
	pokeBackgroundGranterCh chan struct{}
}

// engineState implements pebble.CompactionScheduler (one per engine) and
// pebble.CompactionGrantHandle.
//
// It is created before the engine (pebble.DBForCompaction) is opened, because
// the engine needs a CompactionScheduler passed to the open call. The open
// call then Registers with the engineState.
type engineState struct {
	scheduler  *MultiEngineCompactionScheduler
	engineType EngineType
	// db is not protected by mutex, but is not read unless registered is true.
	// It transitions once from nil to non-nil.
	db pebble.DBForCompaction
	// Fields below are protected by scheduler.mu.

	// registered transitions at most once from false to true, and then back to
	// false on Unregister. The remaining fields below are only relevant when
	// registered is true.
	registered                   bool
	running                      int
	waiting                      waitingState
	lastAllowedWithoutPermission int
}

// waitingState tracks whether an engine has waiting compactions. waitingState
// is a concurrency optimization that avoids having to repeatedly release
// scheduler.mu to call DBForCompaction.GetWaitingCompaction for an engine
// that has no waiting compactions.
//
// The tryScheduleCount field prevents spuriously setting value to false when
// a new TrySchedule arrives between sampling and the attempt to transition to
// false. The waitingState is snapshotted when sampling: say the value is
// (true,5). Then the scheduler tries to get a waiting compaction, but fails
// to, and before it can set the value to false, the value changes to
// (true,6). The increment in tryScheduleCount prevents the stale snapshot
// from incorrectly setting value to false. See the comments near the calls to
// trySetWaitingToFalseLocked for the exact hazard this guards against.
type waitingState struct {
	value bool
	// tryScheduleCount is monotonically increasing and incremented on every
	// TrySchedule.
	tryScheduleCount int64
}

// trySetWaitingToFalseLocked sets waiting.value to false only if
// tryScheduleCount hasn't changed since the snapshot, meaning no new
// TrySchedule arrived.
func (w *waitingState) trySetWaitingToFalseLocked(snapshot waitingState) {
	if w.tryScheduleCount == snapshot.tryScheduleCount {
		w.value = false
	}
}

// setWaitingToTrueLocked sets waiting.value to true and increments
// tryScheduleCount to invalidate any concurrent snapshots.
func (w *waitingState) setWaitingToTrueLocked() {
	w.value = true
	w.tryScheduleCount++
}

// candidate represents a compaction candidate in tryGrant.
type candidate struct {
	engine   *engineState
	wc       pebble.WaitingCompaction
	snapshot waitingState
}

var _ pebble.CompactionScheduler = &engineState{}
var _ pebble.CompactionGrantHandle = &engineState{}

// NewMultiEngineCompactionScheduler creates a scheduler that enforces a
// global concurrency limit across multiple engines. The stopper manages the
// background granter goroutine's lifecycle.
func NewMultiEngineCompactionScheduler(
	opts SchedulerOptions, stopper *stop.Stopper,
) (*MultiEngineCompactionScheduler, error) {
	s := &MultiEngineCompactionScheduler{
		opts:                    opts,
		stopBackgroundGranterCh: make(chan struct{}),
		pokeBackgroundGranterCh: make(chan struct{}, 1),
	}
	s.mu.isGrantingCond = sync.NewCond(&s.mu.Mutex)
	if !opts.testingDisableBackgroundGranter {
		if err := stopper.RunAsyncTask(
			context.Background(), "multi-compaction-scheduler",
			func(ctx context.Context) {
				s.backgroundGranter(stopper.ShouldQuiesce())
			},
		); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// OpeningEngine creates an engineState for the given engine type and returns it
// as a pebble.CompactionScheduler. The caller passes this to
// pebble.Options.CompactionScheduler before calling pebble.Open.
func (s *MultiEngineCompactionScheduler) OpeningEngine(t EngineType) pebble.CompactionScheduler {
	e := &engineState{
		scheduler:  s,
		engineType: t,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.mu.engines {
		if existing.engineType == t {
			panic("only one engine of each type is supported")
		}
	}
	s.mu.engines = append(s.mu.engines, e)
	return e
}

// Close stops the background granter goroutine and waits for any in-progress
// granting to complete.
func (s *MultiEngineCompactionScheduler) Close() {
	close(s.stopBackgroundGranterCh)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.closed = true
	for s.mu.isGranting {
		s.mu.isGrantingCond.Wait()
	}
}

// Register implements pebble.CompactionScheduler.
func (e *engineState) Register(numGoroutinesPerCompaction int, db pebble.DBForCompaction) {
	s := e.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = e.assertInEnginesSliceLocked()
	if e.registered {
		panic("engine already registered")
	}
	e.registered = true
	e.db = db
}

// Unregister implements pebble.CompactionScheduler.
func (e *engineState) Unregister() {
	s := e.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.mu.engines)
	i := e.assertInEnginesSliceLocked()
	if !e.registered {
		panic("engine never registered")
	}
	e.registered = false
	s.mu.engines[i] = nil
	s.mu.engines[i] = s.mu.engines[n-1]
	s.mu.engines = s.mu.engines[:n-1]
	// Wait for isGranting to become false so no further calls to this
	// engine's DBForCompaction methods will be made.
	for s.mu.isGranting {
		s.mu.isGrantingCond.Wait()
	}
}

func (e *engineState) assertInEnginesSliceLocked() int {
	s := e.scheduler
	i := 0
	n := len(s.mu.engines)
	for ; i < n; i++ {
		if s.mu.engines[i] == e {
			break
		}
	}
	if i == n {
		panic("engineState not found in scheduler engines list")
	}
	return i
}

// TrySchedule implements pebble.CompactionScheduler.
func (e *engineState) TrySchedule() (bool, pebble.CompactionGrantHandle) {
	s := e.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	if !e.registered {
		return false, nil
	}
	// If granting is in progress, just mark as waiting so the granter picks
	// this up (we can't wait here since that can deadlock) in the background
	// granter (unless Done triggers a grant first).
	if s.mu.isGranting {
		e.waiting.setWaitingToTrueLocked()
		s.pokeBackgroundGranter()
		return false, nil
	}
	allowed := e.db.GetAllowedWithoutPermission()
	e.lastAllowedWithoutPermission = allowed
	if e.running < allowed && s.mu.totalRunning < s.opts.GetMaxConcurrency() {
		e.running++
		s.mu.totalRunning++
		return true, e
	}
	e.waiting.setWaitingToTrueLocked()
	return false, nil
}

// UpdateGetAllowedWithoutPermission implements pebble.CompactionScheduler.
func (e *engineState) UpdateGetAllowedWithoutPermission() {
	s := e.scheduler
	s.mu.Lock()
	if !e.registered {
		s.mu.Unlock()
		return
	}
	allowed := e.db.GetAllowedWithoutPermission()
	poke := allowed > e.lastAllowedWithoutPermission
	e.lastAllowedWithoutPermission = allowed
	s.mu.Unlock()
	if poke {
		s.pokeBackgroundGranter()
	}
}

// Started implements pebble.CompactionGrantHandle.
func (e *engineState) Started() {
	// Noop, since we are not measuring CPU usage.
}

// MeasureCPU implements pebble.CompactionGrantHandle.
func (e *engineState) MeasureCPU(pebble.CompactionGoroutineKind) {
	// Noop, since we are not measuring CPU usage.
}

// CumulativeStats implements pebble.CompactionGrantHandle.
func (e *engineState) CumulativeStats(stats pebble.CompactionGrantHandleStats) {
	// Noop, since we are not measuring CPU usage.
}

// Done implements pebble.CompactionGrantHandle.
func (e *engineState) Done() {
	s := e.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	e.running--
	s.mu.totalRunning--
	s.pokeBackgroundGranter()
}

// tryGrant attempts to grant compaction slots to waiting engines.
//
// The scratch parameter is used to avoid allocations and the updated scratch
// is returned for future use.
func (s *MultiEngineCompactionScheduler) tryGrant(scratch []candidate) (scratch2 []candidate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Check if all engines are unregistered or scheduler is closed.
	if s.mu.closed || !s.hasRegisteredEnginesLocked() {
		return
	}
	s.mu.isGranting = true
	defer func() {
		s.mu.isGranting = false
		s.mu.isGrantingCond.Broadcast()
	}()

	candidates := scratch
	// Outer loop: keep granting while we have global capacity.
	for s.mu.totalRunning < s.opts.GetMaxConcurrency() {
		candidates = candidates[:0]
		// Collect candidates from all waiting, registered engines.
		for _, e := range s.mu.engines {
			if !e.registered || !e.waiting.value {
				continue
			}
			allowed := e.db.GetAllowedWithoutPermission()
			e.lastAllowedWithoutPermission = allowed
			if e.running >= allowed {
				continue
			}
			// INVARIANT: e.waiting.value is true.
			snapshot := e.waiting
			// Unlock mu to call GetWaitingCompaction (lock ordering).
			s.mu.Unlock()
			waiting, wc := e.db.GetWaitingCompaction()
			// NB: GetWaitingCompaction may return false, and then a new TrySchedule
			// may arrive before the mutex is re-acquired. In this case,
			// trySetWaitingToFalseLocked will not actually set waiting.value to
			// false.
			s.mu.Lock()
			if !waiting {
				e.waiting.trySetWaitingToFalseLocked(snapshot)
				continue
			}
			// Apply deprioritization to log engine scores.
			if e.engineType == EngineTypeLog {
				wc.Score *= s.opts.LogEngineDeprioritizationRatio()
			}
			candidates = append(candidates, candidate{
				engine:   e,
				wc:       wc,
				snapshot: snapshot,
			})
		}
		if len(candidates) == 0 {
			break
		}
		// Sort candidates best-first: Optional (false < true), Priority
		// (desc), Score (desc).
		slices.SortFunc(candidates, func(a, b candidate) int {
			return compareCompactions(a.wc, b.wc)
		})

		// Try candidates in priority order until one accepts.
		granted := false
		for i := range candidates {
			c := candidates[i]
			candidates[i] = candidate{}
			s.mu.Unlock()
			accepted := c.engine.db.Schedule(c.engine)
			// NB: Schedule may return false, and then a new TrySchedule may arrive
			// before the mutex is re-acquired. In this case,
			// trySetWaitingToFalseLocked will not actually set waiting.value to
			// false.
			s.mu.Lock()
			if accepted {
				c.engine.running++
				s.mu.totalRunning++
				granted = true
				break
			}
			c.engine.waiting.trySetWaitingToFalseLocked(c.snapshot)
		}
		if !granted {
			break
		}
	}
	return candidates[:0]
}

// compareCompactions returns a negative value if a is better (higher
// priority) than b, for use with slices.SortFunc.
// Ordering: Optional (false before true), Priority (desc), Score (desc).
func compareCompactions(a, b pebble.WaitingCompaction) int {
	// false (non-optional) sorts before true (optional).
	if a.Optional != b.Optional {
		if !a.Optional {
			return -1
		}
		return 1
	}
	// Higher priority first.
	if c := cmp.Compare(b.Priority, a.Priority); c != 0 {
		return c
	}
	// Higher score first.
	return cmp.Compare(b.Score, a.Score)
}

func (s *MultiEngineCompactionScheduler) hasRegisteredEnginesLocked() bool {
	for _, e := range s.mu.engines {
		if e.registered {
			return true
		}
	}
	return false
}

func (s *MultiEngineCompactionScheduler) pokeBackgroundGranter() {
	// Signal without waiting.
	select {
	case s.pokeBackgroundGranterCh <- struct{}{}:
	default:
	}
}

func (s *MultiEngineCompactionScheduler) backgroundGranter(quiesce <-chan struct{}) {
	var scratch []candidate
	for {
		select {
		case <-s.pokeBackgroundGranterCh:
			scratch = s.tryGrant(scratch)
		case <-s.stopBackgroundGranterCh:
			return
		case <-quiesce:
			return
		}
	}
}
