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
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/pebble"
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

	// testingDisablePeriodicGranter, if true, prevents the periodic granter
	// goroutine from starting. Used in deterministic tests that trigger
	// granting explicitly.
	testingDisablePeriodicGranter bool
}

// EngineType identifies the type of engine.
type EngineType uint8

const (
	// LogEngine is the engine used for the raft log.
	LogEngine EngineType = iota
	// StateEngine is the engine used for the state machine.
	StateEngine
)

// MultiEngineCompactionScheduler enforces a global compaction concurrency
// limit across multiple Pebble engines (e.g. log and state engines in a
// CockroachDB store), while respecting per-engine soft limits from
// GetAllowedWithoutPermission and deprioritizing log engine compactions.
//
// Each engine gets an engineState that implements pebble.CompactionScheduler
// (passed to pebble.Options.CompactionScheduler) and pebble.CompactionGrantHandle.
type MultiEngineCompactionScheduler struct {
	opts SchedulerOptions
	mu   struct {
		sync.Mutex
		engines      []*engineState
		totalRunning int
		// isGranting serializes granting from Done and periodicGranter, and
		// ensures granting is stopped before Unregister returns.
		isGranting     bool
		isGrantingCond *sync.Cond
		closed         bool
	}
	stopPeriodicGranterCh chan struct{}
	pokePeriodicGranterCh chan struct{}
}

// engineState implements pebble.CompactionScheduler (one per engine) and
// pebble.CompactionGrantHandle.
type engineState struct {
	scheduler  *MultiEngineCompactionScheduler
	engineType EngineType
	// db is not protected by mutex, but is not read unless registered is true.
	// It transitions once from nil to non-nil.
	db pebble.DBForCompaction
	// Fields below are protected by scheduler.mu.
	running                      int
	waiting                      waitingState
	registered                   bool
	lastAllowedWithoutPermission int
	numGoroutinesPerCompaction   int
}

// waitingState tracks whether an engine has waiting compactions. The
// tryGetCount field prevents spuriously setting value to false when a new
// TrySchedule arrived between sampling and the false transition. waitingState
// is a concurrency optimization that avoids having to repeatedly release
// scheduler.mu to call DBForCompaction.GetWaitingCompaction for engines that
// have no waiting compactions.
type waitingState struct {
	value       bool
	tryGetCount int
}

var _ pebble.CompactionScheduler = &engineState{}
var _ pebble.CompactionGrantHandle = &engineState{}

// NewMultiEngineCompactionScheduler creates a scheduler that enforces a global
// concurrency limit across multiple engines. The stopper manages the periodic
// granter goroutine's lifecycle.
func NewMultiEngineCompactionScheduler(
	opts SchedulerOptions, stopper *stop.Stopper,
) *MultiEngineCompactionScheduler {
	s := &MultiEngineCompactionScheduler{
		opts:                  opts,
		stopPeriodicGranterCh: make(chan struct{}),
		pokePeriodicGranterCh: make(chan struct{}, 1),
	}
	s.mu.isGrantingCond = sync.NewCond(&s.mu.Mutex)
	if !opts.testingDisablePeriodicGranter {
		if err := stopper.RunAsyncTask(
			context.Background(), "multi-compaction-scheduler",
			func(ctx context.Context) {
				s.periodicGranter(stopper.ShouldQuiesce())
			},
		); err != nil {
			// Stopper is already quiescing; Close will be a no-op.
			s.mu.closed = true
		}
	}
	return s
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
	s.mu.engines = append(s.mu.engines, e)
	return e
}

// Close stops the periodic granter goroutine and waits for any in-progress
// granting to complete.
func (s *MultiEngineCompactionScheduler) Close() {
	if !s.opts.testingDisablePeriodicGranter {
		s.stopPeriodicGranterCh <- struct{}{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.closed = true
	for s.mu.isGranting {
		s.mu.isGrantingCond.Wait()
	}
}

// Register implements pebble.CompactionScheduler.
func (e *engineState) Register(numGoroutinesPerCompaction int, db pebble.DBForCompaction) {
	e.db = db
	s := e.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	e.registered = true
	e.numGoroutinesPerCompaction = numGoroutinesPerCompaction
}

// Unregister implements pebble.CompactionScheduler.
func (e *engineState) Unregister() {
	s := e.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	e.registered = false
	// Wait for isGranting to become false so no further calls to this
	// engine's DBForCompaction methods will be made.
	for s.mu.isGranting {
		s.mu.isGrantingCond.Wait()
	}
}

// TrySchedule implements pebble.CompactionScheduler.
func (e *engineState) TrySchedule() (bool, pebble.CompactionGrantHandle) {
	s := e.scheduler
	s.mu.Lock()
	defer s.mu.Unlock()
	if !e.registered {
		return false, nil
	}
	// Don't race ahead of tryGrant — if granting is in progress, just mark
	// as waiting so the granter picks this up. If this tryGrant misses it,
	// the periodic call to tryGrant will pick it up within 100ms.
	if s.mu.isGranting {
		e.waiting.value = true
		e.waiting.tryGetCount++
		return false, nil
	}
	allowed := e.db.GetAllowedWithoutPermission()
	e.lastAllowedWithoutPermission = allowed
	if e.running < allowed && s.mu.totalRunning < s.opts.GetMaxConcurrency() {
		e.running++
		s.mu.totalRunning++
		return true, e
	}
	e.waiting.value = true
	e.waiting.tryGetCount++
	return false, nil
}

// UpdateGetAllowedWithoutPermission implements pebble.CompactionScheduler.
//
// nolint:deferunlockcheck
func (e *engineState) UpdateGetAllowedWithoutPermission() {
	s := e.scheduler
	s.mu.Lock()
	allowed := e.db.GetAllowedWithoutPermission()
	poke := allowed > e.lastAllowedWithoutPermission
	e.lastAllowedWithoutPermission = allowed
	s.mu.Unlock()
	if poke {
		// Signal without waiting.
		select {
		case s.pokePeriodicGranterCh <- struct{}{}:
		default:
		}
	}
}

// Started implements pebble.CompactionGrantHandle.
func (e *engineState) Started() {}

// MeasureCPU implements pebble.CompactionGrantHandle.
func (e *engineState) MeasureCPU(pebble.CompactionGoroutineKind) {}

// CumulativeStats implements pebble.CompactionGrantHandle.
func (e *engineState) CumulativeStats(stats pebble.CompactionGrantHandleStats) {}

// Done implements pebble.CompactionGrantHandle.
func (e *engineState) Done() {
	s := e.scheduler
	s.mu.Lock()
	e.running--
	s.mu.totalRunning--
	s.tryGrantLockedAndUnlock() // nolint:deferunlockcheck
}

// tryGrantLockedAndUnlock attempts to grant compaction slots to waiting
// engines. Must be called with s.mu held; returns with s.mu unlocked.
//
// nolint:deferunlockcheck
func (s *MultiEngineCompactionScheduler) tryGrantLockedAndUnlock() {
	defer s.mu.Unlock()
	// Check if all engines are unregistered or scheduler is closed.
	if s.mu.closed || !s.hasRegisteredEnginesLocked() {
		return
	}
	// Wait for turn to grant.
	for s.mu.isGranting {
		s.mu.isGrantingCond.Wait()
		if s.mu.closed || !s.hasRegisteredEnginesLocked() {
			return
		}
	}
	s.mu.isGranting = true

	// Outer loop: keep granting while we have global capacity.
	for s.mu.totalRunning < s.opts.GetMaxConcurrency() {
		type candidate struct {
			engine   *engineState
			wc       pebble.WaitingCompaction
			snapshot waitingState
		}
		var candidates []candidate

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
			s.mu.Lock()
			if !waiting {
				e.trySetWaitingToFalse(snapshot)
				continue
			}
			// Apply deprioritization to log engine scores.
			if e.engineType == LogEngine {
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
			s.mu.Unlock()
			accepted := c.engine.db.Schedule(c.engine)
			s.mu.Lock()
			if accepted {
				c.engine.running++
				s.mu.totalRunning++
				granted = true
				break
			}
			c.engine.trySetWaitingToFalse(c.snapshot)
		}
		if !granted {
			break
		}
	}

	s.mu.isGranting = false
	s.mu.isGrantingCond.Broadcast()
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

// trySetWaitingToFalse sets waiting.value to false only if tryGetCount hasn't
// changed since the snapshot, meaning no new TrySchedule arrived.
func (e *engineState) trySetWaitingToFalse(snapshot waitingState) {
	if e.waiting.tryGetCount == snapshot.tryGetCount {
		e.waiting.value = false
	}
}

// nolint:deferunlockcheck
func (s *MultiEngineCompactionScheduler) periodicGranter(quiesce <-chan struct{}) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			s.tryGrantLockedAndUnlock()
		case <-s.pokePeriodicGranterCh:
			s.mu.Lock()
			s.tryGrantLockedAndUnlock()
		case <-s.stopPeriodicGranterCh:
			return
		case <-quiesce:
			return
		}
	}
}
