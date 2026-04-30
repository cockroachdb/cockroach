// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package repro

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// StepID identifies a discrete step in the execution. All steps are unique, and
// there is sequence of consecutive IDs that represents the entire execution.
type StepID uint32

// Logger is used to log reproduction events.
type Logger interface {
	// Warningf logs a reproduction event.
	Warningf(ctx context.Context, format string, args ...interface{})
}

// BusRW gives exclusive read and write access to a Bus.
type BusRW[Bus any] func(*Bus) bool

// Bloke is a bug reproduction utility, which helps to arrange a certain set of
// events (steps) in a concurrent system to happen in a particular serialized
// order. It also helps to carry arbitrary data between these steps, through a
// Bus data structure specified as a generic type parameter.
//
// The execution is presented as sequence of contiguous StepID starting from 0.
// A valid run completes all StepIDs sequentially and exactly once.
//
// If everything is done right, Bloke guarantees that all steps are completed,
// in order. Otherwise, it deadlocks, in which case it is either an impossible
// sequence, or the runtime concurrency prevented it from happening. In the
// latter case, there must be a way to clarify the steps and prevent deadlocks.
type Bloke[Bus any] struct {
	step uint32
	mu   struct {
		syncutil.Mutex
		bus Bus
		log Logger
	}
	cond sync.Cond
}

// NewBloke creates a new reproduction helper, logging to the given logger.
func NewBloke[Bus any](log Logger) *Bloke[Bus] {
	r := &Bloke[Bus]{}
	r.mu.log = log
	r.cond.L = &r.mu
	return r
}

// Step blocks until the next step to run is ID, and marks it as done. Blocks
// indefinitely if this step has been completed before. The caller must call
// Step for each ID exactly once.
func (r *Bloke[Bus]) Step(id StepID, msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.waitLocked(id)
	r.doneLocked(id, msg)
}

// StepIf blocks until the next step to run is ID, and marks it as done if the
// RW function returns true. Blocks indefinitely if this step has been completed
// before. The caller must ensure that, for any step ID, the RW function returns
// true exactly once.
//
// The caller can read and modify the Bus in the RW function, to observe the
// side effects of all previously completed steps, and pass its own side effects
// to future steps (or future runs of StepIf with the same ID).
func (r *Bloke[Bus]) StepIf(id StepID, rw BusRW[Bus], msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.waitLocked(id)
	if rw(&r.mu.bus) {
		r.doneLocked(id, msg)
	}
}

// StepEx is like StepIf but it does not block. It returns fast if ID is not the
// next step to run, or it has been already completed.
//
// The caller must ensure that it has exclusive ownership of the given step ID,
// i.e. there can not be concurrent calls of StepEx with the same ID. This is
// trivially true if there is only one goroutine calling StepEx for a given ID,
// but if there are multiple, the StepEx calls must be serialized externally.
//
// The caller can read and modify the Bus in the RW function, to observe the
// side effects of all previously completed steps, and pass its own side effects
// to future steps (or future runs of StepIf with the same ID).
func (r *Bloke[Bus]) StepEx(id StepID, rw BusRW[Bus], msg string) {
	// NB: the bus is protected by the mutex, but it can be used without because
	// the caller guarantees exclusive ownership of the step ID.
	if r.stepID() == id && rw(&r.mu.bus) {
		r.Step(id, msg)
	}
}

// Done returns true if the step with the given ID is done.
func (r *Bloke[Bus]) Done(id StepID) bool {
	return r.stepID() > id
}

func (r *Bloke[Bus]) waitLocked(id StepID) {
	for StepID(r.step) != id {
		r.cond.Wait()
	}
}

func (r *Bloke[Bus]) doneLocked(id StepID, msg string) {
	r.mu.log.Warningf(context.Background(), "REPRO[%d]: %s", id, msg)
	atomic.AddUint32(&r.step, 1)
	r.cond.Broadcast()
}

func (r *Bloke[Bus]) stepID() StepID {
	return StepID(atomic.LoadUint32(&r.step))
}
