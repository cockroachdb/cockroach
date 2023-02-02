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
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/grunning"
)

// ElasticCPUWorkHandle groups relevant data for admitted elastic CPU work,
// specifically how much on-CPU time a request is allowed to make use of (used
// for cooperative scheduling with elastic CPU granters).
type ElasticCPUWorkHandle struct {
	// cpuStart captures the running time of the calling goroutine when this
	// handle is constructed.
	cpuStart time.Duration
	// allotted captures how much on-CPU running time this CPU handle permits
	// before indicating that it's over limit. It measures the duration since
	// cpuStart.
	allotted time.Duration

	// This handle is used in tight loops that are sensitive to per-iteration
	// overhead (checking against the running time too can have an effect). To
	// reduce the overhead, the handle internally maintains an estimate for how
	// many iterations at the caller correspond to 1ms of running time. It uses
	// the following set of variables to do so.

	// The number of iterations since we last measured the running time, and how
	// many iterations until we need to do so again.
	itersSinceLastCheck, itersUntilCheck int
	// The running time measured the last time we checked, and the difference
	// between that running time and what this handle was allotted.
	runningTimeAtLastCheck, differenceWithAllottedAtLastCheck time.Duration

	testingOverrideRunningTime func() time.Duration

	testingOverrideOverLimit func() (bool, time.Duration)
}

func newElasticCPUWorkHandle(allotted time.Duration) *ElasticCPUWorkHandle {
	h := &ElasticCPUWorkHandle{allotted: allotted}
	h.cpuStart = h.runningTime()
	return h
}

func (h *ElasticCPUWorkHandle) runningTime() time.Duration {
	if h == nil {
		return time.Duration(0)
	}
	if override := h.testingOverrideRunningTime; override != nil {
		return override()
	}
	return grunning.Elapsed(h.cpuStart, grunning.Time())
}

// OverLimit is used to check whether we're over the allotted elastic CPU
// tokens. It also returns the absolute time difference between how long we ran
// for and what was allotted. Integrated callers are expected to invoke this in
// tight loops (we assume most callers are CPU-intensive and thus have tight
// loops somewhere) and bail once done.
func (h *ElasticCPUWorkHandle) OverLimit() (overLimit bool, difference time.Duration) {
	if h == nil { // not applicable
		return false, time.Duration(0)
	}

	// What we're effectively doing is just:
	//
	// 		runningTime := h.runningTime()
	// 		return runningTime > h.allotted, grunning.Difference(runningTime, h.allotted)
	//
	// But since this is invoked in tight loops where we're sensitive to
	// per-iteration overhead (the naive form described above causes a 5%
	// slowdown in BenchmarkMVCCExportToSST), entirely from invoking
	// grunning.Time() frequently, here we try to reduce how frequently that
	// needs to happen. We try to estimate how many iterations at the caller
	// corresponds to 1ms of running time, and only do the expensive check once
	// we've crossed that number. It's fine to be slightly over limit since we
	// adjust for it elsewhere by penalizing subsequent waiters.
	h.itersSinceLastCheck++
	if h.itersSinceLastCheck < h.itersUntilCheck {
		return false, h.differenceWithAllottedAtLastCheck
	}
	return h.overLimitInner()
}

func (h *ElasticCPUWorkHandle) overLimitInner() (overLimit bool, difference time.Duration) {
	if h.testingOverrideOverLimit != nil {
		return h.testingOverrideOverLimit()
	}

	runningTime := h.runningTime()
	if runningTime >= h.allotted {
		return true, grunning.Difference(runningTime, h.allotted)
	}

	if h.itersUntilCheck == 0 {
		h.itersUntilCheck = 1
	} else {
		runningTimeSinceLastCheck := grunning.Difference(runningTime, h.runningTimeAtLastCheck)
		if runningTimeSinceLastCheck < time.Millisecond {
			h.itersUntilCheck *= 2
		}
	}

	h.runningTimeAtLastCheck, h.differenceWithAllottedAtLastCheck = runningTime, grunning.Difference(runningTime, h.allotted)
	h.itersSinceLastCheck = 0
	return false, h.differenceWithAllottedAtLastCheck
}

type handleKey struct{}

// ContextWithElasticCPUWorkHandle returns a Context wrapping the supplied elastic
// CPU handle, if any.
func ContextWithElasticCPUWorkHandle(ctx context.Context, h *ElasticCPUWorkHandle) context.Context {
	if h == nil {
		return ctx
	}
	return context.WithValue(ctx, handleKey{}, h)
}

// ElasticCPUWorkHandleFromContext returns the elastic CPU handle contained in the
// Context, if any.
func ElasticCPUWorkHandleFromContext(ctx context.Context) *ElasticCPUWorkHandle {
	val := ctx.Value(handleKey{})
	h, ok := val.(*ElasticCPUWorkHandle)
	if !ok {
		return nil
	}
	return h
}

// TestingNewElasticCPUHandle exports the ElasticCPUWorkHandle constructor for
// testing purposes.
func TestingNewElasticCPUHandle() *ElasticCPUWorkHandle {
	return newElasticCPUWorkHandle(420 * time.Hour) // use a very high allotment
}

// TestingNewElasticCPUWithCallback constructs an
// ElascticCPUWorkHandle with a testing override for the behaviour of
// OverLimit().
func TestingNewElasticCPUHandleWithCallback(cb func() (bool, time.Duration)) *ElasticCPUWorkHandle {
	h := TestingNewElasticCPUHandle()
	h.testingOverrideOverLimit = cb
	return h
}
