// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
)

// ElasticCPUWorkHandle groups relevant data for admitted elastic CPU work,
// specifically how much on-CPU time a request is allowed to make use of (used
// for cooperative scheduling with elastic CPU granters).
type ElasticCPUWorkHandle struct {
	tenantID roachpb.TenantID
	// cpuStart captures the running time of the calling goroutine when this
	// handle is constructed.
	cpuStart time.Duration
	// allotted captures how much on-CPU running time this CPU handle permits
	// before indicating that it's over limit. It measures the duration since
	// cpuStart.
	allotted time.Duration
	// preWork measures how much on-CPU running time this request accrued before
	// starting doing the actual work it intended to. We don't want the preWork
	// duration to count against what it was allotted, but we still want to
	// track to deduct an appropriate number of granter tokens.
	preWork time.Duration

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

	testingOverrideRunningTime func() time.Duration // overrides the time since cpuStart
	testingOverrideOverLimit   func() (bool, time.Duration)
}

func newElasticCPUWorkHandle(
	tenantID roachpb.TenantID, allotted time.Duration,
) *ElasticCPUWorkHandle {
	h := &ElasticCPUWorkHandle{tenantID: tenantID, allotted: allotted}
	h.cpuStart = grunning.Time()
	return h
}

// StartTimer is used to denote that we're just starting to do the actual on-CPU
// work we acquired CPU tokens for. It's possible for requests to do
// semi-unrelated pre-work up until this point, time spent that we don't want to
// count against our allotted CPU time. But we still want to deduct granter
// tokens for that time to avoid over-admission. StartTimer makes note of how
// much pre-work was done and starts counting any subsequent on-CPU time against
// what was allotted. If StartTimer is never invoked, all on-CPU time is counted
// against what was allotted. It can only be called once.
func (h *ElasticCPUWorkHandle) StartTimer() {
	if h == nil {
		return
	}
	h.preWork = h.runningTime()
	h.cpuStart = grunning.Time()
}

// runningTime returns the time spent since cpuStart. cpuStart is captured when
// the handle is first constructed, and optionally reset if callers make use of
// StartTimer.
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
// tokens. If StartTimer was invoked, we start measuring on-CPU time only after
// the invocation. It also returns the total time difference between how long we
// ran for and what was allotted. The difference includes pre-work, if any. If
// the difference is positive we've exceeded what was allotted, and vice versa.
// It's possible to not be OverLimit but still have exceeded the allotment
// (because of excessive pre-work).
//
// Integrated callers are expected to invoke this in tight loops (we assume most
// callers are CPU-intensive and thus have tight loops somewhere) and bail once
// done.
//
// TODO(irfansharif): Non-test callers use one or the other return value, not
// both. Split this API?
func (h *ElasticCPUWorkHandle) OverLimit() (overLimit bool, difference time.Duration) {
	if h == nil { // not applicable
		return false, time.Duration(0)
	}

	// What we're effectively doing is just:
	//
	// 		runningTime := h.runningTime()
	// 		return runningTime > h.allotted, preWork + runningTime - h.allotted
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
		return false, h.preWork + h.differenceWithAllottedAtLastCheck
	}
	return h.overLimitInner()
}

// RunningTime returns the pre-work duration and the work duration. This
// should not be called in a tight loop (unlike OverLimit()). Expected usage
// is to call this after OverLimit() has returned true, in order to get stats
// about CPU usage.
func (h *ElasticCPUWorkHandle) RunningTime() (preWork time.Duration, work time.Duration) {
	if h == nil {
		return 0, 0
	}
	return h.preWork, h.runningTime()
}

func (h *ElasticCPUWorkHandle) overLimitInner() (overLimit bool, difference time.Duration) {
	if h.testingOverrideOverLimit != nil {
		return h.testingOverrideOverLimit()
	}

	runningTime := h.runningTime()
	if runningTime >= h.allotted {
		return true, h.preWork + (runningTime - h.allotted)
	}

	if h.itersUntilCheck == 0 {
		h.itersUntilCheck = 1
	} else {
		runningTimeSinceLastCheck := grunning.Difference(runningTime, h.runningTimeAtLastCheck)
		if runningTimeSinceLastCheck < time.Millisecond {
			h.itersUntilCheck *= 2
		}
	}

	h.runningTimeAtLastCheck, h.differenceWithAllottedAtLastCheck = runningTime, runningTime-h.allotted
	h.itersSinceLastCheck = 0
	return false, h.differenceWithAllottedAtLastCheck + h.preWork
}

// TestingOverrideOverLimit allows tests to override the behaviour of
// OverLimit().
func (h *ElasticCPUWorkHandle) TestingOverrideOverLimit(f func() (bool, time.Duration)) {
	h.testingOverrideOverLimit = f
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
	return newElasticCPUWorkHandle(roachpb.SystemTenantID, 420*time.Hour) // use a very high allotment
}

// TestingNewElasticCPUHandleWithCallback constructs an ElasticCPUWorkHandle
// with a testing override for the behaviour of OverLimit().
func TestingNewElasticCPUHandleWithCallback(cb func() (bool, time.Duration)) *ElasticCPUWorkHandle {
	h := TestingNewElasticCPUHandle()
	h.testingOverrideOverLimit = cb
	return h
}
