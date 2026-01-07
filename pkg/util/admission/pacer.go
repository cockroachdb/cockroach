// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// Pacer is used in tight loops (CPU-bound) for non-premptible elastic work.
// Callers are expected to invoke Pace() every loop iteration and Close() once
// done. Internally this type integrates with elastic CPU work queue, acquiring
// tokens for the CPU work being done, and blocking if tokens are unavailable.
// This allows for a form of cooperative scheduling with elastic CPU granters.
type Pacer struct {
	unit time.Duration
	wi   WorkInfo
	wq   *ElasticCPUWorkQueue

	cur *ElasticCPUWorkHandle

	// yield, if true, indicates that the ElasticCPUWorkHandle should
	// runtime.Yield() in each Overlimit() call.
	yield bool

	// yieldDelays accumulates yield delays that have not yet been emitted as a
	// tracing span. Once the cumulative delay crosses a threshold (2ms), a span
	// is emitted and this is reset to zero. Any remaining delay < 2ms at Close()
	// is not emitted.
	yieldDelays time.Duration
}

// Pace will block as needed to pace work that calls it. It is
// intended to be called in a tight loop, and will attempt to minimize
// per-call overhead. Non-nil errors are returned only if the context is
// canceled. The readmitted value is set to true if the call involved the
// heavier weight work of asking for admission -- this will be true whenever
// the granted CPU time runs out.
//
// It is safe to call Pace() on a nil *Pacer, but it should not be assumed that
// such a call will always be a no-op: Pace may elect to perform pacing any time
// it is called, even if the *Pacer on which it is called is nil e.g. by
// delegating to the Go runtime or other some global pacing.
func (p *Pacer) Pace(ctx context.Context) (readmitted bool, err error) {
	if p == nil {
		return false, nil
	}

	overLimit, _, yieldDelay := p.cur.IsOverLimitAndPossiblyYield()

	// Inject a tracing span to reflect yield delays if the cumulative delay,
	// including the most recent delay, exceeds the threshold. This should
	// indicate large delays, that individually exceed the threshold, as they
	// happen while still broadly reflecting any significant number of smaller
	// delays that in aggregate become notable.
	if yieldDelay != 0 {
		p.yieldDelays += yieldDelay
		if p.yieldDelays >= 2*time.Millisecond {
			now := timeutil.Now()
			tracing.InjectCompletedSpan(ctx, "admission.yield", now.Add(-p.yieldDelays), p.yieldDelays)
			p.yieldDelays = 0
		}
	}

	if overLimit {
		p.wq.AdmittedWorkDone(p.cur)
		p.cur = nil
	}

	if p.cur == nil {
		handle, err := p.wq.Admit(ctx, p.unit, p.wi, p.yield)
		if err != nil {
			return false, err
		}
		p.cur = handle
	}
	return true, nil
}

// Close is part of the Pacer interface.
func (p *Pacer) Close() {
	if p == nil || p.cur == nil {
		return
	}

	p.wq.AdmittedWorkDone(p.cur)
	p.cur = nil
}
