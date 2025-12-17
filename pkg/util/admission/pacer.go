// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Pacer's internals rely on our fork of Go. See pacer_nofork.go.
//go:build bazel

package admission

import (
	"context"
	"runtime"
	"time"
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

	// Yield, if true, indicates that the Pacer should runtime.Yield() in each
	// Pace() call, even if the call is otherwise a no-op due to wq being nil i.e.
	// when time-based pacing is not enabled. Eventually this might just become
	// the default behavior for nil *Pacer, but the bool allows it to be opt-in
	// initially.
	Yield bool
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

	if p.Yield {
		runtime.Yield()
	}

	if p.wq == nil {
		return false, nil
	}

	if overLimit, _ := p.cur.OverLimit(); overLimit {
		p.wq.AdmittedWorkDone(p.cur)
		p.cur = nil
	}

	if p.cur == nil {
		handle, err := p.wq.Admit(ctx, p.unit, p.wi)
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
