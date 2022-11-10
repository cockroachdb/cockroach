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
}

// Pace is part of the Pacer interface.
func (p *Pacer) Pace(ctx context.Context) error {
	if p == nil {
		return nil
	}

	if overLimit, _ := p.cur.OverLimit(); overLimit {
		p.wq.AdmittedWorkDone(p.cur)
		p.cur = nil
	}

	if p.cur == nil {
		handle, err := p.wq.Admit(ctx, p.unit, p.wi)
		if err != nil {
			return err
		}
		p.cur = handle
	}
	return nil
}

// Close is part of the Pacer interface.
func (p *Pacer) Close() {
	if p == nil || p.cur == nil {
		return
	}

	p.wq.AdmittedWorkDone(p.cur)
	p.cur = nil
}
