// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvprober

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

// Below are exported to enable testing from kvprober_test.

var (
	ReadEnabled          = readEnabled
	ReadInterval         = readInterval
	WriteEnabled         = writeEnabled
	WriteInterval        = writeInterval
	NumStepsToPlanAtOnce = numStepsToPlanAtOnce
)

func (p *Prober) ReadProbe(ctx context.Context, db *kv.DB) {
	p.readProbe(ctx, p.readPlanner)
}

func (p *Prober) WriteProbe(ctx context.Context, db *kv.DB) {
	p.writeProbe(ctx, p.writePlanner)
}

type recordingPlanner struct {
	pl   planner
	last Step
}

func (rp *recordingPlanner) next(ctx context.Context) (Step, error) {
	s, err := rp.pl.next(ctx)
	rp.last = s
	return s, err
}

func (p *Prober) WriteProbeReturnLastStep(ctx context.Context, db *kv.DB) *Step {
	rp := &recordingPlanner{}
	rp.pl = p.writePlanner
	p.writeProbe(ctx, rp)
	return &rp.last
}

func (p *Prober) ReadPlannerNext(ctx context.Context) (Step, error) {
	return p.readPlanner.next(ctx)
}

func (p *Prober) SetPlanningRateLimits(d time.Duration) {
	p.readPlanner.(*meta2Planner).getRateLimit = func(_ time.Duration, _ *cluster.Settings) time.Duration {
		return d
	}
	p.writePlanner.(*meta2Planner).getRateLimit = func(_ time.Duration, _ *cluster.Settings) time.Duration {
		return d
	}
}
