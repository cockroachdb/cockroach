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

import "context"

// Below are exported to enable testing from kvprober_test.

var (
	ReadEnabled          = readEnabled
	ReadInterval         = readInterval
	NumStepsToPlanAtOnce = numStepsToPlanAtOnce
	PlannerRateLimit     = plannerRateLimit
)

func (p *Prober) Probe(ctx context.Context, db dbGet) {
	p.probe(ctx, db)
}

func (p *Prober) PlannerNext(ctx context.Context) (Step, error) {
	return p.planner.next(ctx)
}
