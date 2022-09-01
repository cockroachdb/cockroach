// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package kvprober sends queries to KV in a loop, with configurable sleep
// times, in order to generate data about the healthiness or unhealthiness of
// kvclient & below.
//
// Prober increments metrics that SRE & other operators can use as alerting
// signals. It also writes to logs to help narrow down the problem (e.g. which
// range(s) are acting up).
package kvprober

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type quarantinePool struct {
	steps        []Step
	size         int64
	entryTimeMap map[roachpb.RangeID]time.Time
}

func newQuarantinePool(settings *cluster.Settings) *quarantinePool {
	poolSize := quarantinePoolSize.Get(&settings.SV)
	return &quarantinePool{
		size:         poolSize,
		entryTimeMap: make(map[roachpb.RangeID]time.Time),
		steps:        make([]Step, poolSize),
	}
}

func (qp *quarantinePool) add(ctx context.Context, step Step) {
	if int64(len(qp.steps)) >= qp.size-1 {
		log.Health.Errorf(ctx, "cannot add range %s to quarantine pool, at capacity", step.RangeID.String())
	} else {
		qp.steps = append(qp.steps, step)
		qp.entryTimeMap[step.RangeID] = timeutil.Now()
	}
}

func (qp *quarantinePool) remove(ctx context.Context, step Step) {
	if len(qp.steps) < 1 {
		log.Health.Errorf(ctx, "cannot remove range %s from quarantine pool, pool is empty", step.RangeID.String())
		return
	}
	idx := -1
	for k, v := range qp.steps {
		if v.RangeID == step.RangeID {
			idx = k
			break
		}
	}
	if idx == -1 {
		log.Health.Errorf(ctx, "cannot remove range %s from quarantine pool, not found", step.RangeID.String())
		return
	}
	// Expensive op if pool size is very large.
	qp.steps = append(qp.steps[:idx], qp.steps[idx+1:]...)
	delete(qp.entryTimeMap, step.RangeID)
}

func (qp *quarantinePool) next(ctx context.Context) (Step, error) {
	if len(qp.steps) > 0 {
		step := qp.steps[0]
		return step, nil
	}
	return Step{}, errors.New("there are no keys in quarantine")
}
