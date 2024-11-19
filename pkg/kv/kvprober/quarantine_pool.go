// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvprober

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// quarantinePool provides a way of repeatedly probing ranges that
// have previously failed to probe. This provides confirmation of outages
// over a smaller set of ranges at a high confidence.
type quarantinePool struct {
	size func() int64 // can change over time
	mu   struct {
		syncutil.Mutex
		// steps are the queue of ranges to probe in quarantine.
		steps []Step
		// entryTimeMap keeps track of when ranges enter the quarantine
		// pool. This is used to determine the WriteProbeQuarantineOldestDuration
		// metric.
		entryTimeMap map[roachpb.RangeID]time.Time
	}
}

func newQuarantinePool(settings *cluster.Settings) *quarantinePool {
	return &quarantinePool{
		size: func() int64 { return quarantinePoolSize.Get(&settings.SV) },
	}
}

func (qp *quarantinePool) maybeAdd(ctx context.Context, step Step) (added bool) {
	qp.mu.Lock()
	defer qp.mu.Unlock()
	if _, ok := qp.mu.entryTimeMap[step.RangeID]; ok {
		// Already in the pool.
		return false
	}

	size := qp.size()

	if int64(len(qp.mu.steps)) >= size {
		// The pool is full. Note that we don't log, as we have a full pool of
		// failing ranges, and it should thus be clear that the cluster is likely
		// experiencing a widespread outage.
		//
		// Truncate slice in case size() got lowered.
		qp.mu.steps = qp.mu.steps[:size]
		return false
	}
	qp.mu.steps = append(qp.mu.steps, step)
	if qp.mu.entryTimeMap == nil {
		qp.mu.entryTimeMap = map[roachpb.RangeID]time.Time{}
	}
	qp.mu.entryTimeMap[step.RangeID] = timeutil.Now()
	return true
}

func (qp *quarantinePool) oldestDuration() time.Duration {
	qp.mu.Lock()
	defer qp.mu.Unlock()

	now := timeutil.Now()
	var max time.Duration
	for _, then := range qp.mu.entryTimeMap {
		dur := now.Sub(then)
		if dur > max {
			max = dur
		}
	}
	return max
}

func (qp *quarantinePool) maybeRemove(ctx context.Context, step Step) {
	qp.mu.Lock()
	defer qp.mu.Unlock()
	if _, found := qp.mu.entryTimeMap[step.RangeID]; !found {
		return
	}
	delete(qp.mu.entryTimeMap, step.RangeID)
	idx := -1
	for k, v := range qp.mu.steps {
		if v.RangeID == step.RangeID {
			idx = k
			break
		}
	}
	if idx == -1 {
		// This is a programming error. We had an entry in entryTimeMap, but can't
		// find the corresponding step.
		log.Health.Errorf(ctx, "inconsistent from quarantine pool: %s not found", step.RangeID)
		return
	}
	qp.mu.steps = append(qp.mu.steps[:idx], qp.mu.steps[idx+1:]...)
}

func (qp *quarantinePool) next(ctx context.Context) (Step, error) {
	qp.mu.Lock()
	defer qp.mu.Unlock()

	if len(qp.mu.steps) == 0 {
		return Step{}, nil
	}

	step := qp.mu.steps[0]
	return step, nil
}
