// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type elasticCPUWorkQueue struct {
	workQueue *WorkQueue
	granter   granter
	metrics   *elasticCPUGranterMetrics
}

func (e *elasticCPUWorkQueue) Admit(
	ctx context.Context, duration time.Duration, info WorkInfo,
) (elasticCPUWorkHandle, error) {
	info.requestedCount = duration.Nanoseconds()
	enabled, err := e.workQueue.Admit(ctx, info)
	if err != nil {
		return elasticCPUWorkHandle{}, err
	}
	if !enabled {
		return elasticCPUWorkHandle{}, nil
	}
	e.metrics.AcquiredNanos.Inc(duration.Nanoseconds())
	e.metrics.Acquisitions.Inc(1)
	return newElasticCPUWorkHandle(duration), nil
}

func (e *elasticCPUWorkQueue) AdmittedWorkDone(h elasticCPUWorkHandle) {
	if !h.enabled {
		return // nothing to do
	}
	overLimit, difference := h.overLimit()
	if overLimit {
		e.granter.tookWithoutPermission(difference.Nanoseconds())
		e.metrics.AcquiredNanos.Inc(difference.Nanoseconds())
		return
	}
	e.granter.returnGrant(difference.Nanoseconds())
	e.metrics.ReturnedNanos.Inc(difference.Nanoseconds())
}

// SetTenantWeights passes through to WorkQueue.SetTenantWeights.
func (e *elasticCPUWorkQueue) SetTenantWeights(tenantWeights map[uint64]uint32) {
	e.workQueue.SetTenantWeights(tenantWeights)
}

func (e *elasticCPUWorkQueue) close() {
	e.workQueue.close()
}

func makeElasticCPUStoreWorkQueue(
	ambientCtx log.AmbientContext,
	settings *cluster.Settings,
	granter granter,
	metrics *elasticCPUGranterMetrics,
	opts workQueueOptions,
) *elasticCPUWorkQueue {
	q := &elasticCPUWorkQueue{
		workQueue: &WorkQueue{},
		granter:   granter,
		metrics:   metrics,
	}
	initWorkQueue(q.workQueue, ambientCtx, KVWork, granter, settings, opts)
	return q
}

type elasticCPUWorkHandle struct {
	enabled            bool
	cpuStart, allotted time.Duration
}

func newElasticCPUWorkHandle(allotted time.Duration) elasticCPUWorkHandle {
	return elasticCPUWorkHandle{
		enabled:  true,
		allotted: allotted,
		cpuStart: grunning.Time(),
	}
}

func (h elasticCPUWorkHandle) runningTime() time.Duration {
	if !h.enabled {
		return time.Duration(0)
	}
	return grunning.Subtract(grunning.Time(), h.cpuStart)
}

func (h elasticCPUWorkHandle) overLimit() (overLimit bool, difference time.Duration) {
	if !h.enabled { // not applicable
		return false, time.Duration(0)
	}
	runningTime := h.runningTime()
	if runningTime > h.allotted {
		return true, grunning.Subtract(runningTime, h.allotted)
	}
	return false, grunning.Subtract(h.allotted, runningTime)

	// XXX: Evaluate overhead in tight loops when token bucket limit == +inf.
}
