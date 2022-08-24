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

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var (
	elasticCPUControlEnabled = settings.RegisterBoolSetting(
		settings.SystemOnly,
		"admission.elastic_cpu.enabled",
		"when true, backup work performed by the KV layer is subject to admission control",
		false,
	)
)

// ElasticCPUWorkQueue maintains a queue of elastic work waiting to be admitted.
type ElasticCPUWorkQueue struct {
	settings  *cluster.Settings
	workQueue *WorkQueue
	granter   granter
	metrics   *elasticCPUGranterMetrics
}

// Admit is called when requesting admission for elastic CPU work.
func (e *ElasticCPUWorkQueue) Admit(
	ctx context.Context, duration time.Duration, info WorkInfo,
) (*ElasticCPUWorkHandle, error) {
	if !elasticCPUControlEnabled.Get(&e.settings.SV) {
		return nil, nil
	}
	info.requestedCount = duration.Nanoseconds()
	enabled, err := e.workQueue.Admit(ctx, info)
	if err != nil {
		return nil, err
	}
	if !enabled {
		return nil, nil
	}
	e.metrics.AcquiredNanos.Inc(duration.Nanoseconds())
	return newElasticCPUWorkHandle(duration), nil
}

// AdmittedWorkDone indicates to the queue that the admitted work has
// completed.
func (e *ElasticCPUWorkQueue) AdmittedWorkDone(h *ElasticCPUWorkHandle) {
	if h == nil {
		return // nothing to do
	}
	overLimit, difference := h.OverLimit()
	if overLimit {
		e.granter.tookWithoutPermission(difference.Nanoseconds())
		e.metrics.AcquiredNanos.Inc(difference.Nanoseconds())
		return
	}
	e.granter.returnGrant(difference.Nanoseconds())
	e.metrics.ReturnedNanos.Inc(difference.Nanoseconds())
}

// SetTenantWeights passes through to WorkQueue.SetTenantWeights.
func (e *ElasticCPUWorkQueue) SetTenantWeights(tenantWeights map[uint64]uint32) {
	e.workQueue.SetTenantWeights(tenantWeights)
}

func (e *ElasticCPUWorkQueue) close() {
	e.workQueue.close()
}

func makeElasticCPUStoreWorkQueue(
	ambientCtx log.AmbientContext,
	settings *cluster.Settings,
	granter granter,
	metrics *elasticCPUGranterMetrics,
	opts workQueueOptions,
) *ElasticCPUWorkQueue {
	q := &ElasticCPUWorkQueue{
		settings:  settings,
		workQueue: &WorkQueue{},
		granter:   granter,
		metrics:   metrics,
	}
	initWorkQueue(q.workQueue, ambientCtx, KVWork, granter, settings, opts)
	return q
}

// ElasticCPUWorkHandle groups relevant data for admitted elastic CPU work,
// specifically how much on-CPU time a request is allowed to make use of (used
// for cooperative scheduling with elastic CPU granters).
type ElasticCPUWorkHandle struct {
	cpuStart, allotted time.Duration
}

func newElasticCPUWorkHandle(allotted time.Duration) *ElasticCPUWorkHandle {
	return &ElasticCPUWorkHandle{
		allotted: allotted,
		cpuStart: grunning.Time(),
	}
}

func (h *ElasticCPUWorkHandle) runningTime() time.Duration {
	if h == nil {
		return time.Duration(0)
	}
	return grunning.Difference(grunning.Time(), h.cpuStart)
}

// OverLimit is used to check whether we're over the allotted elastic CPU
// tokens. It also returns the time difference between how long we ran for and
// what was allotted. Integrated callers are expected to invoke this in tight
// loops (we assume most callers are CPU-intensive and thus have tight loops
// somewhere) and bail once done.
//
// TODO(irfansharif): Could this be made smarter/structured as an iterator?
// Perhaps auto-estimating the per-loop-iteration time and only retrieving the
// running time only after the estimated "iters until over limit" has passed. Do
// only if we're sensitive to per-iteration check overhead.
func (h *ElasticCPUWorkHandle) OverLimit() (overLimit bool, difference time.Duration) {
	if h == nil { // not applicable
		return false, time.Duration(0)
	}
	runningTime := h.runningTime()
	return runningTime > h.allotted, grunning.Difference(runningTime, h.allotted)
	// XXX: Evaluate overhead in tight loops when token bucket limit == +inf.
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
