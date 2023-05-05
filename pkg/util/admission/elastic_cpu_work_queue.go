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
)

const (
	// MinElasticCPUDuration is the minimum on-CPU time elastic requests can ask
	// when seeking admission.
	MinElasticCPUDuration = 10 * time.Millisecond

	// MaxElasticCPUDuration is the maximum on-CPU time elastic requests can ask
	// when seeking admission.
	MaxElasticCPUDuration = 100 * time.Millisecond
)

var (
	elasticCPUControlEnabled = settings.RegisterBoolSetting(
		settings.SystemOnly,
		"admission.elastic_cpu.enabled",
		"when true, elastic work (like backups) performed by the KV layer is subject to admission control",
		true,
	)
)

// ElasticCPUWorkQueue maintains a queue of elastic work waiting to be admitted.
type ElasticCPUWorkQueue struct {
	settings  *cluster.Settings
	workQueue elasticCPUInternalWorkQueue
	granter   granter
	metrics   *elasticCPUGranterMetrics

	testingEnabled bool
}

// elasticCPUInternalWorkQueue abstracts *WorkQueue for testing.
type elasticCPUInternalWorkQueue interface {
	requester
	Admit(ctx context.Context, info WorkInfo) (enabled bool, err error)
	SetTenantWeights(tenantWeights map[uint64]uint32)
}

func makeElasticCPUWorkQueue(
	settings *cluster.Settings,
	workQueue elasticCPUInternalWorkQueue,
	granter granter,
	metrics *elasticCPUGranterMetrics,
) *ElasticCPUWorkQueue {
	return &ElasticCPUWorkQueue{
		settings:  settings,
		workQueue: workQueue,
		granter:   granter,
		metrics:   metrics,
	}
}

// Admit is called when requesting admission for elastic CPU work.
func (e *ElasticCPUWorkQueue) Admit(
	ctx context.Context, duration time.Duration, info WorkInfo,
) (*ElasticCPUWorkHandle, error) {
	if !e.enabled() {
		return nil, nil
	}
	if duration < MinElasticCPUDuration {
		duration = MinElasticCPUDuration
	}
	if duration > MaxElasticCPUDuration {
		duration = MaxElasticCPUDuration
	}
	info.RequestedCount = duration.Nanoseconds()
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

	e.metrics.PreWorkNanos.Inc(h.preWork.Nanoseconds())
	overLimit, difference := h.OverLimit()
	if overLimit {
		// We've used up our allotted slice, which we've already deducted tokens
		// for. But we've gone over by difference, and we've also done some
		// pre-work which we've yet to deduct tokens for. Do so now.
		additionalNanosToAcquire := h.preWork.Nanoseconds() + difference.Nanoseconds()
		e.granter.tookWithoutPermission(additionalNanosToAcquire) // +difference+preWork
		e.metrics.AcquiredNanos.Inc(additionalNanosToAcquire)
		e.metrics.OverLimitDuration.RecordValue(difference.Nanoseconds())
		return
	}

	// We've done some pre-work that we want to tokens for, but from the
	// allotted slice, we've not used some difference, which we want to return.
	if d := -difference.Nanoseconds() + h.preWork.Nanoseconds(); d > 0 {
		// If -difference+preWork > 0, we need to additionally acquire
		// -difference+preWork nanos.
		additionalNanosToAcquire := d
		e.granter.tookWithoutPermission(additionalNanosToAcquire)
		e.metrics.AcquiredNanos.Inc(additionalNanosToAcquire)
	} else {
		// If -difference+preWork < 0, we need return -(-difference+preWork)
		// nanos
		nanosToReturn := -d
		e.granter.returnGrant(nanosToReturn)
		e.metrics.ReturnedNanos.Inc(nanosToReturn)
	}
}

// SetTenantWeights passes through to WorkQueue.SetTenantWeights.
func (e *ElasticCPUWorkQueue) SetTenantWeights(tenantWeights map[uint64]uint32) {
	e.workQueue.SetTenantWeights(tenantWeights)
}

func (e *ElasticCPUWorkQueue) enabled() bool {
	if e.testingEnabled {
		return true
	}

	return elasticCPUControlEnabled.Get(&e.settings.SV)
}

func (e *ElasticCPUWorkQueue) close() {
	e.workQueue.close()
}
