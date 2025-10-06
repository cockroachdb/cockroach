// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	adjustTenantUsed(tenantID roachpb.TenantID, additionalUsed int64)
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
	return newElasticCPUWorkHandle(info.TenantID, duration), nil
}

// AdmittedWorkDone indicates to the queue that the admitted work has
// completed.
func (e *ElasticCPUWorkQueue) AdmittedWorkDone(h *ElasticCPUWorkHandle) {
	if h == nil {
		return // nothing to do
	}

	e.metrics.PreWorkNanos.Inc(h.preWork.Nanoseconds())
	_, difference := h.OverLimit()
	e.workQueue.adjustTenantUsed(h.tenantID, difference.Nanoseconds())
	if difference > 0 {
		// We've used up our allotted slice, which we've already deducted tokens
		// for. But we've gone over by difference, which we now need to deduct
		// tokens for.
		e.granter.tookWithoutPermission(difference.Nanoseconds())
		e.metrics.AcquiredNanos.Inc(difference.Nanoseconds())
		e.metrics.OverLimitDuration.RecordValue(difference.Nanoseconds())
		return
	}

	e.granter.returnGrant(-difference.Nanoseconds())
	e.metrics.ReturnedNanos.Inc(-difference.Nanoseconds())
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
