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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

const (
	minElasticCPUDuration = 10 * time.Millisecond
	maxElasticCPUDuration = 100 * time.Millisecond
)

var (
	elasticCPUControlEnabled = settings.RegisterBoolSetting(
		settings.SystemOnly,
		"admission.elastic_cpu.enabled",
		"when true, backup work performed by the KV layer is subject to admission control",
		false,
	)

	// ElasticCPUDurationPerExportRequest controls how many CPU tokens are
	// allotted for each export request.
	ElasticCPUDurationPerExportRequest = settings.RegisterDurationSetting(
		settings.SystemOnly,
		"admission.elastic_cpu.duration_per_export_request",
		"controls how many CPU tokens are allotted for each export request",
		maxElasticCPUDuration,
		func(duration time.Duration) error {
			if duration < minElasticCPUDuration {
				return fmt.Errorf("minimum CPU duration allowed per export request is %s, got %s",
					minElasticCPUDuration, duration)
			}
			if duration > maxElasticCPUDuration {
				return fmt.Errorf("maximum CPU duration allowed per export request is %s, got %s",
					maxElasticCPUDuration, duration)
			}
			return nil
		},
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
	if duration < minElasticCPUDuration {
		duration = minElasticCPUDuration
	}
	if duration > maxElasticCPUDuration {
		duration = maxElasticCPUDuration
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

func (e *ElasticCPUWorkQueue) enabled() bool {
	if e.testingEnabled {
		return true
	}

	return elasticCPUControlEnabled.Get(&e.settings.SV)
}

func (e *ElasticCPUWorkQueue) close() {
	e.workQueue.close()
}
