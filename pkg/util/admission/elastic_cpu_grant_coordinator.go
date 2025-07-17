// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

func makeElasticCPUGrantCoordinator(
	ambientCtx log.AmbientContext, st *cluster.Settings, registry *metric.Registry,
) *ElasticCPUGrantCoordinator {
	schedulerLatencyListenerMetrics := makeSchedulerLatencyListenerMetrics()
	registry.AddMetricStruct(schedulerLatencyListenerMetrics)
	elasticCPUGranterMetrics := makeElasticCPUGranterMetrics()
	registry.AddMetricStruct(elasticCPUGranterMetrics)

	elasticWorkQueueMetrics := makeWorkQueueMetrics("elastic-cpu", registry,
		admissionpb.BulkNormalPri, admissionpb.NormalPri)

	elasticCPUGranter := newElasticCPUGranter(ambientCtx, st, elasticCPUGranterMetrics)
	schedulerLatencyListener := newSchedulerLatencyListener(ambientCtx, st, schedulerLatencyListenerMetrics, elasticCPUGranter)

	elasticCPUInternalWorkQueue := &WorkQueue{}
	initWorkQueue(elasticCPUInternalWorkQueue, ambientCtx, KVWork, "kv-elastic-cpu-queue", elasticCPUGranter, st,
		elasticWorkQueueMetrics,
		workQueueOptions{usesTokens: true}, nil /* knobs */) // will be closed by the embedding *ElasticCPUWorkQueue
	elasticCPUWorkQueue := makeElasticCPUWorkQueue(st, elasticCPUInternalWorkQueue, elasticCPUGranter, elasticCPUGranterMetrics)
	elasticCPUGrantCoordinator := newElasticCPUGrantCoordinator(elasticCPUGranter, elasticCPUWorkQueue, schedulerLatencyListener)
	elasticCPUGranter.setRequester(elasticCPUInternalWorkQueue)
	schedulerLatencyListener.setCoord(elasticCPUGrantCoordinator)
	return elasticCPUGrantCoordinator
}

// ElasticCPUGrantCoordinator coordinates grants for elastic CPU tokens, it has
// a single granter-requester pair. Since it's used for elastic CPU work, and
// the total allotment of CPU available for such work is reduced before getting
// close to CPU saturation (we observe 1ms+ p99 scheduling latencies when
// running at 65% utilization on 8vCPU machines, which is enough to affect
// foreground latencies), we don't want it to serve as a gatekeeper for
// SQL-level admission. All this informs why its structured as a separate grant
// coordinator.
//
// TODO(irfansharif): Ideally we wouldn't use this separate
// ElasticGrantCoordinator and just make this part of the one GrantCoordinator
// above but given we're dealing with a different workClass (elasticWorkClass)
// but for an existing WorkKind (KVWork), and not all APIs on the grant
// coordinator currently segment across the two, it was easier to copy over some
// of the mediating code instead (grant chains also don't apply in this scheme).
// Try to do something better here and revisit the existing abstractions; see
// github.com/cockroachdb/cockroach/pull/86638#pullrequestreview-1084437330.
type ElasticCPUGrantCoordinator struct {
	SchedulerLatencyListener SchedulerLatencyListener
	ElasticCPUWorkQueue      *ElasticCPUWorkQueue
	elasticCPUGranter        *elasticCPUGranter
}

func newElasticCPUGrantCoordinator(
	elasticCPUGranter *elasticCPUGranter,
	elasticCPUWorkQueue *ElasticCPUWorkQueue,
	listener *schedulerLatencyListener,
) *ElasticCPUGrantCoordinator {
	return &ElasticCPUGrantCoordinator{
		elasticCPUGranter:        elasticCPUGranter,
		ElasticCPUWorkQueue:      elasticCPUWorkQueue,
		SchedulerLatencyListener: listener,
	}
}

func (e *ElasticCPUGrantCoordinator) close() {
	e.ElasticCPUWorkQueue.close()
}

// tryGrant is used to attempt to grant to waiting requests.
func (e *ElasticCPUGrantCoordinator) tryGrant() {
	e.elasticCPUGranter.tryGrant()
}

// NewPacer implements the PacerMaker interface.
func (e *ElasticCPUGrantCoordinator) NewPacer(unit time.Duration, wi WorkInfo) *Pacer {
	if e == nil {
		return nil
	}
	return &Pacer{
		unit: unit,
		wi:   wi,
		wq:   e.ElasticCPUWorkQueue,
	}
}
