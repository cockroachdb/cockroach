// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Label values for the "result" label on execution metrics and events.
const (
	resultSuccess  = "success"
	resultFailed   = "failed"
	resultPanicked = "panicked"
)

// Label values for the "result" label on cleanup metrics and events.
const (
	cleanupResultSuccess = "success"
	cleanupResultFailed  = "failed"
	cleanupResultSkipped = "skipped"
)

// Label values for the "result" label on dependency check events.
const (
	depCheckFailed = "failed"
	depCheckError  = "error"
)

// Label values for the "state" label on worker state metrics.
const (
	workerStateExecuting   = "executing"
	workerStateIdle        = "idle"
	workerStateWaitingLock = "waiting_lock"
)

// Label value for the "operation" label when a worker is idle.
const workerOperationIdle = "idle"

// operationMetrics holds Prometheus metrics for the operations runner.
type operationMetrics struct {
	// activeOps tracks the number of currently running operations (1 while
	// running, 0 otherwise).
	activeOps *prometheus.GaugeVec

	// workerCurrentOperation tracks each worker's current state. Labels:
	// worker (numeric index), operation (name or "idle"), state (executing,
	// idle, waiting_lock).
	workerCurrentOperation *prometheus.GaugeVec

	// pendingCleanups tracks cleanups waiting to execute.
	pendingCleanups *prometheus.GaugeVec
}

func newOperationMetrics(factory promauto.Factory) *operationMetrics {
	return &operationMetrics{
		activeOps: factory.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "roachtest",
			Subsystem: "operation",
			Name:      "active_operations",
			Help:      "Number of currently running operations.",
		}, []string{"operation", "worker"}),

		workerCurrentOperation: factory.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "roachtest",
			Subsystem: "operation",
			Name:      "worker_current_operation",
			Help:      "Current state of each worker. 1 when worker is in the given state for the given operation.",
		}, []string{"worker", "operation", "state"}),

		pendingCleanups: factory.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "roachtest",
			Subsystem: "operation",
			Name:      "pending_cleanups",
			Help:      "Number of cleanups waiting to execute.",
		}, []string{"operation"}),
	}
}
