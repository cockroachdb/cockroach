// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqltelemetry

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
)

// DistSQLExecCounter is to be incremented whenever a query is distributed
// across multiple nodes.
var DistSQLExecCounter = telemetry.GetCounterOnce("sql.exec.query.is-distributed")

// VecExecCounter is to be incremented whenever a query runs with the vectorized
// execution engine.
var VecExecCounter = telemetry.GetCounterOnce("sql.exec.query.is-vectorized")

// VecModeCounter is to be incremented every time the vectorized execution mode
// is changed (including turned off).
func VecModeCounter(mode string) telemetry.Counter {
	return telemetry.GetCounter(fmt.Sprintf("sql.exec.vectorized-setting.%s", mode))
}

// CascadesLimitReached is to be incremented whenever the limit of foreign key
// cascade for a single query is exceeded.
var CascadesLimitReached = telemetry.GetCounterOnce("sql.exec.cascade-limit-reached")

// RecursionDepthLimitReached is to be incremented whenever the limit of nested
// triggers and/or recursive UDFs for a single query is exceeded.
var RecursionDepthLimitReached = telemetry.GetCounterOnce("sql.exec.recursion-depth-limit-reached")

// HashAggregationDiskSpillingDisabled is to be incremented whenever the disk
// spilling of the vectorized hash aggregator is disabled.
var HashAggregationDiskSpillingDisabled = telemetry.GetCounterOnce("sql.exec.hash-agg-spilling-disabled")

// DistributedErrorLocalRetryAttempt is to be incremented whenever a distributed
// query error results in a local rerun.
var DistributedErrorLocalRetryAttempt = telemetry.GetCounterOnce("sql.exec.dist-query-rerun-locally-attempt")

// DistributedErrorLocalRetrySuccess is to be incremented whenever the local
// rerun - of a distributed query that hit SQL retryable error - failed itself.
var DistributedErrorLocalRetryFailure = telemetry.GetCounterOnce("sql.exec.dist-query-rerun-locally-failure")
