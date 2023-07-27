// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

// HashAggregationDiskSpillingDisabled is to be incremented whenever the disk
// spilling of the vectorized hash aggregator is disabled.
var HashAggregationDiskSpillingDisabled = telemetry.GetCounterOnce("sql.exec.hash-agg-spilling-disabled")
