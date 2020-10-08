// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// Metrics are for production monitoring of each job type.
type Metrics struct {
	Changefeed metric.Struct
}

// MetricStruct implements the metric.Struct interface.
func (Metrics) MetricStruct() {}

// InitHooks initializes the metrics for job monitoring.
func (m *Metrics) InitHooks(histogramWindowInterval time.Duration) {
	if MakeChangefeedMetricsHook != nil {
		m.Changefeed = MakeChangefeedMetricsHook(histogramWindowInterval)
	}
}

// MakeChangefeedMetricsHook allows for registration of changefeed metrics from
// ccl code.
var MakeChangefeedMetricsHook func(time.Duration) metric.Struct
