// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
