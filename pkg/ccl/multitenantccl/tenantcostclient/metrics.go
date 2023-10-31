// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostclient

import "github.com/cockroachdb/cockroach/pkg/util/metric"

var (
	metaCurrentBlocked = metric.Metadata{
		Name:        "tenant.cost_client.blocked_requests",
		Help:        "Number of requests currently blocked by the rate limiter",
		Measurement: "Requests",
		Unit:        metric.Unit_COUNT,
	}
)

// metrics manage the metrics used by the tenant cost client.
type metrics struct {
	CurrentBlocked *metric.Gauge
}

// Init initializes the tenant cost client metrics.
func (m *metrics) Init() {
	m.CurrentBlocked = metric.NewGauge(metaCurrentBlocked)
}
