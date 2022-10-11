// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metrics

import "context"

// RatedStoreMetricListener implements the Listen interface and rates select
// store fields as they are recorded.
type RatedStoreMetricListener struct {
	listener StoreMetricsListener
	rater    rateMapper
}

// NewRatedStoreMetricListener returns a new RatedStoreMetricListener.
func NewRatedStoreMetricListener(listener StoreMetricsListener) *RatedStoreMetricListener {
	return &RatedStoreMetricListener{listener: listener}
}

// Listen implements the metrics Listen interface.
func (rml *RatedStoreMetricListener) Listen(ctx context.Context, sms []StoreMetrics) {
	rml.listener.Listen(ctx, rml.rater.rate(sms))
}
