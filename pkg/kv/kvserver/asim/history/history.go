// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package history

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
)

// History contains recorded information that summarizes a simulation run.
// Currently it only contains the store metrics of the run.
// TODO(kvoli): Add a range log like structure to the history.
type History struct {
	Recorded [][]metrics.StoreMetrics
	S        state.State
}

// Listen implements the metrics.StoreMetricListener interface.
func (h *History) Listen(ctx context.Context, sms []metrics.StoreMetrics) {
	h.Recorded = append(h.Recorded, sms)
}
