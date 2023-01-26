// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metric_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

func TestHistogramPrometheus(t *testing.T) {
	// Regression test against https://github.com/cockroachdb/cockroach/pull/88331.
	// The output includes buckets for which the upper bound equals the previous
	// bucket's upper bound.
	h := metric.NewHistogram(metric.HistogramOptions{
		Mode:     metric.HistogramModePrometheus,
		Metadata: metric.Metadata{},
		Duration: time.Second,
		Buckets:  []float64{1, 2, 3, 4, 5, 6, 10, 20, 30},
	})
	h.RecordValue(1)
	h.RecordValue(5)
	h.RecordValue(5)
	h.RecordValue(10)
	act, err := json.MarshalIndent(*h.ToPrometheusMetric().Histogram, "", "  ")
	require.NoError(t, err)
	echotest.Require(t, string(act), datapathutils.TestDataPath(t, "histogram.txt"))
}
