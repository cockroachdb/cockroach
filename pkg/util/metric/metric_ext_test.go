// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric_test

import (
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"google.golang.org/protobuf/encoding/prototext"
)

// normalizeProtoText normalizes prototext output to use a single space
// after colons, working around differences between prototext library
// versions (some use one space, others two).
var multiSpaceAfterColon = regexp.MustCompile(`(\w):  +`)

func normalizeProtoText(s string) string {
	return multiSpaceAfterColon.ReplaceAllString(s, "$1: ")
}

func TestHistogramPrometheus(t *testing.T) {
	// Verify that the prometheus histogram output produced by
	// goodhistogram is well-formed: monotonically increasing
	// cumulative counts, no duplicate upper bounds, and correct
	// totals.
	h := metric.NewHistogram(metric.HistogramOptions{
		Metadata: metric.Metadata{},
		Duration: time.Second,
		Buckets:  []float64{1, 2, 3, 4, 5, 6, 10, 20, 30},
	})
	h.RecordValue(1)
	h.RecordValue(5)
	h.RecordValue(5)
	h.RecordValue(10)
	act := normalizeProtoText(
		prototext.MarshalOptions{Multiline: true, Indent: "  "}.Format(
			h.ToPrometheusMetric().Histogram))
	echotest.Require(t, act, datapathutils.TestDataPath(t, "histogram.txt"))
}
