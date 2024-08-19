// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exporter

import (
	"regexp"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/gogo/protobuf/proto"
	prom "github.com/prometheus/client_model/go"
)

var (
	invalidCharRegex      = regexp.MustCompile(`[^a-zA-Z0-9_]`)
	invalidFirstCharRegex = regexp.MustCompile(`^[^a-zA-Z_]`)
)

func sanitizeOpenmetricsLabels(input string) string {
	sanitized := invalidCharRegex.ReplaceAllString(input, "_")
	sanitized = invalidFirstCharRegex.ReplaceAllString(sanitized, "_")
	return sanitized
}

// ConvertHdrHistogramToPrometheusMetricFamily converts a Hdr histogram into MetricFamily which is used
// by expfmt.MetricFamilyToOpenMetrics to export openmetrics
func ConvertHdrHistogramToPrometheusMetricFamily(
	h *hdrhistogram.Histogram, name *string, start time.Time, labels []*prom.LabelPair,
) *prom.MetricFamily {
	hist := &prom.Histogram{}

	bars := h.Distribution()
	hist.Bucket = make([]*prom.Bucket, 0, len(bars))

	var cumCount uint64
	var sum float64
	for _, bar := range bars {
		upperBound := float64(bar.To)
		sum += upperBound * float64(bar.Count)

		cumCount += uint64(bar.Count)
		curCumCount := cumCount

		hist.Bucket = append(hist.Bucket, &prom.Bucket{
			CumulativeCount: &curCumCount,
			UpperBound:      &upperBound,
		})
	}
	hist.SampleCount = &cumCount
	hist.SampleSum = &sum // can do better here; we approximate in the loop

	return &prom.MetricFamily{
		Name: name,
		Type: prom.MetricType_HISTOGRAM.Enum(),
		Metric: []*prom.Metric{{
			Histogram:   hist,
			TimestampMs: proto.Int64(start.UTC().UnixMilli()),
			Label:       labels,
		}},
	}
}
