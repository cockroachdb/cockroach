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
	io_prometheus_client "github.com/prometheus/client_model/go"
)

func sanitizeOpenmetricsLabels(input string) string {
	regex := regexp.MustCompile(`[^a-zA-Z0-9_]`)
	sanitized := regex.ReplaceAllString(input, "_")
	firstCharRegex := regexp.MustCompile(`^[^a-zA-Z_]`)
	sanitized = firstCharRegex.ReplaceAllString(sanitized, "_")
	return sanitized
}

// ConvertHdrHistogramToPrometheusMetricFamily converts a Hdr histogram into MetricFamily which can be used
// by expfmt.MetricFamilyToOpenMetrics to export openmetrics
func ConvertHdrHistogramToPrometheusMetricFamily(
	h *hdrhistogram.Histogram, name *string, start time.Time, labels *map[string]string,
) *io_prometheus_client.MetricFamily {
	hist := &io_prometheus_client.Histogram{}

	bars := h.Distribution()
	hist.Bucket = make([]*io_prometheus_client.Bucket, 0, len(bars))

	var cumCount uint64
	var sum float64
	for _, bar := range bars {
		if bar.Count == 0 {
			// No need to expose trivial buckets.
			continue
		}
		upperBound := float64(bar.To)
		sum += upperBound * float64(bar.Count)

		cumCount += uint64(bar.Count)
		curCumCount := cumCount

		hist.Bucket = append(hist.Bucket, &io_prometheus_client.Bucket{
			CumulativeCount: &curCumCount,
			UpperBound:      &upperBound,
		})
	}
	hist.SampleCount = &cumCount
	hist.SampleSum = &sum // can do better here; we approximate in the loop

	var labelValues []*io_prometheus_client.LabelPair
	if labels != nil {
		for label, value := range *labels {
			labelName := sanitizeOpenmetricsLabels(label)
			labelValue := sanitizeOpenmetricsLabels(value)
			labelPair := &io_prometheus_client.LabelPair{
				Name:  &labelName,
				Value: &labelValue,
			}
			labelValues = append(labelValues, labelPair)
		}
	}

	return &io_prometheus_client.MetricFamily{
		Name: name,
		Type: io_prometheus_client.MetricType_HISTOGRAM.Enum(),
		Metric: []*io_prometheus_client.Metric{{
			Histogram:   hist,
			TimestampMs: proto.Int64(start.UTC().UnixMilli()),
			Label:       labelValues,
		}},
	}
}
