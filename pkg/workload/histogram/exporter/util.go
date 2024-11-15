// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package exporter

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/util"
	"github.com/codahale/hdrhistogram"
	"github.com/gogo/protobuf/proto"
	prom "github.com/prometheus/client_model/go"
)

var (
	summaryQuantiles = []float64{50, 95, 99, 100}
)

// ConvertHdrHistogramToPrometheusMetricFamily converts a Hdr histogram into MetricFamily which is used
// by expfmt.MetricFamilyToOpenMetrics to export openmetrics
func ConvertHdrHistogramToPrometheusMetricFamily(
	h *hdrhistogram.Histogram, name *string, start time.Time, labels []*prom.LabelPair,
) *prom.MetricFamily {

	// We are emitting a summary metric rather than the whole histogram.
	summary := &prom.Summary{}
	totalCount := uint64(h.TotalCount())

	var valueQuantiles []*prom.Quantile
	for _, quantile := range summaryQuantiles {
		value := float64(h.ValueAtQuantile(quantile))
		if value == 0 {
			continue
		}
		valueQuantile := prom.Quantile{
			Quantile: &quantile,
			Value:    &value,
		}

		valueQuantiles = append(valueQuantiles, &valueQuantile)
	}
	summary.Quantile = valueQuantiles
	summary.SampleCount = &totalCount
	timestampMs := proto.Int64(start.UTC().UnixMilli())
	sanitizedName := util.SanitizeMetricName(*name)
	return &prom.MetricFamily{
		Name: &sanitizedName,
		Type: prom.MetricType_SUMMARY.Enum(),
		Metric: []*prom.Metric{{
			Summary:     summary,
			TimestampMs: timestampMs,
			Label:       labels,
		}},
	}
}
