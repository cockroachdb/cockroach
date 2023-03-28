// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import (
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	prometheusgo "github.com/prometheus/client_model/go"
)

// GenerateCatalog creates an array of ChartSections, which is served at
// /_admin/v1/chartcatalog.
//
// The original intent of this method was to inform the layout of DB console
// dashboards, but this use case never manifested. The code is retained only
// because the endpoint made it into our `v1` API. Once that endpoint is
// deprecated and its users have been phased out, the `catalog` package should
// be removed.
//
// The generated catalog is a stub: each metric is returned as its own
// ChartSection in which almost everything is a stub. It is thus suitable for
// discovering the available metrics (though the AllMetricMetadata Admin
// endpoint is more suitable for this task). It's not suitable for laying out
// the metrics in any useful way.
//
// No new uses of this endpoint should be added.
func GenerateCatalog(metadata map[string]metric.Metadata) ([]ChartSection, error) {
	var sl []ChartSection
	avgAgg := tspb.TimeSeriesQueryAggregator_AVG
	for name, meta := range metadata {
		der := tspb.TimeSeriesQueryDerivative_NONE
		if meta.MetricType == prometheusgo.MetricType_COUNTER {
			der = tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE
		}
		sl = append(sl, ChartSection{
			Title:           name,
			LongTitle:       name,
			CollectionTitle: name,
			Description:     name,
			Level:           0,
			Charts: []*IndividualChart{{
				Title:           name,
				LongTitle:       name,
				CollectionTitle: name,
				Downsampler:     &avgAgg,
				Aggregator:      &avgAgg,
				Derivative:      &der,
				Units:           AxisUnits_UNSET_UNITS,
				AxisLabel:       meta.Measurement,
				Metrics: []ChartMetric{
					{
						Name:           name,
						Help:           meta.Help,
						AxisLabel:      meta.Measurement,
						PreferredUnits: AxisUnits_UNSET_UNITS,
						MetricType:     meta.MetricType,
					},
				},
			}},
		})
	}
	return sl, nil
}
