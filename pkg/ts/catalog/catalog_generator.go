// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catalog

import (
	"fmt"

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
func GenerateCatalog(nodeMd, appMd, srvMd map[string]metric.Metadata) ([]ChartSection, error) {
	var sl []ChartSection
	sl = generateInternal(nodeMd, sl, MetricLayer_STORAGE)
	sl = generateInternal(appMd, sl, MetricLayer_APPLICATION)
	sl = generateInternal(srvMd, sl, MetricLayer_SERVER)
	return sl, nil
}

func generateInternal(
	metadata map[string]metric.Metadata, sl []ChartSection, metricLayer MetricLayer,
) []ChartSection {
	avgAgg := tspb.TimeSeriesQueryAggregator_AVG
	chartSections := make(map[metric.Metadata_Category]*ChartSection)

	for name, meta := range metadata {
		der := tspb.TimeSeriesQueryDerivative_NONE
		if meta.MetricType == prometheusgo.MetricType_COUNTER {
			der = tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE
		}
		origUnit := meta.Unit.String()
		dstUnit := AxisUnits_UNSET_UNITS
		if candidate, ok := AxisUnits_value[origUnit]; ok {
			dstUnit = AxisUnits(candidate)
		}

		if meta.Essential {
			if meta.Category == metric.Metadata_UNSET {
				panic(fmt.Sprintf("Metric %s is essential but has no Category", name))
			}
			if meta.HowToUse == "" {
				panic(fmt.Sprintf("Metric %s is essential but has no HowToUse", name))
			}
		}

		section, ok := chartSections[meta.Category]
		if !ok {
			chartSections[meta.Category] = &ChartSection{
				Title:           meta.Category.String(),
				LongTitle:       meta.Category.String(),
				CollectionTitle: meta.Category.String(),
				Description:     meta.Category.String(),
				Level:           0,
				MetricLayer:     metricLayer,
			}
			section = chartSections[meta.Category]
		}

		section.Charts = append(section.Charts, &IndividualChart{
			Title:           name,
			LongTitle:       name,
			CollectionTitle: name,
			Downsampler:     &avgAgg,
			Aggregator:      &avgAgg,
			Derivative:      &der,
			Units:           dstUnit,
			AxisLabel:       meta.Measurement,
			Metrics: []ChartMetric{
				{
					ExportedName:   metric.ExportedName(name),
					Name:           name,
					Help:           meta.Help,
					AxisLabel:      meta.Measurement,
					PreferredUnits: dstUnit,
					MetricType:     meta.MetricType,
					Essential:      meta.Essential,
					HowToUse:       meta.HowToUse,
				},
			},
		})
	}
	for _, s := range chartSections {
		sl = append(sl, *s)
	}
	return sl
}
