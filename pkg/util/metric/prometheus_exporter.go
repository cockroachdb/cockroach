// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metric

import (
	"io"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// PrometheusExporter contains a map of metric families (a metric with multiple labels).
// It initializes each metric family once and reuses it for each prometheus scrape.
// It is NOT thread-safe.
// TODO(marc): we should really keep our metric objects here so we can avoid creating
// new prometheus.Metric every time we are scraped.
// see: https://github.com/cockroachdb/cockroach/issues/9326
//  pe := MakePrometheusExporter()
//  pe.AddMetricsFromRegistry(nodeRegistry)
//  pe.AddMetricsFromRegistry(storeOneRegistry)
//  ...
//  pe.AddMetricsFromRegistry(storeNRegistry)
//  pe.Export(w)
type PrometheusExporter struct {
	families map[string]*prometheusgo.MetricFamily
}

// MakePrometheusExporter returns an initialized prometheus exporter.
func MakePrometheusExporter() PrometheusExporter {
	return PrometheusExporter{families: map[string]*prometheusgo.MetricFamily{}}
}

// find the family for the passed-in metric, or create and return it if not found.
func (pm *PrometheusExporter) findOrCreateFamily(
	prom PrometheusExportable,
) *prometheusgo.MetricFamily {
	familyName := exportedName(prom.GetName())
	if family, ok := pm.families[familyName]; ok {
		return family
	}

	family := &prometheusgo.MetricFamily{
		Name: proto.String(familyName),
		Help: proto.String(prom.GetHelp()),
		Type: prom.GetType(),
	}

	pm.families[familyName] = family
	return family
}

// ScrapeRegistry scrapes all metrics contained in the registry to the metric
// family map, holding on only to the scraped data (which is no longer
// connected to the registry and metrics within) when returning from the the
// call. It creates new families as needed.
func (pm *PrometheusExporter) ScrapeRegistry(registry *Registry, includeChildMetrics bool) {
	labels := registry.getLabels()
	registry.Each(func(_ string, v interface{}) {
		prom, ok := v.(PrometheusExportable)
		if !ok {
			return
		}
		m := prom.ToPrometheusMetric()
		// Set registry and metric labels.
		m.Label = append(labels, prom.GetLabels()...)

		family := pm.findOrCreateFamily(prom)
		family.Metric = append(family.Metric, m)

		// Deal with metrics which have children which are exposed to
		// prometheus if we should.
		promIter, ok := v.(PrometheusIterable)
		if !ok || !includeChildMetrics {
			return
		}
		promIter.Each(m.Label, func(metric *prometheusgo.Metric) {
			family.Metric = append(family.Metric, metric)
		})
	})
}

// PrintAsText writes all metrics in the families map to the io.Writer in
// prometheus' text format. It removes individual metrics from the families
// as it goes, readying the families for another found of registry additions.
func (pm *PrometheusExporter) PrintAsText(w io.Writer) error {
	for _, family := range pm.families {
		if _, err := expfmt.MetricFamilyToText(w, family); err != nil {
			return err
		}
	}
	pm.clearMetrics()
	return nil
}

// Verify GraphiteExporter implements Gatherer interface.
var _ prometheus.Gatherer = (*PrometheusExporter)(nil)

// Gather implements prometheus.Gatherer
func (pm *PrometheusExporter) Gather() ([]*prometheusgo.MetricFamily, error) {
	v := make([]*prometheusgo.MetricFamily, len(pm.families))
	i := 0
	for _, family := range pm.families {
		v[i] = family
		i++
	}
	return v, nil
}

// Clear metrics for reuse.
func (pm *PrometheusExporter) clearMetrics() {
	for _, family := range pm.families {
		// Set to nil to avoid allocation if the family never gets any metrics.
		family.Metric = nil
	}
}
