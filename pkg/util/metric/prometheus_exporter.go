// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package metric

import (
	"io"

	"github.com/gogo/protobuf/proto"
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
func (pm *PrometheusExporter) ScrapeRegistry(registry *Registry) {
	labels := registry.getLabels()
	for _, metric := range registry.tracked {
		metric.Inspect(func(v interface{}) {
			if prom, ok := v.(PrometheusExportable); ok {
				m := prom.ToPrometheusMetric()
				// Set registry and metric labels.
				m.Label = append(labels, prom.GetLabels()...)

				family := pm.findOrCreateFamily(prom)
				family.Metric = append(family.Metric, m)
			}
		})
	}
}

// PrintAsText writes all metrics in the families map to the io.Writer in
// prometheus' text format. It removes individual metrics from the families
// as it goes, readying the families for another found of registry additions.
func (pm *PrometheusExporter) PrintAsText(w io.Writer) error {
	for _, family := range pm.families {
		if _, err := expfmt.MetricFamilyToText(w, family); err != nil {
			return err
		}
		// Clear metrics for reuse.
		family.Metric = []*prometheusgo.Metric{}
	}
	return nil
}
