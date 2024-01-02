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

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// PrometheusExporter contains a map of metric families (a metric with multiple labels).
// It initializes each metric family once and reuses it for each prometheus scrape.
// Using ScrapeAndPrintAsText is a thread safe way to scrape and export the
// metrics under lock.
// The exporter can be configured to only export selected metrics by using
// MakePrometheusExporterForSelectedMetrics. The default is to export all metrics.
// TODO(marc): we should really keep our metric objects here so we can avoid creating
// new prometheus.Metric every time we are scraped.
// see: https://github.com/cockroachdb/cockroach/issues/9326
//
//	pe := MakePrometheusExporter()
//	pe.AddMetricsFromRegistry(nodeRegistry)
//	pe.AddMetricsFromRegistry(storeOneRegistry)
//	...
//	pe.AddMetricsFromRegistry(storeNRegistry)
//	pe.Export(w)
type PrometheusExporter struct {
	muScrapeAndPrint syncutil.Mutex
	families         map[string]*prometheusgo.MetricFamily
	selection        map[string]struct{}
}

// MakePrometheusExporter returns an initialized prometheus exporter.
func MakePrometheusExporter() PrometheusExporter {
	return PrometheusExporter{families: map[string]*prometheusgo.MetricFamily{}}
}

// MakePrometheusExporterForSelectedMetrics returns an initialized prometheus
// exporter. It would only consider selected metrics when scraping. The caller
// should not modify the map after the call.
func MakePrometheusExporterForSelectedMetrics(selection map[string]struct{}) PrometheusExporter {
	return PrometheusExporter{families: map[string]*prometheusgo.MetricFamily{}, selection: selection}
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
	labels := registry.GetLabels()
	f := func(name string, v interface{}) {
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
	}
	if pm.selection == nil {
		registry.Each(f)
	} else {
		registry.Select(pm.selection, f)
	}
}

// printAsText writes all metrics in the families map to the io.Writer in
// prometheus' text format. It removes individual metrics from the families
// as it goes, readying the families for another found of registry additions.
func (pm *PrometheusExporter) printAsText(w io.Writer, contentType expfmt.Format) error {
	enc := expfmt.NewEncoder(w, contentType)
	for _, family := range pm.families {
		if err := enc.Encode(family); err != nil {
			return err
		}
	}
	pm.clearMetrics()
	return nil
}

// ScrapeAndPrintAsText scrapes metrics first by calling the provided scrape func
// and then writes all metrics in the families map to the io.Writer in
// prometheus' text format. It removes individual metrics from the families
// as it goes, readying the families for another found of registry additions.
// It does this under lock so it is thread safe and can be called concurrently.
func (pm *PrometheusExporter) ScrapeAndPrintAsText(
	w io.Writer, contentType expfmt.Format, scrapeFunc func(*PrometheusExporter),
) error {
	pm.muScrapeAndPrint.Lock()
	defer pm.muScrapeAndPrint.Unlock()
	scrapeFunc(pm)
	return pm.printAsText(w, contentType)
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
