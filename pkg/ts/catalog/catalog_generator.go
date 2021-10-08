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
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	prometheusgo "github.com/prometheus/client_model/go"
)

// catalog_generator.go generates a protobuf describing a set of pre-defined
// Admin UI charts that can aid users in debugging a CockroachDB cluster. This
// file generates the catalog at <Admin UI host>/_admin/v1/chartcatalog. The
// source describing the catalog to be generated is located in pkg/ts/catalog/chart_catalog.go.
// The page that leverages this structure will be added in a subsequent PR.

// The protobuf, viewed as a catalog, is organized into a hierarchy:
// 1. Top level: This level represents the "layers" of CockroachDB's architecture
//		(for reference https://www.cockroachlabs.com/docs/stable/architecture/overview.html),
// 2. Section: A grouping of similar charts or subsections.
// 3. Subsections: The most granular level of organization in the hierarchy.

// Each section and subsection can contains individual charts; users will be able
// to view all charts in a section or subsection.

// These consts represent the top levels of the hierarchy:
const (
	Process            = `Process`
	SQLLayer           = `SQL Layer`
	KVTransactionLayer = `KV Transaction Layer`
	DistributionLayer  = `Distribution Layer`
	ReplicationLayer   = `Replication Layer`
	StorageLayer       = `Storage Layer`
	Timeseries         = `Timeseries`
	Jobs               = `Jobs`
)

// sectionDescription describes either a section or subsection of the chart.
// During processing, these are converted in ChartSections (pkg/ts/catalog/chart_catalog.proto).
type sectionDescription struct {
	// Organization identifies where in the hierarchy to insert these charts.
	// The inner array describes where to insert these charts, and the outer
	// array lets you use the same charts in multiple places in the hierarchy
	// without needing to redefine them.
	Organization [][]string
	// Charts describes the specifics of the charts you want to add to the
	// section. At render time, users can choose to view individual charts
	// or all charts at a given level of the hierarchy/organization.
	Charts []chartDescription
}

// chartDescription describes an individual chart.
// Only Title, Organization, and Metrics must be set; other values have useful
// defaults based on the types of metrics.
// During processing, these are converted in IndividualCharts (pkg/ts/catalog/chart_catalog.proto).
type chartDescription struct {
	// Title of the chart.
	Title string
	// Metrics to include in the chart using their util.metric.Metadata.name;
	// these values are used to generate ChartMetrics.
	// NOTE: All Metrics in a chart must be of the same prometheusgo.MetricType.
	Metrics []string
	// Units in which the chart is viewed, e.g. BYTES for storage.
	// Does not need to be set if all Metrics have the same Unit value.
	Units AxisUnits
	// Axis label for the chart's y-axis.
	// Does not need to be set if all Metrics have the same Measurement value.
	AxisLabel string
	// The downsampler function the chart uses.
	Downsampler DescribeAggregator
	// The aggregator function for the chart's downsampled values.
	Aggregator DescribeAggregator
	// The derivative function the chart uses (e.g. NONE).
	Rate DescribeDerivative
	// Whether or not the chart should be converted into percentiles.
	// True only for Histograms. Unsupported by other metric types.
	Percentiles bool
}

// chartDefault provides default values to simplify adding charts to the catalog.
type chartDefault struct {
	Downsampler DescribeAggregator
	Aggregator  DescribeAggregator
	Rate        DescribeDerivative
	Percentiles bool
}

// chartDefaultsPerMetricType defines default values for the chart's
// Downsampler, Aggregator, Rate, and Percentiles based on the metric's type.
var chartDefaultsPerMetricType = map[prometheusgo.MetricType]chartDefault{
	prometheusgo.MetricType_COUNTER: {
		Downsampler: DescribeAggregator_AVG,
		Aggregator:  DescribeAggregator_SUM,
		Rate:        DescribeDerivative_NON_NEGATIVE_DERIVATIVE,
		Percentiles: false,
	},
	prometheusgo.MetricType_GAUGE: {
		Downsampler: DescribeAggregator_AVG,
		Aggregator:  DescribeAggregator_AVG,
		Rate:        DescribeDerivative_NONE,
		Percentiles: false,
	},
	prometheusgo.MetricType_HISTOGRAM: {
		Downsampler: DescribeAggregator_AVG,
		Aggregator:  DescribeAggregator_AVG,
		Rate:        DescribeDerivative_NONE,
		Percentiles: true,
	},
}

// chartCatalog represents the entire chart catalog, which is an array of
// ChartSections, to which the individual charts and subsections defined above
// are added.
var chartCatalog = []ChartSection{
	{
		Title:           Process,
		LongTitle:       Process,
		CollectionTitle: "process-all",
		Description: `These charts detail the overall performance of the 
		<code>cockroach</code> process running on this server.`,
		Level: 0,
	},
	{
		Title:           SQLLayer,
		LongTitle:       SQLLayer,
		CollectionTitle: "sql-layer-all",
		Description: `In the SQL layer, nodes receive commands and then parse, plan, 
		and execute them. <br/><br/><a class="catalog-link" 
		href="https://www.cockroachlabs.com/docs/stable/architecture/sql-layer.html">
		SQL Layer Architecture Docs >></a>"`,
		Level: 0,
	},
	{
		Title:           KVTransactionLayer,
		LongTitle:       KVTransactionLayer,
		CollectionTitle: "kv-transaction-layer-all",
		Description: `The KV Transaction Layer coordinates concurrent requests as 
		key-value operations. To maintain consistency, this is also where the cluster 
		manages time. <br/><br/><a class="catalog-link" 
		href="https://www.cockroachlabs.com/docs/stable/architecture/transaction-layer.html">
		Transaction Layer Architecture Docs >></a>`,
		Level: 0,
	},
	{
		Title:           DistributionLayer,
		LongTitle:       DistributionLayer,
		CollectionTitle: "distribution-layer-all",
		Description: `The Distribution Layer provides a unified view of your cluster’s data, 
		which are actually broken up into many key-value ranges. <br/><br/><a class="catalog-link" 
		href="https://www.cockroachlabs.com/docs/stable/architecture/distribution-layer.html"> 
		Distribution Layer Architecture Docs >></a>`,
		Level: 0,
	},
	{
		Title:           ReplicationLayer,
		LongTitle:       ReplicationLayer,
		CollectionTitle: "replication-layer-all",
		Description: `The Replication Layer maintains consistency between copies of ranges 
		(known as replicas) through our consensus algorithm, Raft. <br/><br/><a class="catalog-link" 
			href="https://www.cockroachlabs.com/docs/stable/architecture/replication-layer.html"> 
			Replication Layer Architecture Docs >></a>`,
		Level: 0,
	},
	{
		Title:           StorageLayer,
		LongTitle:       StorageLayer,
		CollectionTitle: "replication-layer-all",
		Description: `The Storage Layer reads and writes data to disk, as well as manages 
		garbage collection. <br/><br/><a class="catalog-link" 
		href="https://www.cockroachlabs.com/docs/stable/architecture/storage-layer.html">
		Storage Layer Architecture Docs >></a>`,
		Level: 0,
	},
	{
		Title:           Timeseries,
		LongTitle:       Timeseries,
		CollectionTitle: "timeseries-all",
		Description: `Your cluster collects data about its own performance, which is used to 
		power the very charts you\'re using, among other things.`,
		Level: 0,
	},
	{
		Title:           Jobs,
		LongTitle:       Jobs,
		CollectionTitle: "jobs-all",
		Description:     `Your cluster executes various background jobs, as well as scheduled jobs`,
		Level:           0,
	},
}

var catalogGenerated = false

// catalogKey provides an index to simplify ordering ChartSections, as well as
// limiting the search space every chart uses when being added to the catalog.
var catalogKey = map[string]int{
	Process:            0,
	SQLLayer:           1,
	KVTransactionLayer: 2,
	DistributionLayer:  3,
	ReplicationLayer:   4,
	StorageLayer:       5,
	Timeseries:         6,
	Jobs:               7,
}

// unitsKey converts between metric.Unit and catalog.AxisUnits which is
// necessary because charts only support a subset of unit types.
var unitsKey = map[metric.Unit]AxisUnits{
	metric.Unit_BYTES:         AxisUnits_BYTES,
	metric.Unit_CONST:         AxisUnits_COUNT,
	metric.Unit_COUNT:         AxisUnits_COUNT,
	metric.Unit_NANOSECONDS:   AxisUnits_DURATION,
	metric.Unit_PERCENT:       AxisUnits_COUNT,
	metric.Unit_SECONDS:       AxisUnits_DURATION,
	metric.Unit_TIMESTAMP_NS:  AxisUnits_DURATION,
	metric.Unit_TIMESTAMP_SEC: AxisUnits_DURATION,
}

// aggKey converts between catalog.DescribeAggregator to
// tspb.TimeSeriesQueryAggregator which is necessary because
// tspb.TimeSeriesQueryAggregator doesn't have a checkable zero value, which the
// catalog requires to support defaults (DescribeAggregator_UNSET_AGG).
var aggKey = map[DescribeAggregator]tspb.TimeSeriesQueryAggregator{
	DescribeAggregator_AVG: tspb.TimeSeriesQueryAggregator_AVG,
	DescribeAggregator_MAX: tspb.TimeSeriesQueryAggregator_MAX,
	DescribeAggregator_MIN: tspb.TimeSeriesQueryAggregator_MIN,
	DescribeAggregator_SUM: tspb.TimeSeriesQueryAggregator_SUM,
}

// derKey converts between catalog.DescribeDerivative to
// tspb.TimeSeriesQueryDerivative which is necessary because
// tspb.TimeSeriesQueryDerivative doesn't have a checkable zero value, which the
// catalog requires to support defaults (DescribeDerivative_UNSET_DER).
var derKey = map[DescribeDerivative]tspb.TimeSeriesQueryDerivative{
	DescribeDerivative_DERIVATIVE:              tspb.TimeSeriesQueryDerivative_DERIVATIVE,
	DescribeDerivative_NONE:                    tspb.TimeSeriesQueryDerivative_NONE,
	DescribeDerivative_NON_NEGATIVE_DERIVATIVE: tspb.TimeSeriesQueryDerivative_NON_NEGATIVE_DERIVATIVE,
}

// GenerateCatalog creates an array of ChartSections, which is served at
// /_admin/v1/chartcatalog.
func GenerateCatalog(metadata map[string]metric.Metadata) ([]ChartSection, error) {

	if !catalogGenerated {
		for _, sd := range charts {

			if err := createIndividualCharts(metadata, sd); err != nil {
				return nil, err
			}
		}
		catalogGenerated = true
	}

	return chartCatalog, nil
}

// createIndividualChart creates IndividualCharts, and ultimately places them
// in the appropriate place in chartCatalog based on the hierarchy described
// in sd.Organization.
func createIndividualCharts(metadata map[string]metric.Metadata, sd sectionDescription) error {

	var ics []*IndividualChart

	for _, cd := range sd.Charts {

		ic := new(IndividualChart)

		if err := ic.addMetrics(cd, metadata); err != nil {
			return err
		}

		// If ic has no Metrics, skip. Note that this isn't necessarily an error
		// e.g. nodes without SSLs do not have certificate expiration timestamps,
		// so those charts should not be added to the catalog.
		if len(ic.Metrics) == 0 {
			continue
		}

		if err := ic.addDisplayProperties(cd); err != nil {
			return err
		}

		ic.Title = cd.Title

		ics = append(ics, ic)
	}

	for _, org := range sd.Organization {
		// Ensure the organization has both Level 0 and Level 1 organization.
		if len(org) < 2 {
			return errors.Errorf(`Sections must have at least Level 0 and 
				Level 1 organization, but only have %v in %v`, org, sd)
		} else if len(org) > 3 {
			return errors.Errorf(`Sections cannot be more than 3 levels deep,
				but %v has %d`, sd, len(org))
		}

		for _, ic := range ics {
			ic.addNames(org)
		}

		// Make sure the organization's top level is valid.
		topLevelCatalogIndex, ok := catalogKey[org[0]]

		if !ok {
			return errors.Errorf(`Undefined Level 0 organization; you must 
			use a const defined in pkg/ts/catalog/catalog_generator.go for %v`, sd)
		}

		chartCatalog[topLevelCatalogIndex].addChartAndSubsections(org, ics)
	}

	return nil
}

// addMetrics sets the IndividualChart's Metric values by looking up the
// chartDescription metrics in the metadata map.
func (ic *IndividualChart) addMetrics(
	cd chartDescription, metadata map[string]metric.Metadata,
) error {
	for _, x := range cd.Metrics {

		md, ok := metadata[x]

		// If metric is missing from metadata, don't add it to this chart e.g.
		// insecure nodes do not metadata related to SSL certificates, so those metrics
		// should not be added to any charts.
		if !ok {
			continue
		}

		unit, ok := unitsKey[md.Unit]

		if !ok {
			return errors.Errorf(
				"%s's metric.Metadata has an unrecognized Unit, %v", md.Name, md.Unit,
			)
		}

		ic.Metrics = append(ic.Metrics, ChartMetric{
			Name:           md.Name,
			Help:           md.Help,
			AxisLabel:      md.Measurement,
			PreferredUnits: unit,
			MetricType:     md.MetricType,
		})

		if ic.Metrics[0].MetricType != md.MetricType {
			return errors.Errorf(`%s and %s have different MetricTypes, but are being 
			added to the same chart, %v`, ic.Metrics[0].Name, md.Name, ic)
		}
	}

	return nil
}

// addNames sets the IndividualChart's Title, Longname, and CollectionName.
func (ic *IndividualChart) addNames(organization []string) {

	// Find string delimiters that are not dashes, including spaces, slashes, and
	// commas.
	nondashDelimiters := regexp.MustCompile("( )|/|,")

	// LongTitles look like "SQL Layer | SQL | Connections".
	// CollectionTitless look like "sql-layer-sql-connections".
	for _, n := range organization {
		ic.LongTitle += n + string(" | ")
		ic.CollectionTitle += nondashDelimiters.ReplaceAllString(strings.ToLower(n), "-") + "-"
	}

	ic.LongTitle += ic.Title
	ic.CollectionTitle += nondashDelimiters.ReplaceAllString(strings.ToLower(ic.Title), "-")

}

// addDisplayProperties sets the IndividualChart's display properties, such as
// its Downsampler and Aggregator.
func (ic *IndividualChart) addDisplayProperties(cd chartDescription) error {
	// All metrics in a chart must have the same MetricType, so each
	// IndividualChart has only one potential set of default values.
	defaults := chartDefaultsPerMetricType[ic.Metrics[0].MetricType]

	// Create copy of cd to avoid argument mutation.
	cdFull := cd

	// Set all zero values to the chartDefault's value
	if cdFull.Downsampler == DescribeAggregator_UNSET_AGG {
		cdFull.Downsampler = defaults.Downsampler
	}
	if cdFull.Aggregator == DescribeAggregator_UNSET_AGG {
		cdFull.Aggregator = defaults.Aggregator
	}
	if cdFull.Rate == DescribeDerivative_UNSET_DER {
		cdFull.Rate = defaults.Rate
	}
	if !cdFull.Percentiles {
		cdFull.Percentiles = defaults.Percentiles
	}

	// Set unspecified AxisUnits to the first metric's value.
	if cdFull.Units == AxisUnits_UNSET_UNITS {

		pu := ic.Metrics[0].PreferredUnits
		for _, m := range ic.Metrics {
			if m.PreferredUnits != pu {
				return errors.Errorf(`Chart %s has metrics with different preferred 
				units; need to specify Units in its chartDescription: %v`, cd.Title, ic)
			}
		}

		cdFull.Units = pu
	}

	// Set unspecified AxisLabels to the first metric's value.
	if cdFull.AxisLabel == "" {
		al := ic.Metrics[0].AxisLabel

		for _, m := range ic.Metrics {
			if m.AxisLabel != al {
				return errors.Errorf(`Chart %s has metrics with different axis labels (%s vs %s); 
				need to specify an AxisLabel in its chartDescription: %v`, al, m.AxisLabel, cd.Title, ic)
			}
		}

		cdFull.AxisLabel = al
	}

	// Populate the IndividualChart values.
	ds, ok := aggKey[cdFull.Downsampler]
	if !ok {
		return errors.Errorf(
			"%s's chartDescription has an unrecognized Downsampler, %v", cdFull.Title, cdFull.Downsampler,
		)
	}
	ic.Downsampler = &ds

	agg, ok := aggKey[cdFull.Aggregator]
	if !ok {
		return errors.Errorf(
			"%s's chartDescription has an unrecognized Aggregator, %v", cdFull.Title, cdFull.Aggregator,
		)
	}
	ic.Aggregator = &agg

	der, ok := derKey[cdFull.Rate]
	if !ok {
		return errors.Errorf(
			"%s's chartDescription has an unrecognized Rate, %v", cdFull.Title, cdFull.Rate,
		)
	}
	ic.Derivative = &der

	ic.Percentiles = cdFull.Percentiles
	ic.Units = cdFull.Units
	ic.AxisLabel = cdFull.AxisLabel

	return nil
}

// addChartAndSubsections adds subsections identified in the organization to the calling
// ChartSection until it reaches the last level of organization, where it adds the chart
// to the last subsection.
func (cs *ChartSection) addChartAndSubsections(organization []string, ics []*IndividualChart) {

	// subsection is either an existing or new element of cs.Subsections that will either contain
	// more Subsections or the IndividualChart we want to add.
	var subsection *ChartSection

	// subsectionLevel identifies the level of the organization slice we're using; it will always
	// be one greater than its parent's level.
	subsectionLevel := int(cs.Level + 1)

	// To identify how to treat subsection, we need to search for a cs.Subsections element with
	// the same name as the current organization index.

	for _, ss := range cs.Subsections {
		if ss.Title == organization[subsectionLevel] {
			subsection = ss
			break
		}
	}

	// If not found, create a new ChartSection and append it as a subsection.
	if subsection == nil {

		// Find string delimiters that are not dashes, including spaces, slashes, and
		// commas.
		nondashDelimiters := regexp.MustCompile("( )|/|,")

		subsection = &ChartSection{
			Title: organization[subsectionLevel],
			// LongTitles look like "SQL Layer | SQL".
			LongTitle: "All",
			// CollectionTitles look like "sql-layer-sql".
			CollectionTitle: nondashDelimiters.ReplaceAllString(strings.ToLower(organization[0]), "-"),
			Level:           int32(subsectionLevel),
		}

		// Complete LongTitle and CollectionTitle values.
		for i := 1; i <= subsectionLevel; i++ {
			subsection.LongTitle += " " + organization[i]
			subsection.CollectionTitle += "-" + nondashDelimiters.ReplaceAllString(strings.ToLower(organization[i]), "-")
		}

		cs.Subsections = append(cs.Subsections, subsection)
	}

	// If this is the last level of the organization, add the IndividualChart here. Otherwise, recurse.
	if subsectionLevel == (len(organization) - 1) {
		subsection.Charts = append(subsection.Charts, ics...)
	} else {
		subsection.addChartAndSubsections(organization, ics)
	}
}
