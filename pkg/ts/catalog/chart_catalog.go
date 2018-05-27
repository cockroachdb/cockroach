package catalog

import (
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/pkg/errors"

	prometheusgo "github.com/prometheus/client_model/go"
)

// chartDescription describes an individual chart.
// Only Title, Organization, and Metrics must be set; other values have useful
// defaults if the included metrics are similar.
type chartDescription struct {
	// Title of the chart.
	Title string
	// Organization identifies the sections in which to place the chart.
	// (Sections are created ad hoc as ChartSections whenever they're identified in
	// a chartDescription.)
	// Inner array is Level 0 (req), Level 1 (req), Level 2 (opt).
	// Outer array lets you use the same chart in multiple places.
	Organization [][]string
	// Metrics to include in the chart using their util.metric.Metadata.name;
	// these values are used to generate ChartMetrics.
	// NOTE: All Metrics must be of the same prometheusgo.MetricType.
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
	prometheusgo.MetricType_COUNTER: chartDefault{
		Downsampler: DescribeAggregator_AVG,
		Aggregator:  DescribeAggregator_SUM,
		Rate:        DescribeDerivative_NON_NEGATIVE_DERIVATIVE,
		Percentiles: false,
	},
	prometheusgo.MetricType_GAUGE: chartDefault{
		Downsampler: DescribeAggregator_AVG,
		Aggregator:  DescribeAggregator_AVG,
		Rate:        DescribeDerivative_NONE,
		Percentiles: false,
	},
	prometheusgo.MetricType_HISTOGRAM: chartDefault{
		Downsampler: DescribeAggregator_AVG,
		Aggregator:  DescribeAggregator_AVG,
		Rate:        DescribeDerivative_NONE,
		Percentiles: true,
	},
}

// These consts represent the catalog's Level 0 organization options.
const (
	Process            = `Process`
	SQLLayer           = `SQL Layer`
	KVTransactionLayer = `KV Transaction Layer`
	DistributionLayer  = `Distribution Layer`
	ReplicationLayer   = `Replication Layer`
	StorageLayer       = `Storage Layer`
	Timeseries         = `Timeseries`
)

// charts defines all of the charts in the catalog.
// NOTE: This is an small section of the complete catalog, meant only to
// demonstrate how charts will be identified. The rest of this list will
// added in a following commit.
var charts = []chartDescription{
	{
		Title:        "Exec Latency",
		Organization: [][]string{{SQLLayer, "DistSQL"}},
		Metrics:      []string{"sql.distsql.exec.latency"},
	},
	{
		Title:        "DML Mix",
		Organization: [][]string{{SQLLayer, "DistSQL"}},
		Metrics:      []string{"sql.distsql.select.count"},
	},
	{
		Title:        "Service Latency",
		Organization: [][]string{{SQLLayer, "DistSQL"}},
		Metrics:      []string{"sql.distsql.service.latency"},
	},
	{
		Title:        "Durations",
		Organization: [][]string{{KVTransactionLayer, "Transactions"}},
		Metrics:      []string{"txn.durations"},
	},
	{
		Title:        "Epoch Increment Count",
		Organization: [][]string{{ReplicationLayer, "Node Liveness"}},
		Metrics:      []string{"liveness.epochincrements"},
	},
	{
		Title:        "Counts",
		Organization: [][]string{{KVTransactionLayer, "Storage"}},
		AxisLabel:    "MVCC Keys & Values",
		Metrics: []string{
			"intentcount",
			"keycount",
			"livecount",
			"syscount",
			"valcount",
		},
	},
	{
		Title:        "Counts",
		Organization: [][]string{{StorageLayer, "Storage", "KV"}},
		AxisLabel:    "MVCC Keys & Values",
		Metrics: []string{
			"intentcount",
			"keycount",
			"livecount",
			"syscount",
			"valcount",
		},
	},
	{
		Title:        "Node Cert Expiration",
		Organization: [][]string{{Process, "Certificates"}},
		Downsampler:  DescribeAggregator_MAX,
		Aggregator:   DescribeAggregator_MAX,
		Metrics:      []string{"security.certificate.expiration.node"},
	},
	{
		Title: "Snapshots",
		Organization: [][]string{
			{DistributionLayer, "Ranges"},
			{ReplicationLayer, "Ranges"},
		},
		Metrics: []string{
			"range.snapshots.generated",
			"range.snapshots.normal-applied",
			"range.snapshots.preemptive-applied",
		},
	},
	{
		Title:        "Index & Filter Block Size",
		Organization: [][]string{{StorageLayer, "RocksDB", "Overview"}},
		Metrics:      []string{"rocksdb.table-readers-mem-estimate"},
	},
	{
		Title:        "Restarts",
		Organization: [][]string{{KVTransactionLayer, "Transactions"}},
		Downsampler:  DescribeAggregator_MAX,
		Metrics:      []string{"txn.restarts"},
	},
	{
		Title:        "Restart Cause Mix",
		Organization: [][]string{{KVTransactionLayer, "Transactions"}},
		Metrics: []string{
			"txn.restarts.deleterange",
			"txn.restarts.possiblereplay",
			"txn.restarts.serializable",
			"txn.restarts.writetooold",
		},
	},
	{
		Title: "Time Spent",
		Organization: [][]string{
			{DistributionLayer, "Split Queue"},
			{ReplicationLayer, "Split Queue"},
		},
		Metrics: []string{"queue.split.processingnanos"},
	},
	{
		Title:        "Lease Transfer Count",
		Organization: [][]string{{ReplicationLayer, "Replicate Queue"}},
		Metrics:      []string{"queue.replicate.transferlease"},
	},
	{
		Title:        "Txn All",
		Organization: [][]string{{SQLLayer, "SQL Memory", "Connections"}},
		Metrics:      []string{"sql.mem.conns.txn.max"},
	},
	{
		Title:        "Uptime",
		Organization: [][]string{{Process, "Server", "Overview"}},
		Metrics:      []string{"sys.uptime"},
	},
	{
		Title:        "Size",
		Organization: [][]string{{Timeseries, "Overview"}},
		Metrics:      []string{"timeseries.write.bytes"},
	},
}

// chartCatalog represents the entire chart catalog, which is an array of
// ChartSections, to which the individual charts and subsections defined above
// are added.
var chartCatalog = []ChartSection{
	{
		Title:          Process,
		LongName:       Process,
		CollectionName: "process-all",
		Description: `These charts detail the overall performance of the 
		<code>cockroach</code> process running on this server.`,
		Level: 0,
	},
	{
		Title:          SQLLayer,
		LongName:       SQLLayer,
		CollectionName: "sql-layer-all",
		Description: `In the SQL layer, nodes receive commands and then parse, plan, 
		and execute them. <br/><br/><a class="catalog-link" 
		href="https://www.cockroachlabs.com/docs/stable/architecture/sql-layer.html">
		SQL Layer Architecture Docs >></a>"`,
		Level: 0,
	},
	{
		Title:          KVTransactionLayer,
		LongName:       KVTransactionLayer,
		CollectionName: "kv-transaction-layer-all",
		Description: `The KV Transaction Layer coordinates concurrent requests as 
		key-value operations. To maintain consistency, this is also where the cluster 
		manages time. <br/><br/><a class="catalog-link" 
		href="https://www.cockroachlabs.com/docs/stable/architecture/transaction-layer.html">
		Transaction Layer Architecture Docs >></a>`,
		Level: 0,
	},
	{
		Title:          DistributionLayer,
		LongName:       DistributionLayer,
		CollectionName: "distribution-layer-all",
		Description: `The Distribution Layer provides a unified view of your cluster’s data, 
		which are actually broken up into many key-value ranges. <br/><br/><a class="catalog-link" 
		href="https://www.cockroachlabs.com/docs/stable/architecture/distribution-layer.html"> 
		Distribution Layer Architecture Docs >></a>`,
		Level: 0,
	},
	{
		Title:          ReplicationLayer,
		LongName:       ReplicationLayer,
		CollectionName: "replication-layer-all",
		Description: `The Replication Layer maintains consistency between copies of ranges 
		(known as replicas) through our consensus algorithm, Raft. <br/><br/><a class="catalog-link" 
			href="https://www.cockroachlabs.com/docs/stable/architecture/replication-layer.html"> 
			Replication Layer Architecture Docs >></a>`,
		Level: 0,
	},
	{
		Title:          StorageLayer,
		LongName:       StorageLayer,
		CollectionName: "replication-layer-all",
		Description: `The Storage Layer reads and writes data to disk, as well as manages 
		garbage collection. <br/><br/><a class="catalog-link" 
		href="https://www.cockroachlabs.com/docs/stable/architecture/storage-layer.html">
		Storage Layer Architecture Docs >></a>`,
		Level: 0,
	},
	{
		Title:          Timeseries,
		LongName:       Timeseries,
		CollectionName: "timeseries-all",
		Description: `Your cluster collects data about its own performance, which is used to 
		power the very charts you\'re using, among other things.`,
		Level: 0,
	},
}

// catalogKey indexes the array position of the Level 0 ChartSections.
// This optimizes generating the chart catalog by limiting the search space every
// chart uses when being added to the catalog.
var catalogKey = map[string]int{
	Process:            0,
	SQLLayer:           1,
	KVTransactionLayer: 2,
	DistributionLayer:  3,
	ReplicationLayer:   4,
	StorageLayer:       5,
	Timeseries:         6,
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
// _admin/v1/chartcatalog.
func GenerateCatalog(metadata map[string]metric.Metadata) ([]ChartSection, error) {

	// Range over all chartDescriptions.
	for _, cd := range charts {

		// Range over each level of organization.
		for i, organization := range cd.Organization {

			// Ensure the organization has both Level 0 and Level 1 organization.
			if len(organization) < 2 {
				return nil, errors.Errorf(`%s must have at Level 0 and Level 1 organization,
				 only has %v`, cd.Title, organization)
			}

			// Make sure the organization's Level 0 is in the catalog.
			level0CatalogIndex, ok := catalogKey[organization[0]]

			if !ok {
				return nil, errors.Errorf("Invalid Level 0 for %s using %v", cd.Title, organization)
			}

			ic, err := createIndividualChart(metadata, cd, i)

			if err != nil {
				return nil, err
			}

			// If ic has no Metrics, skip. Note that this isn't necessarily an error
			// e.g. nodes without SSLs do not have certificate expiration timestamps,
			// so those charts should not be added to the catalog.
			if len(ic.Metrics) == 0 {
				continue
			}

			// Add the individual chart and its subsections to the chart catalog
			chartCatalog[level0CatalogIndex].addChartAndSubsections(organization, ic)
		}
	}

	return chartCatalog, nil
}

// createIndividualChart creates an individual chart, which will later be added
// to the approrpiate ChartSection.
// - metadata is used to find meaningful information about cd.Metrics' values.
// - cd describes the individual chart to create.
// - organizationIndex specifies which array from cd.Organization to use.
func createIndividualChart(
	metadata map[string]metric.Metadata,
	cd chartDescription,
	organizationIndex int) (IndividualChart, error) {

	var ic IndividualChart

	err := ic.addMetrics(cd, metadata)

	if err != nil {
		return ic, err
	}

	// If chart has no data, abandon. Note that this isn't necessarily an error
	// e.g. nodes without SSLs do not have certificate expiration timestamps,
	// so those charts should not be added to the catalog.
	if len(ic.Metrics) == 0 {
		return ic, nil
	}

	// Get first metric's MetricType, which will be used to determine its default values.
	mt := ic.Metrics[0].MetricType

	ic.addDisplayProperties(cd, mt)

	ic.addNames(cd, organizationIndex)

	return ic, nil
}

// addMetrics sets the IndividualChart's Metric values by looking up the chartDescription
// metrics in the metadata map.
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
func (ic *IndividualChart) addNames(cd chartDescription, organizationIndex int) {

	ic.Title = cd.Title

	// Find string delimeters that are not dashes, including spaces, slashes, and
	// commas.
	nondashDelimeters := regexp.MustCompile("( )|/|,")

	// Longnames look like "SQL Layer | SQL | Connections".
	// CollectionNames look like "sql-layer-sql-connections".
	for _, n := range cd.Organization[organizationIndex] {
		ic.Longname += n + " | "
		ic.CollectionName += nondashDelimeters.ReplaceAllString(strings.ToLower(n), "-") + "-"
	}

	ic.CollectionName += nondashDelimeters.ReplaceAllString(strings.ToLower(cd.Title), "-")
	ic.Longname += cd.Title

}

// addDisplayProperties sets the IndividualChart's display properties, such as
// its Downsampler and Aggregator.
func (ic *IndividualChart) addDisplayProperties(
	cd chartDescription, mt prometheusgo.MetricType,
) error {

	defaults := chartDefaultsPerMetricType[mt]

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
	if cdFull.Percentiles == false {
		cdFull.Percentiles = defaults.Percentiles
	}

	// Set unspecified AxisUnits to the first metric's value.
	if cdFull.Units == AxisUnits_UNSET_UNITS {

		pu := ic.Metrics[0].PreferredUnits
		for _, m := range ic.Metrics {
			if m.PreferredUnits != pu {
				return errors.Errorf(`Chart %s has metrics with different preferred 
				units; need to specify Units in its chartDescription`, cd.Title)
			}
		}

		cdFull.Units = pu
	}

	// Set unspecified AxisLabels to the first metric's value.
	if cdFull.AxisLabel == "" {
		al := ic.Metrics[0].AxisLabel

		for _, m := range ic.Metrics {
			if m.AxisLabel != al {
				return errors.Errorf(`Chart %s has metrics with different axis labels; 
				need to specify an AxisLabel in its chartDescription`, cd.Title)
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
func (cs *ChartSection) addChartAndSubsections(organization []string, ic IndividualChart) {

	// subsection is either an existing or new element of cs.Subsections that will either contain
	// more Subsections or the IndividualChart we want to add.
	var subsection *ChartSection

	// subsectionLevel identifies the level of the organization slice we're using; it will always
	// be one greater than its parent's level.
	subsectionLevel := int(cs.Level + 1)

	// To identify how to treat subsection, we need to search for a cs.Subsections element with
	// the same name as the current organization index. However, because ChartSection contains
	// a slice of pointers, it cannot be compared to an empty ChartSection, so use a bool to
	// to track if it's found.
	found := false

	for _, ss := range cs.Subsections {
		if ss.Title == organization[subsectionLevel] {
			found = true
			subsection = ss
			break
		}
	}

	// If not found, create a new ChartSection and append it as a subsection.
	if !found {

		// Find string delimeters that are not dashes, including spaces, slashes, and
		// commas.
		nondashDelimeters := regexp.MustCompile("( )|/|,")

		subsection = &ChartSection{
			Title: organization[subsectionLevel],
			// Longnames look like "SQL Layer | SQL".
			LongName: "All",
			// CollectionNames look like "sql-layer-sql".
			CollectionName: nondashDelimeters.ReplaceAllString(strings.ToLower(organization[0]), "-"),
			Level:          int32(subsectionLevel),
		}

		// Complete Longname and Colectionname values.
		for i := 1; i <= subsectionLevel; i++ {
			subsection.LongName += " " + organization[i]
			subsection.CollectionName += "-" + nondashDelimeters.ReplaceAllString(strings.ToLower(organization[i]), "-")
		}

		cs.Subsections = append(cs.Subsections, subsection)
	}

	// If this is the last level of the organization, add the IndividualChart here. Otheriwse, recurse.
	if subsectionLevel == (len(organization) - 1) {
		subsection.Charts = append(subsection.Charts, &ic)
	} else {
		subsection.addChartAndSubsections(organization, ic)
	}
}
