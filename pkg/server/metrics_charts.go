package server

import (
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/ts/catalog"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// TODO (koorosh): define proper structs names below.
type dashboard struct {
	title  string
	charts []chart
}

type chart struct {
	catalog.ChartDescription
	// tooltip is a text (that can contain HTML markup attributes) of tooltip rendered when user hovers the chart.
	// Tooltip string can include following placeholders that are replaced on frontend right before chart is rendered:
	// - {{NODES_CLUSTER_SELECTION}} placeholder is used in tooltip to alter text to specify that metric reference
	// either to selected node or entire cluster. Currently, either "across all nodes" or "on node X".
	// TODO (koorosh): tooltips for charts can contain either plain text or HTML template with dynamically
	// introduced variables, it makes difficult to define tooltips properly here as part of configuration.
	tooltip string
	// metricPerNode indicates that chart will render every metric for every node.
	metricPerNode bool
}

var overviewCharts = []chart{
	{
		ChartDescription: catalog.ChartDescription{
			Title: "SQL Statements",
			Metrics: []string{
				"cr.node.sql.select.count",
				"cr.node.sql.update.count",
				"cr.node.sql.insert.count",
				"cr.node.sql.delete.count",
			},
			AxisLabel: "queries",
			Rate:      catalog.DescribeDerivative_NON_NEGATIVE_DERIVATIVE,
		},
		tooltip: "A ten-second moving average of the # of SELECT, INSERT, UPDATE, and DELETE statements successfully " +
			"executed per second {{NODES_CLUSTER_SELECTION}}",
	},
	{
		ChartDescription: catalog.ChartDescription{
			Title: "Service Latency: SQL Statements, 99th percentile",
			Metrics: []string{
				"cr.node.sql.service.latency-p99",
			},
			Units:       catalog.AxisUnits_DURATION,
			AxisLabel:   "latency",
			Downsampler: catalog.DescribeAggregator_MAX,
		},
		metricPerNode: true,
		tooltip: `
			<div>
				Over the last minute, this node executed 99% of SQL statements within
				this time.&nbsp;
				<em>
					This time only includes SELECT, INSERT, UPDATE and DELETE statements
					and does not include network latency between the node and client.
				</em>
			</div>`,
	},
}

var distributedCharts = []chart{
	{
		ChartDescription: catalog.ChartDescription{
			Title: "Batches",
			Metrics: []string{
				"cr.node.distsender.batches",
				"cr.node.distsender.batches.partial",
			},
			AxisLabel: "batches",
			Rate:      catalog.DescribeDerivative_NON_NEGATIVE_DERIVATIVE,
		},
	},
	{
		ChartDescription: catalog.ChartDescription{
			Title: "KV Transaction Durations: 99th percentile",
			Metrics: []string{
				"cr.node.txn.durations-p99",
			},
			Units:       catalog.AxisUnits_DURATION,
			AxisLabel:   "transaction duration",
			Downsampler: catalog.DescribeAggregator_MAX,
		},
		tooltip: "The 99th percentile of transaction durations over a 1 minute period." +
			" Values are displayed individually for each node.",
		metricPerNode: true,
	},
}

var metricDashboards = []dashboard{
	{
		title:  "Overview",
		charts: overviewCharts,
	},
	{
		title:  "Distributed",
		charts: distributedCharts,
	},
}

// GenerateDashboards generates metric dashboards based on defined configuration (metricDashboards) that allows
// to render charts in Db Console on Metrics page.
func GenerateDashboards(metadata map[string]metric.Metadata) ([]*serverpb.MetricsDashboardsResponse_Dashboard, error) {
	dashboards := make([]*serverpb.MetricsDashboardsResponse_Dashboard, len(metricDashboards))
	for _, md := range metricDashboards {
		d := &serverpb.MetricsDashboardsResponse_Dashboard{
			Title: md.title,
		}
		for _, c := range md.charts {
			ic := new(catalog.IndividualChart)
			if err := ic.AddMetrics(c.ChartDescription, metadata, nil); err != nil {
				return nil, err
			}
			d.Charts = append(d.Charts, &serverpb.MetricsDashboardsResponse_Chart{
				ChartDescription: ic,
				Metadata: &serverpb.MetricsDashboardsResponse_Chart_Metadata{
					Tooltip:       c.tooltip,
					MetricPerNode: c.metricPerNode,
				},
			})
		}
	}
	return dashboards, nil
}
