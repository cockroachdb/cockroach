package future

type Metric struct {
	name        string // 'sys.rss', 'sql.select.count', etc.
	aggregation string // 'SUM'
	derivative  string // 'NON_NEGATIVE_DERIVATIVE'
	label       string
	downsampler string
}

type DashboardGraph struct {
	// Only one of the next 3 is populated.
	// For "metrics", we aggregate every metric over all nodes, or show from one node
	// For "perNode", we show the same metric for each node, or show from one node
	// For "perStore", we show the same metric for each store, or show from one node
	metrics      []Metric // nil for per-node/store metrics
	perNode      Metric   // `sys.rss`
	perStoreName Metric   // `etc.`

	units      string // 'BYTES'
	unitsLabel string // 'memory usage'

	title   string
	tooltip string // static tooltip text shown above the list of metrics in this chart
}

var DASHBOARDS = map[string][]DashboardGraph{
	"overview": {
		{
			title:      "SQL Queries Per Second",
			tooltip:    "A moving average of the number of SELECT, INSERT, UPDATE, and DELETE statements, and the sum of all four, successfully executed per second across all nodes.",
			units:      "COUNT",
			unitsLabel: "queries per second",
			metrics: []Metric{
				{"sql.select.count", "SUM", "NON_NEGATIVE_DERIVATIVE", "Selects", ""},
				{"sql.update.count", "SUM", "NON_NEGATIVE_DERIVATIVE", "Updates", ""},
				{"sql.insert.count", "SUM", "NON_NEGATIVE_DERIVATIVE", "Inserts", ""},
				{"sql.delete.count", "SUM", "NON_NEGATIVE_DERIVATIVE", "Deletes", ""},
				{"sql.crud_query.count", "SUM", "NON_NEGATIVE_DERIVATIVE", "Total Queries", ""},
			},
		},
		{
			title:      "Service Latency: SQL Statements, 99th percentile",
			tooltip:    "Over the last minute, this node executed 99% of SQL statements within this time. This time only includes SELECT, INSERT, UPDATE and DELETE statements and does not include network latency between the node and client.",
			units:      "DURATION",
			unitsLabel: "latency",
			perNode: Metric{
				name:        "sql.service.latency-p99",
				downsampler: "MAX",
				// Label omitted since per-node labels will be used
				// No aggregation, since per-node series will be shown
				// No derivative, since captured value can be shown directly
			},
		},
	},
}
