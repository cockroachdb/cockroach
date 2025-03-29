// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metrics

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// basicMetricsAPI is a simplified implementation for testing
type basicMetricsAPI struct {
	debugMode      bool
	lastQuery      string
	lastQueryValue float64
}

// basicMetricQuery is a simplified implementation for testing
type basicMetricQuery struct {
	api         *basicMetricsAPI
	metricName  string
	labels      map[string]string
	timeRange   string
	isRateQuery bool
	aggregation string
}

func (b *basicMetricsAPI) EnableDebugMode(enabled bool) {
	b.debugMode = enabled
}

func (b *basicMetricsAPI) GetLastQuery() string {
	return b.lastQuery
}

func (b *basicMetricsAPI) GetLastQueryResult() float64 {
	return b.lastQueryValue
}

func (b *basicMetricsAPI) AssertThat(metricName string) MetricQuery {
	return &basicMetricQuery{
		api:        b,
		metricName: metricName,
		labels:     make(map[string]string),
	}
}

func (b *basicMetricsAPI) Close() error {
	return nil
}

func (b *basicMetricsAPI) CompareValuesOverTime(
	metricName string,
	startTime, endTime time.Time,
) MetricComparison {
	// For testing, just store the query
	b.lastQuery = metricName
	b.lastQueryValue = 1.0
	return &basicMetricComparison{api: b}
}

func (q *basicMetricQuery) ForService(serviceName string) MetricQuery {
	q.labels["service"] = serviceName
	return q
}

func (q *basicMetricQuery) ForNode(nodeID string) MetricQuery {
	q.labels["node"] = nodeID
	return q
}

func (q *basicMetricQuery) WithLabel(name, value string) MetricQuery {
	q.labels[name] = value
	return q
}

func (q *basicMetricQuery) OverLast(duration string) MetricQuery {
	q.timeRange = duration
	return q
}

func (q *basicMetricQuery) Between(start, end time.Time) MetricQuery {
	// No-op for testing
	return q
}

func (q *basicMetricQuery) Sum() MetricQuery {
	q.aggregation = "sum"
	return q
}

func (q *basicMetricQuery) Avg() MetricQuery {
	q.aggregation = "avg"
	return q
}

func (q *basicMetricQuery) Max() MetricQuery {
	q.aggregation = "max"
	return q
}

func (q *basicMetricQuery) Rate() MetricQuery {
	q.isRateQuery = true
	return q
}

func (q *basicMetricQuery) GetBuiltQuery() string {
	return q.buildQuery()
}

func (q *basicMetricQuery) buildQuery() string {
	query := q.metricName

	// Add label filters
	if len(q.labels) > 0 {
		query += "{"
		first := true
		for k, v := range q.labels {
			if !first {
				query += ","
			}
			query += k + "=\"" + v + "\""
			first = false
		}
		query += "}"
	}

	// Add rate function if this is a rate query
	if q.isRateQuery {
		timeRange := q.timeRange
		if timeRange == "" {
			timeRange = "5m"
		}
		query = "rate(" + query + "[" + timeRange + "])"
	}

	// Add aggregation if specified
	if q.aggregation != "" {
		query = q.aggregation + "(" + query + ")"
	}

	// Store the query for debugging
	q.api.lastQuery = query
	q.api.lastQueryValue = 1.0 // Mock value

	return query
}

func (q *basicMetricQuery) HasValueAtLeast(threshold float64) error {
	query := q.buildQuery()
	q.api.lastQuery = query
	q.api.lastQueryValue = 1.0
	return nil
}

func (q *basicMetricQuery) HasValueAtMost(threshold float64) error {
	query := q.buildQuery()
	q.api.lastQuery = query
	q.api.lastQueryValue = 1.0
	return nil
}

func (q *basicMetricQuery) HasRateAtLeast(threshold float64) error {
	// Ensure this is a rate query
	rateQuery := &basicMetricQuery{}
	*rateQuery = *q
	rateQuery.isRateQuery = true

	query := rateQuery.buildQuery()
	q.api.lastQuery = query
	q.api.lastQueryValue = 1.0
	return nil
}

func (q *basicMetricQuery) HasRateAtMost(threshold float64) error {
	// Ensure this is a rate query
	rateQuery := &basicMetricQuery{}
	*rateQuery = *q
	rateQuery.isRateQuery = true

	query := rateQuery.buildQuery()
	q.api.lastQuery = query
	q.api.lastQueryValue = 1.0
	return nil
}

func (q *basicMetricQuery) HasPercentile(percentile int, threshold float64) error {
	percentileQuery := "histogram_quantile(0." + fmt.Sprintf("%d", percentile) + ", sum by(le) (" + q.metricName + "_bucket))"
	q.api.lastQuery = percentileQuery
	q.api.lastQueryValue = 1.0
	return nil
}

func (q *basicMetricQuery) Value() (float64, error) {
	query := q.buildQuery()
	q.api.lastQuery = query
	q.api.lastQueryValue = 1.0
	return 1.0, nil
}

// basicMetricComparison is a simplified implementation for testing
type basicMetricComparison struct {
	api *basicMetricsAPI
}

func (c *basicMetricComparison) HasIncreasedByLessThan(percentage float64) error {
	return nil
}

func (c *basicMetricComparison) HasIncreasedByAtLeast(percentage float64) error {
	return nil
}

func (c *basicMetricComparison) HasChangedByLessThan(percentage float64) error {
	return nil
}

// TestQueryGeneration verifies that queries are generated correctly.
func TestQueryGeneration(t *testing.T) {
	// Create a basic metrics API for testing
	api := &basicMetricsAPI{}

	// Enable debug mode to capture queries
	api.EnableDebugMode(true)

	// Test simple query
	query := api.AssertThat("sql_txn_count").(*basicMetricQuery)
	generatedQuery := query.GetBuiltQuery()
	assert.Equal(t, "sql_txn_count", generatedQuery, "Simple query should match metric name")

	// Test query with node filter
	query = api.AssertThat("liveness_heartbeats_successful").ForNode("1").(*basicMetricQuery)
	generatedQuery = query.GetBuiltQuery()
	assert.Equal(t, `liveness_heartbeats_successful{node="1"}`, generatedQuery, "Query with node filter should include node label")

	// Test query with time range and rate
	query = api.AssertThat("sql_select_count").OverLast("30s").Rate().(*basicMetricQuery)
	generatedQuery = query.GetBuiltQuery()
	assert.Equal(t, `rate(sql_select_count[30s])`, generatedQuery, "Rate query should wrap metric in rate function")

	// Test query with aggregation
	query = api.AssertThat("sql_txn_count").Sum().(*basicMetricQuery)
	generatedQuery = query.GetBuiltQuery()
	assert.Equal(t, `sum(sql_txn_count)`, generatedQuery, "Query with sum aggregation should be wrapped in sum function")

	// Test complex query with multiple filters and aggregation
	query = api.AssertThat("sql_exec_latency").ForNode("1").WithLabel("app", "test").OverLast("1m").Avg().Rate().(*basicMetricQuery)
	generatedQuery = query.GetBuiltQuery()
	assert.Equal(t, `avg(rate(sql_exec_latency{node="1",app="test"}[1m]))`, generatedQuery, "Complex query should combine all elements correctly")
}

// TestDebugModeRetrieval verifies that debug mode properly stores queries and results.
func TestDebugModeRetrieval(t *testing.T) {
	// Create a basic metrics API for testing
	api := &basicMetricsAPI{}

	// Enable debug mode
	api.EnableDebugMode(true)

	// Execute a query
	err := api.AssertThat("sql_txn_count").HasValueAtLeast(1)
	require.NoError(t, err, "Mock query should not fail")

	// Verify the query was stored
	lastQuery := api.GetLastQuery()
	assert.Equal(t, "sql_txn_count", lastQuery, "Last query should be stored correctly")

	// Verify the result was stored
	lastResult := api.GetLastQueryResult()
	assert.Equal(t, 1.0, lastResult, "Last result should be stored correctly")

	// Execute a more complex query
	err = api.AssertThat("sql_select_count").ForNode("2").OverLast("1m").Rate().HasRateAtLeast(0.1)
	require.NoError(t, err, "Mock complex query should not fail")

	// Verify the complex query was stored
	lastQuery = api.GetLastQuery()
	assert.True(t, strings.Contains(lastQuery, "sql_select_count"), "Last query should contain the metric name")
	assert.True(t, strings.Contains(lastQuery, `node="2"`), "Last query should contain the node filter")
	assert.True(t, strings.Contains(lastQuery, "rate"), "Last query should be a rate query")
}
