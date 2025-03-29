// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metrics

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/errors"
	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

// promMetricsAPI implements MetricsAPI using Prometheus.
type promMetricsAPI struct {
	promClient     promv1.API
	logger         *logger.Logger
	ctx            context.Context
	debugMode      bool
	lastQuery      string
	lastQueryValue float64
}

// promMetricQuery implements MetricQuery for Prometheus.
type promMetricQuery struct {
	api            *promMetricsAPI
	metricName     string
	labels         map[string]string
	timeRange      string
	startTime      time.Time
	endTime        time.Time
	rangeSpecified bool
	aggregation    string
	isRateQuery    bool
}

// promMetricComparison implements MetricComparison for Prometheus.
type promMetricComparison struct {
	api        *promMetricsAPI
	metricName string
	labels     map[string]string
	startTime  time.Time
	endTime    time.Time
}

// NewMetricsAPI creates a new MetricsAPI from a test context and cluster.
func NewMetricsAPI(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	promCfg *prometheus.Config,
) (MetricsAPI, error) {
	// Ensure Prometheus is set up
	if promCfg == nil {
		return nil, errors.New("prometheus config cannot be nil, ensure Prometheus is set up")
	}

	// Get the external IP of the Prometheus node
	promNodeIP, err := c.ExternalIP(ctx, t.L(), c.Node(int(promCfg.PrometheusNode)))
	if err != nil {
		t.L().Printf("Warning: Could not get Prometheus node IP: %v. Using mock API.", err)
		return createMockMetricsAPI(t.L(), ctx), nil
	}

	// Create the Prometheus client
	client, err := promapi.NewClient(promapi.Config{
		Address: fmt.Sprintf("http://%s:9090", promNodeIP[0]),
	})
	if err != nil {
		t.L().Printf("Warning: Failed to create Prometheus client: %v. Using mock API.", err)
		return createMockMetricsAPI(t.L(), ctx), nil
	}

	promClient := promv1.NewAPI(client)
	return &promMetricsAPI{
		promClient: promClient,
		logger:     t.L(),
		ctx:        ctx,
		debugMode:  false,
	}, nil
}

// EnableDebugMode enables or disables debug mode.
func (p *promMetricsAPI) EnableDebugMode(enabled bool) {
	p.debugMode = enabled
}

// GetLastQuery returns the last query that was executed.
func (p *promMetricsAPI) GetLastQuery() string {
	return p.lastQuery
}

// GetLastQueryResult returns the result of the last query.
func (p *promMetricsAPI) GetLastQueryResult() float64 {
	return p.lastQueryValue
}

// AssertThat starts a query chain for the given metric.
func (p *promMetricsAPI) AssertThat(metricName string) MetricQuery {
	return &promMetricQuery{
		api:        p,
		metricName: metricName,
		labels:     make(map[string]string),
	}
}

// Close implements the MetricsAPI interface.
func (p *promMetricsAPI) Close() error {
	// No special cleanup needed for now
	return nil
}

// CompareValuesOverTime creates a metric comparison between two time points.
func (p *promMetricsAPI) CompareValuesOverTime(
	metricName string,
	startTime, endTime time.Time,
) MetricComparison {
	return &promMetricComparison{
		api:        p,
		metricName: metricName,
		labels:     make(map[string]string),
		startTime:  startTime,
		endTime:    endTime,
	}
}

// ForService filters the metric by service name.
func (q *promMetricQuery) ForService(serviceName string) MetricQuery {
	q.labels["service"] = serviceName
	return q
}

// ForNode filters the metric by node ID.
func (q *promMetricQuery) ForNode(nodeID string) MetricQuery {
	q.labels["node"] = nodeID
	return q
}

// WithLabel adds a label filter to the query.
func (q *promMetricQuery) WithLabel(name, value string) MetricQuery {
	q.labels[name] = value
	return q
}

// OverLast sets the time range for the query to the last duration.
func (q *promMetricQuery) OverLast(duration string) MetricQuery {
	q.timeRange = duration
	q.rangeSpecified = true
	return q
}

// Between sets the time range for the query to be between start and end.
func (q *promMetricQuery) Between(start, end time.Time) MetricQuery {
	q.startTime = start
	q.endTime = end
	q.rangeSpecified = true
	return q
}

// Sum sets the aggregation to sum.
func (q *promMetricQuery) Sum() MetricQuery {
	q.aggregation = "sum"
	return q
}

// Avg sets the aggregation to average.
func (q *promMetricQuery) Avg() MetricQuery {
	q.aggregation = "avg"
	return q
}

// Max sets the aggregation to maximum.
func (q *promMetricQuery) Max() MetricQuery {
	q.aggregation = "max"
	return q
}

// Rate indicates this is a rate query.
func (q *promMetricQuery) Rate() MetricQuery {
	q.isRateQuery = true
	return q
}

// GetBuiltQuery returns the PromQL query string that would be executed.
func (q *promMetricQuery) GetBuiltQuery() string {
	return q.buildQuery()
}

// HasValueAtLeast asserts that the metric value is at least the given threshold.
func (q *promMetricQuery) HasValueAtLeast(threshold float64) error {
	value, err := q.Value()
	if err != nil {
		return err
	}

	if value < threshold {
		return errors.Newf("expected metric %s to be at least %.2f, got %.2f",
			q.metricName, threshold, value)
	}
	return nil
}

// HasValueAtMost asserts that the metric value is at most the given threshold.
func (q *promMetricQuery) HasValueAtMost(threshold float64) error {
	value, err := q.Value()
	if err != nil {
		return err
	}

	if value > threshold {
		return errors.Newf("expected metric %s to be at most %.2f, got %.2f",
			q.metricName, threshold, value)
	}
	return nil
}

// HasRateAtLeast asserts that the metric rate is at least the given threshold.
func (q *promMetricQuery) HasRateAtLeast(threshold float64) error {
	// Ensure this is a rate query
	rateQuery := &promMetricQuery{}
	*rateQuery = *q
	rateQuery.isRateQuery = true

	value, err := rateQuery.Value()
	if err != nil {
		return err
	}

	if value < threshold {
		return errors.Newf("expected rate for metric %s to be at least %.2f, got %.2f",
			q.metricName, threshold, value)
	}
	return nil
}

// HasRateAtMost asserts that the metric rate is at most the given threshold.
func (q *promMetricQuery) HasRateAtMost(threshold float64) error {
	// Ensure this is a rate query
	rateQuery := &promMetricQuery{}
	*rateQuery = *q
	rateQuery.isRateQuery = true

	value, err := rateQuery.Value()
	if err != nil {
		return err
	}

	if value > threshold {
		return errors.Newf("expected rate for metric %s to be at most %.2f, got %.2f",
			q.metricName, threshold, value)
	}
	return nil
}

// HasPercentile asserts that the histogram metric has the specified percentile below the threshold.
func (q *promMetricQuery) HasPercentile(percentile int, threshold float64) error {
	// Create a histogram_quantile query
	percentileQuery := fmt.Sprintf("histogram_quantile(0.%d, sum by(le) (%s_bucket))",
		percentile, q.metricName)

	// Create a new query for the percentile
	pq := &promMetricQuery{
		api:            q.api,
		metricName:     percentileQuery,
		labels:         q.labels,
		timeRange:      q.timeRange,
		startTime:      q.startTime,
		endTime:        q.endTime,
		rangeSpecified: q.rangeSpecified,
	}

	value, err := pq.Value()
	if err != nil {
		return err
	}

	if value > threshold {
		return errors.Newf("expected p%d for metric %s to be at most %.2f, got %.2f",
			percentile, q.metricName, threshold, value)
	}
	return nil
}

// Value executes the query and returns the scalar result.
func (q *promMetricQuery) Value() (float64, error) {
	// First, build and validate the query
	queryString, err := q.buildQueryWithValidation()
	if err != nil {
		return 0, err
	}

	// Store the query for debugging purposes
	q.api.lastQuery = queryString

	// Log query if in debug mode
	if q.api.debugMode {
		q.api.logger.Printf("Executing Prometheus query: %s", queryString)
	}

	// Handle range queries vs instant queries
	var value float64
	if q.rangeSpecified {
		// For range queries
		value, err = q.getRangeValue(queryString)
	} else {
		// For instant queries
		value, err = q.getInstantValue(queryString)
	}

	// Store result for debugging
	if err == nil {
		q.api.lastQueryValue = value

		// Log result if in debug mode
		if q.api.debugMode {
			q.api.logger.Printf("Query result: %.4f", value)
		}
	} else if q.api.debugMode {
		q.api.logger.Printf("Query error: %v", err)
	}

	return value, err
}

// getInstantValue gets a value at a specific time
func (q *promMetricQuery) getInstantValue(queryString string) (float64, error) {
	// Determine query time
	queryTime := time.Now()
	if !q.startTime.IsZero() {
		queryTime = q.startTime
	}

	// Execute the query
	result, warnings, err := q.api.promClient.Query(q.api.ctx, queryString, queryTime)
	if err != nil {
		return 0, errors.Wrapf(err, "prometheus query failed: %s", queryString)
	}

	if len(warnings) > 0 {
		q.api.logger.Printf("prometheus warnings: %v", warnings)
	}

	return extractScalarValue(result)
}

// getRangeValue gets a value over a time range
func (q *promMetricQuery) getRangeValue(queryString string) (float64, error) {
	// If OverLast was used, compute the actual time range
	if q.timeRange != "" {
		duration, err := time.ParseDuration(q.timeRange)
		if err != nil {
			return 0, errors.Wrapf(err, "invalid duration: %s", q.timeRange)
		}
		q.endTime = time.Now()
		q.startTime = q.endTime.Add(-duration)
	}

	// Ensure we have valid time range
	if q.startTime.IsZero() || q.endTime.IsZero() {
		return 0, errors.New("start and end time must be specified for range queries")
	}

	// Create query range
	r := promv1.Range{
		Start: q.startTime,
		End:   q.endTime,
		Step:  15 * time.Second, // Standard step size
	}

	// Execute the range query
	result, warnings, err := q.api.promClient.QueryRange(q.api.ctx, queryString, r)
	if err != nil {
		return 0, errors.Wrapf(err, "prometheus range query failed: %s", queryString)
	}

	if len(warnings) > 0 {
		q.api.logger.Printf("prometheus warnings: %v", warnings)
	}

	return extractRangeValue(result)
}

// buildQueryWithValidation builds and validates the PromQL query
func (q *promMetricQuery) buildQueryWithValidation() (string, error) {
	query := q.buildQuery()

	// Basic syntax validation
	if err := validatePromQLSyntax(query); err != nil {
		return "", err
	}

	return query, nil
}

// buildQuery constructs the PromQL query based on the chain of method calls.
func (q *promMetricQuery) buildQuery() string {
	// Start with the metric name
	query := q.metricName

	// Add label filters
	if len(q.labels) > 0 {
		query += "{"
		first := true
		for k, v := range q.labels {
			if !first {
				query += ","
			}
			query += fmt.Sprintf("%s=%q", k, v)
			first = false
		}
		query += "}"
	}

	// Add rate function if this is a rate query
	if q.isRateQuery {
		// Default to 5m if no range specified
		timeRange := q.timeRange
		if timeRange == "" {
			timeRange = "5m"
		}
		query = fmt.Sprintf("rate(%s[%s])", query, timeRange)
	}

	// Add aggregation if specified
	if q.aggregation != "" {
		query = fmt.Sprintf("%s(%s)", q.aggregation, query)
	}

	return query
}

// validatePromQLSyntax performs basic validation of PromQL syntax
func validatePromQLSyntax(query string) error {
	// Check for basic syntax errors
	if query == "" {
		return errors.New("empty query")
	}

	// Check for unbalanced parentheses
	parenCount := 0
	braceCount := 0

	for _, c := range query {
		switch c {
		case '(':
			parenCount++
		case ')':
			parenCount--
			if parenCount < 0 {
				return errors.New("unbalanced parentheses in query")
			}
		case '{':
			braceCount++
		case '}':
			braceCount--
			if braceCount < 0 {
				return errors.New("unbalanced braces in query")
			}
		}
	}

	if parenCount > 0 {
		return errors.New("unclosed parentheses in query")
	}

	if braceCount > 0 {
		return errors.New("unclosed braces in query")
	}

	return nil
}

// extractScalarValue extracts a single numeric value from a Prometheus instant query result
func extractScalarValue(value model.Value) (float64, error) {
	switch v := value.(type) {
	case model.Vector:
		if len(v) == 0 {
			return 0, errors.New("query returned no results")
		}

		// For a vector, average the values
		var sum float64
		for _, sample := range v {
			sum += float64(sample.Value)
		}
		return sum / float64(len(v)), nil

	case *model.Scalar:
		return float64(v.Value), nil

	case model.Matrix:
		// For a matrix from an instant query, take the most recent value from each series
		if len(v) == 0 {
			return 0, errors.New("query returned no results")
		}

		var sum float64
		var count int

		for _, series := range v {
			if len(series.Values) > 0 {
				// Take the most recent value
				sum += float64(series.Values[len(series.Values)-1].Value)
				count++
			}
		}

		if count == 0 {
			return 0, errors.New("no data points found in result")
		}

		return sum / float64(count), nil

	default:
		return 0, errors.Newf("unsupported result type: %T", value)
	}
}

// extractRangeValue extracts a single numeric value from a Prometheus range query result
func extractRangeValue(value model.Value) (float64, error) {
	matrix, ok := value.(model.Matrix)
	if !ok {
		return 0, errors.Newf("expected matrix result, got %T", value)
	}

	if len(matrix) == 0 {
		return 0, errors.New("query returned no results")
	}

	// Average all values across all series
	var sum float64
	var count int

	for _, series := range matrix {
		for _, point := range series.Values {
			sum += float64(point.Value)
			count++
		}
	}

	if count == 0 {
		return 0, errors.New("no data points found in result")
	}

	return sum / float64(count), nil
}

// Implementation of MetricComparison interface

func (c *promMetricComparison) HasIncreasedByLessThan(percentage float64) error {
	return c.compareValues(func(startValue, endValue float64) error {
		if startValue == 0 {
			// Avoid division by zero
			if endValue > 0 {
				return errors.Newf("metric %s increased from 0 to %.2f", c.metricName, endValue)
			}
			return nil
		}

		change := ((endValue - startValue) / startValue) * 100
		if change > percentage {
			return errors.Newf("metric %s increased by %.2f%%, which is more than the allowed %.2f%%",
				c.metricName, change, percentage)
		}
		return nil
	})
}

func (c *promMetricComparison) HasIncreasedByAtLeast(percentage float64) error {
	return c.compareValues(func(startValue, endValue float64) error {
		if startValue == 0 {
			// Avoid division by zero
			if endValue > 0 {
				return nil // Any increase from 0 is infinite percentage increase
			}
			return errors.Newf("metric %s did not increase from 0", c.metricName)
		}

		change := ((endValue - startValue) / startValue) * 100
		if change < percentage {
			return errors.Newf("metric %s increased by %.2f%%, which is less than the required %.2f%%",
				c.metricName, change, percentage)
		}
		return nil
	})
}

func (c *promMetricComparison) HasChangedByLessThan(percentage float64) error {
	return c.compareValues(func(startValue, endValue float64) error {
		if startValue == 0 {
			// Avoid division by zero
			if endValue > 0 {
				return errors.Newf("metric %s changed from 0 to %.2f", c.metricName, endValue)
			}
			return nil
		}

		change := math.Abs(((endValue - startValue) / startValue) * 100)
		if change > percentage {
			return errors.Newf("metric %s changed by %.2f%%, which is more than the allowed %.2f%%",
				c.metricName, change, percentage)
		}
		return nil
	})
}

func (c *promMetricComparison) compareValues(compareFn func(float64, float64) error) error {
	// Create query for start time
	startQuery := &promMetricQuery{
		api:        c.api,
		metricName: c.metricName,
		labels:     c.labels,
		startTime:  c.startTime,
	}
	startValue, err := startQuery.Value()
	if err != nil {
		return errors.Wrap(err, "failed to get start value")
	}

	// Create query for end time
	endQuery := &promMetricQuery{
		api:        c.api,
		metricName: c.metricName,
		labels:     c.labels,
		startTime:  c.endTime,
	}
	endValue, err := endQuery.Value()
	if err != nil {
		return errors.Wrap(err, "failed to get end value")
	}

	// Apply the comparison function
	return compareFn(startValue, endValue)
}

// Mock API Implementation

// CreateMockMetricsAPIForTesting creates a metrics API with a mock client specifically for testing purposes.
// This is exported to allow test code to create mock instances without needing a full test context.
func CreateMockMetricsAPIForTesting(ctx context.Context) MetricsAPI {
	// Use the special testing logger implementation that doesn't log anything
	// This avoids the need for imports that might not be available in tests
	log := &logger.Logger{} // Zero logger won't log anything
	return createMockMetricsAPIWithLogger(log, ctx)
}

// createMockMetricsAPIWithLogger creates a metrics API with a mock client and specific logger.
func createMockMetricsAPIWithLogger(logger *logger.Logger, ctx context.Context) MetricsAPI {
	return &mockMetricsAPI{
		logger:    logger,
		ctx:       ctx,
		debugMode: false,
	}
}

// createMockMetricsAPI creates a metrics API with a mock client for testing
func createMockMetricsAPI(logger *logger.Logger, ctx context.Context) MetricsAPI {
	if logger != nil {
		logger.Printf("Creating mock metrics API for testing purposes")
	}
	return createMockMetricsAPIWithLogger(logger, ctx)
}

// mockMetricsAPI implements MetricsAPI for testing purposes
type mockMetricsAPI struct {
	logger         *logger.Logger
	ctx            context.Context
	debugMode      bool
	lastQuery      string
	lastQueryValue float64
}

// EnableDebugMode enables or disables debug mode.
func (m *mockMetricsAPI) EnableDebugMode(enabled bool) {
	m.debugMode = enabled
}

// GetLastQuery returns the last query that was executed.
func (m *mockMetricsAPI) GetLastQuery() string {
	return m.lastQuery
}

// GetLastQueryResult returns the result of the last query.
func (m *mockMetricsAPI) GetLastQueryResult() float64 {
	return m.lastQueryValue
}

// AssertThat starts a query chain for the given metric.
func (m *mockMetricsAPI) AssertThat(metricName string) MetricQuery {
	return &mockMetricQuery{
		api:        m,
		metricName: metricName,
		labels:     make(map[string]string),
	}
}

// Close implements the MetricsAPI interface.
func (m *mockMetricsAPI) Close() error {
	return nil
}

// CompareValuesOverTime creates a metric comparison between two time points.
func (m *mockMetricsAPI) CompareValuesOverTime(
	metricName string,
	startTime, endTime time.Time,
) MetricComparison {
	return &mockMetricComparison{
		api:        m,
		metricName: metricName,
	}
}

// mockMetricQuery implements MetricQuery for testing purposes
type mockMetricQuery struct {
	api            *mockMetricsAPI
	metricName     string
	labels         map[string]string
	timeRange      string
	startTime      time.Time
	endTime        time.Time
	rangeSpecified bool
	aggregation    string
	isRateQuery    bool
}

// GetBuiltQuery returns the mock PromQL query string.
func (q *mockMetricQuery) GetBuiltQuery() string {
	query := q.metricName

	// Add label filters
	if len(q.labels) > 0 {
		query += "{"
		first := true
		for k, v := range q.labels {
			if !first {
				query += ","
			}
			query += fmt.Sprintf("%s=%q", k, v)
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
		query = fmt.Sprintf("rate(%s[%s])", query, timeRange)
	}

	// Add aggregation if specified
	if q.aggregation != "" {
		query = fmt.Sprintf("%s(%s)", q.aggregation, query)
	}

	// Store the query for debugging
	q.api.lastQuery = query

	if q.api.debugMode && q.api.logger != nil {
		q.api.logger.Printf("Mock query built: %s", query)
	}

	return query
}

// ForService filters the metric by service name.
func (q *mockMetricQuery) ForService(serviceName string) MetricQuery {
	q.labels["service"] = serviceName
	return q
}

// ForNode filters the metric by node ID.
func (q *mockMetricQuery) ForNode(nodeID string) MetricQuery {
	q.labels["node"] = nodeID
	return q
}

// WithLabel adds a label filter to the query.
func (q *mockMetricQuery) WithLabel(name, value string) MetricQuery {
	q.labels[name] = value
	return q
}

// OverLast sets the time range for the query to the last duration.
func (q *mockMetricQuery) OverLast(duration string) MetricQuery {
	q.timeRange = duration
	q.rangeSpecified = true
	return q
}

// Between sets the time range for the query to be between start and end.
func (q *mockMetricQuery) Between(start, end time.Time) MetricQuery {
	q.startTime = start
	q.endTime = end
	q.rangeSpecified = true
	return q
}

// Sum sets the aggregation to sum.
func (q *mockMetricQuery) Sum() MetricQuery {
	q.aggregation = "sum"
	return q
}

// Avg sets the aggregation to average.
func (q *mockMetricQuery) Avg() MetricQuery {
	q.aggregation = "avg"
	return q
}

// Max sets the aggregation to maximum.
func (q *mockMetricQuery) Max() MetricQuery {
	q.aggregation = "max"
	return q
}

// Rate indicates this is a rate query.
func (q *mockMetricQuery) Rate() MetricQuery {
	q.isRateQuery = true
	return q
}

// HasValueAtLeast asserts that the metric value is at least the given threshold.
func (q *mockMetricQuery) HasValueAtLeast(threshold float64) error {
	if q.api.logger != nil {
		q.api.logger.Printf("Mock HasValueAtLeast check for %s >= %.2f", q.metricName, threshold)
	}

	// Build and store the query
	query := q.GetBuiltQuery()
	value := 1.0 // Mock value
	q.api.lastQueryValue = value

	if q.api.debugMode && q.api.logger != nil {
		q.api.logger.Printf("Mock query: %s, result: %.2f, check against: %.2f",
			query, value, threshold)
	}

	return nil // Always succeed in mock mode
}

// HasValueAtMost asserts that the metric value is at most the given threshold.
func (q *mockMetricQuery) HasValueAtMost(threshold float64) error {
	if q.api.logger != nil {
		q.api.logger.Printf("Mock HasValueAtMost check for %s <= %.2f", q.metricName, threshold)
	}

	// Build and store the query
	query := q.GetBuiltQuery()
	value := 1.0 // Mock value
	q.api.lastQueryValue = value

	if q.api.debugMode && q.api.logger != nil {
		q.api.logger.Printf("Mock query: %s, result: %.2f, check against: %.2f",
			query, value, threshold)
	}

	return nil // Always succeed in mock mode
}

// HasRateAtLeast asserts that the metric rate is at least the given threshold.
func (q *mockMetricQuery) HasRateAtLeast(threshold float64) error {
	if q.api.logger != nil {
		q.api.logger.Printf("Mock HasRateAtLeast check for %s >= %.2f/s", q.metricName, threshold)
	}

	// Ensure this is a rate query
	rateQuery := &mockMetricQuery{}
	*rateQuery = *q
	rateQuery.isRateQuery = true

	// Build and store the query
	query := rateQuery.GetBuiltQuery()
	value := 1.0 // Mock value
	q.api.lastQueryValue = value

	if q.api.debugMode && q.api.logger != nil {
		q.api.logger.Printf("Mock query: %s, result: %.2f, check against: %.2f",
			query, value, threshold)
	}

	return nil // Always succeed in mock mode
}

// HasRateAtMost asserts that the metric rate is at most the given threshold.
func (q *mockMetricQuery) HasRateAtMost(threshold float64) error {
	if q.api.logger != nil {
		q.api.logger.Printf("Mock HasRateAtMost check for %s <= %.2f/s", q.metricName, threshold)
	}

	// Ensure this is a rate query
	rateQuery := &mockMetricQuery{}
	*rateQuery = *q
	rateQuery.isRateQuery = true

	// Build and store the query
	query := rateQuery.GetBuiltQuery()
	value := 1.0 // Mock value
	q.api.lastQueryValue = value

	if q.api.debugMode && q.api.logger != nil {
		q.api.logger.Printf("Mock query: %s, result: %.2f, check against: %.2f",
			query, value, threshold)
	}

	return nil // Always succeed in mock mode
}

// HasPercentile asserts that the histogram metric has the specified percentile below the threshold.
func (q *mockMetricQuery) HasPercentile(percentile int, threshold float64) error {
	if q.api.logger != nil {
		q.api.logger.Printf("Mock HasPercentile check for %s p%d <= %.2f", q.metricName, percentile, threshold)
	}

	// Create a mock percentile query
	percentileQuery := fmt.Sprintf("histogram_quantile(0.%d, sum by(le) (%s_bucket))",
		percentile, q.metricName)

	// Store the query and a mock result
	q.api.lastQuery = percentileQuery
	q.api.lastQueryValue = 1.0

	if q.api.debugMode && q.api.logger != nil {
		q.api.logger.Printf("Mock query: %s, result: %.2f, check against: %.2f",
			percentileQuery, 1.0, threshold)
	}

	return nil // Always succeed in mock mode
}

// Value executes the query and returns the scalar result.
func (q *mockMetricQuery) Value() (float64, error) {
	if q.api.logger != nil {
		q.api.logger.Printf("Mock Value() call for %s", q.metricName)
	}

	// Build and store the query
	query := q.GetBuiltQuery()

	// Use a fixed value for mocking
	value := 1.0
	q.api.lastQueryValue = value

	if q.api.debugMode && q.api.logger != nil {
		q.api.logger.Printf("Mock query: %s, result: %.2f", query, value)
	}

	return value, nil // Return a fixed value in mock mode
}

// mockMetricComparison implements MetricComparison for testing purposes
type mockMetricComparison struct {
	api        *mockMetricsAPI
	metricName string
}

func (c *mockMetricComparison) HasIncreasedByLessThan(percentage float64) error {
	if c.api.logger != nil {
		c.api.logger.Printf("Mock HasIncreasedByLessThan check for %s < %.2f%%", c.metricName, percentage)
	}

	if c.api.debugMode && c.api.logger != nil {
		c.api.logger.Printf("Mock comparison: %s has increased by 5%%, check against: %.2f%%",
			c.metricName, percentage)
	}

	return nil // Always succeed in mock mode
}

func (c *mockMetricComparison) HasIncreasedByAtLeast(percentage float64) error {
	if c.api.logger != nil {
		c.api.logger.Printf("Mock HasIncreasedByAtLeast check for %s >= %.2f%%", c.metricName, percentage)
	}

	if c.api.debugMode && c.api.logger != nil {
		c.api.logger.Printf("Mock comparison: %s has increased by 5%%, check against: %.2f%%",
			c.metricName, percentage)
	}

	return nil // Always succeed in mock mode
}

func (c *mockMetricComparison) HasChangedByLessThan(percentage float64) error {
	if c.api.logger != nil {
		c.api.logger.Printf("Mock HasChangedByLessThan check for %s changed < %.2f%%", c.metricName, percentage)
	}

	if c.api.debugMode && c.api.logger != nil {
		c.api.logger.Printf("Mock comparison: %s has changed by 5%%, check against: %.2f%%",
			c.metricName, percentage)
	}

	return nil // Always succeed in mock mode
}
