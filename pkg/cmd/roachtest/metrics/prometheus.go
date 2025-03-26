// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metrics

import (
	"context"
	"fmt"
	"math"
	"os"
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

// PromMetricsAPI is a publicly-accessible version of promMetricsAPI with exported fields.
// It can be used for direct testing with a Prometheus client.
type PromMetricsAPI struct {
	PromClient promv1.API
	Logger     *logger.Logger
	Ctx        context.Context
	DebugMode  bool
	LastQuery  string
	LastValue  float64
}

// Ensure PromMetricsAPI implements MetricsAPI
var _ MetricsAPI = (*PromMetricsAPI)(nil)

// EnableDebugMode enables or disables debug mode.
func (p *PromMetricsAPI) EnableDebugMode(enabled bool) {
	p.DebugMode = enabled
}

// GetLastQuery returns the last executed query.
func (p *PromMetricsAPI) GetLastQuery() string {
	return p.LastQuery
}

// GetLastQueryResult returns the result of the last executed query.
func (p *PromMetricsAPI) GetLastQueryResult() float64 {
	return p.LastValue
}

// Query starts a new query for the given metric
func (p *PromMetricsAPI) Query(metricName string) MetricQuery {
	return &promMetricQuery{
		api: &promMetricsAPI{
			promClient:     p.PromClient,
			logger:         p.Logger,
			ctx:            p.Ctx,
			debugMode:      p.DebugMode,
			lastQuery:      p.LastQuery,
			lastQueryValue: p.LastValue,
		},
		metricName: metricName,
		labels:     make(map[string]string),
	}
}

// Close implements the MetricsAPI interface.
func (p *PromMetricsAPI) Close() error {
	// No special cleanup needed
	return nil
}

// CompareValuesOverTime creates a metric comparison between two time points.
func (p *PromMetricsAPI) CompareValuesOverTime(
	metricName string,
	startTime, endTime time.Time,
) MetricComparison {
	return &promMetricComparison{
		api: &promMetricsAPI{
			promClient:     p.PromClient,
			logger:         p.Logger,
			ctx:            p.Ctx,
			debugMode:      p.DebugMode,
			lastQuery:      p.LastQuery,
			lastQueryValue: p.LastValue,
		},
		metricName: metricName,
		labels:     make(map[string]string),
		startTime:  startTime,
		endTime:    endTime,
	}
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

	var promAddress string

	// Check for direct Prometheus URL from env var (for local testing)
	directPromURL := os.Getenv("PROMETHEUS_ADDR")
	if directPromURL != "" {
		t.Status(fmt.Sprintf("Using direct Prometheus URL: %s", directPromURL))
		promAddress = directPromURL
	} else {
		// Get the external IP of the Prometheus node
		promNodeIP, err := c.ExternalIP(ctx, t.L(), c.Node(int(promCfg.PrometheusNode)))
		if err != nil {
			return nil, errors.Wrap(err, "failed to get Prometheus node IP")
		}
		promAddress = fmt.Sprintf("http://%s:9090", promNodeIP[0])
	}

	// Create the Prometheus client
	client, err := promapi.NewClient(promapi.Config{
		Address: promAddress,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Prometheus client")
	}

	promClient := promv1.NewAPI(client)
	return &promMetricsAPI{
		promClient:     promClient,
		logger:         t.L(),
		ctx:            ctx,
		debugMode:      false,
		lastQuery:      "",
		lastQueryValue: 0,
	}, nil
}

// EnableDebugMode enables or disables debug mode.
func (p *promMetricsAPI) EnableDebugMode(enabled bool) {
	p.debugMode = enabled
}

// GetLastQuery returns the last executed query.
func (p *promMetricsAPI) GetLastQuery() string {
	return p.lastQuery
}

// GetLastQueryResult returns the result of the last executed query.
func (p *promMetricsAPI) GetLastQueryResult() float64 {
	return p.lastQueryValue
}

// Query starts a new query for the given metric
func (p *promMetricsAPI) Query(metricName string) MetricQuery {
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

// buildQuery constructs the PromQL query string.
func (q *promMetricQuery) buildQuery() string {
	var query string

	// Build the metric selector with labels
	query = q.metricName
	if len(q.labels) > 0 {
		query += "{"
		first := true
		for name, value := range q.labels {
			if !first {
				query += ","
			}
			query += fmt.Sprintf(`%s="%s"`, name, value)
			first = false
		}
		query += "}"
	}

	// Add time range if specified
	if q.rangeSpecified {
		if q.timeRange != "" {
			query += fmt.Sprintf("[%s]", q.timeRange)
		}
	}

	// Add rate if specified
	if q.isRateQuery {
		query = fmt.Sprintf("rate(%s)", query)
	}

	// Add aggregation if specified
	if q.aggregation != "" {
		query = fmt.Sprintf("%s(%s)", q.aggregation, query)
	}

	// Store the query for debugging
	q.api.lastQuery = query

	return query
}

// Value executes the query and returns the result as a float64.
func (q *promMetricQuery) Value() (float64, error) {
	result, warnings, err := q.api.promClient.Query(q.api.ctx, q.buildQuery(), time.Now())
	if err != nil {
		return 0, errors.Wrap(err, "failed to execute query")
	}

	if len(warnings) > 0 && q.api.debugMode {
		q.api.logger.Printf("Query warnings: %v", warnings)
	}

	vector, ok := result.(model.Vector)
	if !ok {
		return 0, errors.New("query result is not a vector")
	}

	if len(vector) == 0 {
		return 0, errors.New("no data points found")
	}

	value := float64(vector[0].Value)
	q.api.lastQueryValue = value
	return value, nil
}

// Scan executes the query and stores the result in the provided destination.
func (q *promMetricQuery) Scan(dest interface{}) error {
	value, err := q.Value()
	if err != nil {
		return err
	}

	switch d := dest.(type) {
	case *float64:
		*d = value
	case *int:
		*d = int(value)
	case *int64:
		*d = int64(value)
	default:
		return errors.Newf("unsupported destination type %T", dest)
	}

	return nil
}

// AssertHasValue asserts that the metric has the expected value.
func (q *promMetricQuery) AssertHasValue(expected float64) error {
	value, err := q.Value()
	if err != nil {
		return err
	}

	if value != expected {
		return errors.Newf("expected value %f but got %f", expected, value)
	}

	return nil
}

// AssertHasValueAtLeast asserts that the metric is at least the given threshold.
func (q *promMetricQuery) AssertHasValueAtLeast(threshold float64) error {
	value, err := q.Value()
	if err != nil {
		return err
	}

	if value < threshold {
		return errors.Newf("expected value >= %f but got %f", threshold, value)
	}

	return nil
}

// AssertHasValueAtMost asserts that the metric is at most the given threshold.
func (q *promMetricQuery) AssertHasValueAtMost(threshold float64) error {
	value, err := q.Value()
	if err != nil {
		return err
	}

	if value > threshold {
		return errors.Newf("expected value <= %f but got %f", threshold, value)
	}

	return nil
}

// AssertHasRateAtLeast asserts that the rate is at least the given threshold.
func (q *promMetricQuery) AssertHasRateAtLeast(threshold float64) error {
	q.isRateQuery = true
	return q.AssertHasValueAtLeast(threshold)
}

// AssertHasRateAtMost asserts that the rate is at most the given threshold.
func (q *promMetricQuery) AssertHasRateAtMost(threshold float64) error {
	q.isRateQuery = true
	return q.AssertHasValueAtMost(threshold)
}

// AssertHasPercentile asserts that the percentile is at most the given threshold.
func (q *promMetricQuery) AssertHasPercentile(percentile int, threshold float64) error {
	if percentile < 0 || percentile > 100 {
		return errors.Newf("percentile must be between 0 and 100, got %d", percentile)
	}

	// Add histogram_quantile function
	query := fmt.Sprintf("histogram_quantile(0.%d, %s)", percentile, q.buildQuery())
	result, warnings, err := q.api.promClient.Query(q.api.ctx, query, time.Now())
	if err != nil {
		return errors.Wrap(err, "failed to execute percentile query")
	}

	if len(warnings) > 0 && q.api.debugMode {
		q.api.logger.Printf("Percentile query warnings: %v", warnings)
	}

	vector, ok := result.(model.Vector)
	if !ok {
		return errors.New("percentile query result is not a vector")
	}

	if len(vector) == 0 {
		return errors.New("no data points found for percentile")
	}

	value := float64(vector[0].Value)
	q.api.lastQueryValue = value
	if value > threshold {
		return errors.Newf("expected p%d <= %f but got %f", percentile, threshold, value)
	}

	return nil
}

// HasIncreasedByLessThan implements the MetricComparison interface.
func (c *promMetricComparison) HasIncreasedByLessThan(percentage float64) error {
	startValue, endValue, err := c.getStartAndEndValues()
	if err != nil {
		return err
	}

	if startValue == 0 {
		return nil // Avoid division by zero
	}

	actualPercentage := ((endValue - startValue) / startValue) * 100
	if actualPercentage >= percentage {
		return errors.Newf(
			"metric increased by %.2f%%, which is not less than %.2f%%",
			actualPercentage, percentage)
	}

	return nil
}

// HasIncreasedByAtLeast implements the MetricComparison interface.
func (c *promMetricComparison) HasIncreasedByAtLeast(percentage float64) error {
	startValue, endValue, err := c.getStartAndEndValues()
	if err != nil {
		return err
	}

	if startValue == 0 {
		if endValue > 0 {
			return nil // Any increase from zero is infinite percentage
		}
		return errors.New("metric remained at zero")
	}

	actualPercentage := ((endValue - startValue) / startValue) * 100
	if actualPercentage < percentage {
		return errors.Newf(
			"metric increased by %.2f%%, which is less than %.2f%%",
			actualPercentage, percentage)
	}

	return nil
}

// HasChangedByLessThan implements the MetricComparison interface.
func (c *promMetricComparison) HasChangedByLessThan(percentage float64) error {
	startValue, endValue, err := c.getStartAndEndValues()
	if err != nil {
		return err
	}

	if startValue == 0 {
		if endValue == 0 {
			return nil // No change
		}
		return errors.New("cannot calculate percentage change from zero")
	}

	actualPercentage := math.Abs(((endValue - startValue) / startValue) * 100)
	if actualPercentage >= percentage {
		return errors.Newf(
			"metric changed by %.2f%%, which is not less than %.2f%%",
			actualPercentage, percentage)
	}

	return nil
}

// getStartAndEndValues returns the metric values at the start and end times.
func (c *promMetricComparison) getStartAndEndValues() (float64, float64, error) {
	query := &promMetricQuery{
		api:        c.api,
		metricName: c.metricName,
		labels:     c.labels,
	}

	// Get start value
	result, warnings, err := c.api.promClient.Query(c.api.ctx, query.buildQuery(), c.startTime)
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to get start value")
	}

	if len(warnings) > 0 && c.api.debugMode {
		c.api.logger.Printf("Start value query warnings: %v", warnings)
	}

	vector, ok := result.(model.Vector)
	if !ok {
		return 0, 0, errors.New("start value query result is not a vector")
	}

	if len(vector) == 0 {
		return 0, 0, errors.New("no data points found for start time")
	}

	startValue := float64(vector[0].Value)
	c.api.lastQueryValue = startValue

	// Get end value
	result, warnings, err = c.api.promClient.Query(c.api.ctx, query.buildQuery(), c.endTime)
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to get end value")
	}

	if len(warnings) > 0 && c.api.debugMode {
		c.api.logger.Printf("End value query warnings: %v", warnings)
	}

	vector, ok = result.(model.Vector)
	if !ok {
		return 0, 0, errors.New("end value query result is not a vector")
	}

	if len(vector) == 0 {
		return 0, 0, errors.New("no data points found for end time")
	}

	endValue := float64(vector[0].Value)
	c.api.lastQueryValue = endValue

	return startValue, endValue, nil
}
