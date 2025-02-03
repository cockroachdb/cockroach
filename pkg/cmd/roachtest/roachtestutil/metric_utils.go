// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/util"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram/exporter"
	"github.com/cockroachdb/errors"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"golang.org/x/exp/maps"
)

var openmetricsLineRegex = regexp.MustCompile(`^(\w+){([^}]*)} ([\d.e+-]+) ([\d.e+-]+)$`)

// AggregatedPerfMetrics is the output of PostProcessPerfMetrics function in individual test
type AggregatedPerfMetrics []*AggregatedMetric

// MetricPoint denotes the metric point both in case of summaries and gauge
type MetricPoint float64

// PerfMetricLabels is a slice of labels that were parsed from openmetrics bytes
type PerfMetricLabels []*Label

// AggregatedMetric is a single metric aggregated from summaries.
type AggregatedMetric struct {
	// Name denotes the name of the metric
	Name string
	// Value is the aggregated MetricPoint from the summaries
	Value MetricPoint
	// Unit is a custom string to denote the unit of the metric
	Unit string
	// IsHigherBetter denotes that during comparison, a higher value of this metric is considered better.
	// This will be used by during comparisons
	IsHigherBetter bool
}

// Label is a pair of name and value of a label
type Label struct {
	Name  string
	Value string
}

// HistogramSummaryMetric is a struct that is constructed from openmetrics bytes
type HistogramSummaryMetric struct {
	// Name is the name of the metric
	Name string
	// Values is a slice of HistogramSummaryMetricPoint during the entire run
	Values []*HistogramSummaryMetricPoint
	// HighestTrackableValue is the max value that the histogram can track
	HighestTrackableValue MetricPoint
}

// HistogramSummaryMetricPoint denotes a single reading of the histogram.
type HistogramSummaryMetricPoint struct {
	// P50, P95, P99 and P100 are the latency percentile readings
	P50  MetricPoint
	P95  MetricPoint
	P99  MetricPoint
	P100 MetricPoint
	// Mean is the mean of all readings
	Mean MetricPoint
	// Max is the max of all readings
	Max MetricPoint
	// Count denotes the cumulative count of all readings until Timestamp
	Count MetricPoint
	// Elapsed is the total time elapsed from the last reading
	Elapsed MetricPoint
	// Timestamp is the timestamp at the time of the reading
	Timestamp time.Time
}

// HistogramMetric is the input to PostProcessPerfMetrics for each test
type HistogramMetric struct {
	// Summaries contains all summaries parsed from the histograms openmetrics
	Summaries []*HistogramSummaryMetric
	// TotalCount is the total of all metrics' counts
	TotalCount MetricPoint
	// TotalElapsed is the total time of all metrics' runs
	TotalElapsed MetricPoint
}

// GetWorkloadHistogramArgs creates a histogram flag string based on the roachtest to pass to workload binary
// This is used to make use of t.ExportOpenmetrics() method and create appropriate exporter
func GetWorkloadHistogramArgs(t test.Test, c cluster.Cluster, labels map[string]string) string {
	var histogramArgs string
	if t.ExportOpenmetrics() {
		// Add openmetrics related labels and arguments
		histogramArgs = fmt.Sprintf(" --histogram-export-format='openmetrics' --histograms=%s/%s --openmetrics-labels='%s'",
			t.PerfArtifactsDir(), GetBenchmarkMetricsFileName(t), GetOpenmetricsLabelString(t, c, labels))
	} else {
		// Since default is json, no need to add --histogram-export-format flag in this case and also the labels
		histogramArgs = fmt.Sprintf(" --histograms=%s/%s", t.PerfArtifactsDir(), GetBenchmarkMetricsFileName(t))
	}

	return histogramArgs
}

// GetBenchmarkMetricsFileName returns the file name to store the benchmark output
func GetBenchmarkMetricsFileName(t test.Test) string {
	if t.ExportOpenmetrics() {
		return "stats.om"
	}

	return "stats.json"
}

// CreateWorkloadHistogramExporter creates a exporter.Exporter based on the roachtest parameters with no labels
func CreateWorkloadHistogramExporter(t test.Test, c cluster.Cluster) exporter.Exporter {
	return CreateWorkloadHistogramExporterWithLabels(t, c, nil)
}

// CreateWorkloadHistogramExporterWithLabels creates a exporter.Exporter based on the roachtest parameters with additional labels
func CreateWorkloadHistogramExporterWithLabels(
	t test.Test, c cluster.Cluster, labelMap map[string]string,
) exporter.Exporter {
	var metricsExporter exporter.Exporter
	if t.ExportOpenmetrics() {
		labels := GetOpenmetricsLabelMap(t, c, labelMap)
		openMetricsExporter := &exporter.OpenMetricsExporter{}
		openMetricsExporter.SetLabels(&labels)
		metricsExporter = openMetricsExporter

	} else {
		metricsExporter = &exporter.HdrJsonExporter{}
	}

	return metricsExporter
}

// UploadPerfStats creates stats file from buffer in the node
func UploadPerfStats(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	perfBuf *bytes.Buffer,
	node option.NodeListOption,
	fileNamePrefix string,
) error {

	if perfBuf == nil {
		return errors.New("perf buffer is nil")
	}
	destinationFileName := fmt.Sprintf("%s%s", fileNamePrefix, GetBenchmarkMetricsFileName(t))
	// Upload the perf artifacts to any one of the nodes so that the test
	// runner copies it into an appropriate directory path.
	dest := filepath.Join(t.PerfArtifactsDir(), destinationFileName)
	if err := c.RunE(ctx, option.WithNodes(node), "mkdir -p "+filepath.Dir(dest)); err != nil {
		return err
	}
	if err := c.PutString(ctx, perfBuf.String(), dest, 0755, node); err != nil {
		return err
	}
	return nil
}

// CloseExporter closes the exporter and also upload the metrics artifacts to a stats file in the node
func CloseExporter(
	ctx context.Context,
	exporter exporter.Exporter,
	t test.Test,
	c cluster.Cluster,
	perfBuf *bytes.Buffer,
	node option.NodeListOption,
	fileNamePrefix string,
) {
	if err := exporter.Close(func() error {
		if err := UploadPerfStats(ctx, t, c, perfBuf, node, fileNamePrefix); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Errorf("failed to export perf stats: %v", err)
	}
}

// GetPerfArtifacts gets the perf file from the node into the destination provided by dest
func GetPerfArtifacts(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	node option.NodeListOption,
	dest string,
	fileNamePrefix string,
) error {
	sourceFileName := fmt.Sprintf("%s%s", fileNamePrefix, GetBenchmarkMetricsFileName(t))
	sourcePerfDirectory := filepath.Join(t.PerfArtifactsDir(), sourceFileName)
	if err := c.Get(ctx, t.L(), sourcePerfDirectory, dest, node); err != nil {
		return err
	}
	return nil
}

// PostProcessMetrics is used to first convert the perfBuf into prometheus metric family,
// call the processFunction passed and then returns a byte buffer which has the metrics returned by process function
// and the initial metric bytes
func PostProcessMetrics(
	test string,
	perfBuf *bytes.Buffer,
	processFunction func(string, *HistogramMetric) (AggregatedPerfMetrics, error),
) (AggregatedPerfMetrics, []*Label, error) {
	if processFunction == nil {
		return nil, nil, errors.New("process function is nil")
	}

	histogramMetrics, labels, err := getHistogramMetrics(perfBuf)
	if err != nil {
		return nil, nil, err
	}

	// Call the process function
	processedMetrics, err := processFunction(test, histogramMetrics)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to post process metrics")
	}

	return processedMetrics, labels, nil
}

// GetAggregatedMetricBytes is used to convert AggregatedPerfMetrics to bytes which are added to metricBytes in
// openmetrics format
func GetAggregatedMetricBytes(
	aggregatedMetrics AggregatedPerfMetrics,
	labels []*Label,
	timestamp time.Time,
	metricBytes *bytes.Buffer,
) error {
	writer := bufio.NewWriter(metricBytes)
	defer writer.Flush()

	// At the end of this function, add `# EOF` at the end of the file
	defer func() {
		_, _ = expfmt.FinalizeOpenMetrics(writer)
	}()

	var aggregateLabels []*dto.LabelPair
	for _, label := range labels {

		labelName := util.SanitizeKey(label.Name)
		// In case the label value already has surrounding quotes, we should trim them
		labelValue := util.SanitizeValue(strings.Trim(label.Value, `"`))

		aggregateLabels = append(aggregateLabels, &dto.LabelPair{
			Name:  &labelName,
			Value: &labelValue,
		})
	}
	timeMillis := timestamp.UnixMilli()
	for _, metric := range aggregatedMetrics {
		metricName := util.SanitizeMetricName(metric.Name)
		metricValue := float64(metric.Value)
		currentLabels := addAggregateLabels(aggregateLabels, metric)
		metricFamily := &dto.MetricFamily{
			Name: &metricName,
			Type: dto.MetricType_GAUGE.Enum(),
			Metric: []*dto.Metric{{
				Gauge: &dto.Gauge{
					Value: &metricValue,
				},
				Label:       currentLabels,
				TimestampMs: &timeMillis,
			}},
		}

		if _, err := expfmt.MetricFamilyToOpenMetrics(writer, metricFamily); err != nil {
			return errors.Wrapf(err, "failed to parse metrics")
		}
	}
	return nil
}

func addAggregateLabels(finalLabels []*dto.LabelPair, metric *AggregatedMetric) []*dto.LabelPair {
	var labels []*dto.LabelPair
	labels = append(labels, finalLabels...)

	// Add `unit` label
	labelName := util.SanitizeKey("unit")
	labelValue := util.SanitizeValue(metric.Unit)
	labels = append(labels, &dto.LabelPair{
		Name:  &labelName,
		Value: &labelValue,
	})

	// Add `is_higher_better` label
	newLabelName := util.SanitizeKey("is_higher_better")
	newLabelValue := util.SanitizeValue(strconv.FormatBool(metric.IsHigherBetter))
	labels = append(labels, &dto.LabelPair{
		Name:  &newLabelName,
		Value: &newLabelValue,
	})

	return labels
}

func getHistogramMetrics(perfBuf *bytes.Buffer) (*HistogramMetric, PerfMetricLabels, error) {
	scanner := bufio.NewScanner(perfBuf)
	metricMap := make(map[string]*HistogramSummaryMetric)

	var labels []*Label

	var metricPoint *HistogramSummaryMetricPoint
	var currentMetric *HistogramSummaryMetric
	for scanner.Scan() {
		line := scanner.Text()
		var err error
		if strings.HasPrefix(line, "# TYPE") {
			// If we encountered a new summary metric, we need to reset currentMetric.
			if strings.Contains(line, "summary") {
				currentMetric, err = processMetricTypeLine(line, metricMap)
				metricPoint = &HistogramSummaryMetricPoint{}
				currentMetric.Values = append(currentMetric.Values, metricPoint)
			} else if strings.Contains(line, "highest_trackable_value") {
				// Process highest trackable value in a different function from other gauge metrics since it is calculated
				// at the end of the file and is unique for the whole run rather than each iteration of the run
				err = processHighestTrackableValue(line, metricMap, scanner)
			}
		} else if strings.HasPrefix(line, "# EOF") {
			break
		} else {

			// We got a non-histogram metric value, we should skip it.
			if currentMetric == nil || metricPoint == nil {
				continue
			}

			// Process the current metric line for the current metric
			labels, err = processMetricLine(line, metricPoint)
		}
		if err != nil {
			return nil, nil, err
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, nil, errors.Wrapf(err, "failed to scan metrics")
	}

	var summaries []*HistogramSummaryMetric
	totalCount := MetricPoint(0)
	totalElapsed := MetricPoint(0)

	for _, metric := range metricMap {
		summaries = append(summaries, metric)
		for _, value := range metric.Values {
			totalCount += value.Count
			totalElapsed += value.Elapsed
		}
	}
	return &HistogramMetric{
		Summaries:    summaries,
		TotalCount:   totalCount,
		TotalElapsed: totalElapsed,
	}, labels, nil
}

func processHighestTrackableValue(
	line string, metricMap map[string]*HistogramSummaryMetric, scanner *bufio.Scanner,
) error {
	parts := strings.Fields(line)
	if len(parts) >= 3 {
		metricName := strings.TrimSuffix(parts[2], "_highest_trackable_value")

		metric, ok := metricMap[metricName]
		if !ok {
			return errors.Errorf("metric %s not found", metricName)
		}

		// Scan the next metric value, if error then return
		scanner.Scan()
		if err := scanner.Err(); err != nil {
			return err
		}
		line = scanner.Text()
		matches := openmetricsLineRegex.FindStringSubmatch(line)
		if matches == nil {
			return errors.New("error parsing metric line")
		}
		value, err := strconv.ParseFloat(matches[3], 64)
		if err != nil {
			return err
		}
		metric.HighestTrackableValue = MetricPoint(value)
	}
	return nil
}

func processMetricTypeLine(
	line string, metricMap map[string]*HistogramSummaryMetric,
) (*HistogramSummaryMetric, error) {
	parts := strings.Fields(line)
	if len(parts) >= 3 {
		// # TYPE <metric_name> summary
		metricName := parts[2]

		metric, ok := metricMap[metricName]
		if !ok {
			metric = &HistogramSummaryMetric{
				Name:                  metricName,
				Values:                []*HistogramSummaryMetricPoint{},
				HighestTrackableValue: 0,
			}
			metricMap[metricName] = metric
		}

		return metric, nil
	}
	return nil, errors.New("error parsing metric line")
}

func processMetricLine(line string, metric *HistogramSummaryMetricPoint) ([]*Label, error) {
	// <metric_name>{<label_key=label_value>} <value> <timestamp>
	matches := openmetricsLineRegex.FindStringSubmatch(line)
	if matches == nil {
		return nil, errors.New("error parsing metric line")
	}

	labels, err := getLabels(matches[2])
	if err != nil {
		return nil, err
	}

	metricName := matches[1]
	value, err := strconv.ParseFloat(matches[3], 64)
	if err != nil {
		return nil, err
	}
	timestampFloat, err := strconv.ParseFloat(matches[4], 64)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp in OpenMetrics line: %s", line)
	}
	metric.Timestamp = timeutil.Unix(int64(timestampFloat), int64((timestampFloat-float64(int64(timestampFloat)))*1e9))
	switch {
	case strings.HasSuffix(metricName, "count"):
		metric.Count = MetricPoint(value)
	case strings.HasSuffix(metricName, "elapsed"):
		metric.Elapsed = MetricPoint(value)
	case strings.HasSuffix(metricName, "mean"):
		metric.Mean = MetricPoint(value)
	case strings.HasSuffix(metricName, "max"):
		metric.Max = MetricPoint(value)
	case strings.HasSuffix(metricName, "sum"):
	default:
		quantile, err := getQuantile(labels)
		if err != nil {
			return nil, err
		}
		switch quantile {
		case 1.0:
			metric.P100 = MetricPoint(value)
		case 0.99:
			metric.P99 = MetricPoint(value)
		case 0.95:
			metric.P95 = MetricPoint(value)
		case 0.5:
			metric.P50 = MetricPoint(value)
		}
	}

	return labels, nil
}

func getLabels(labels string) ([]*Label, error) {
	labelSlice := strings.Split(labels, ",")
	var finalLabels []*Label

	for _, label := range labelSlice {
		labelKeyValue := strings.SplitN(label, "=", 2)
		if len(labelKeyValue) != 2 {
			return nil, errors.New("invalid label format")
		}

		finalLabels = append(finalLabels, &Label{
			Name:  strings.Trim(strings.TrimSpace(labelKeyValue[0]), "\""),
			Value: strings.Trim(strings.TrimSpace(labelKeyValue[1]), "\""),
		})
	}

	if len(finalLabels) == 0 {
		return nil, errors.New("no valid labels found")
	}
	return finalLabels, nil
}

func getQuantile(labels []*Label) (float64, error) {
	for _, label := range labels {
		if label.Name == "quantile" {
			return strconv.ParseFloat(strings.TrimSpace(label.Value), 64)
		}
	}
	return 0.0, nil
}

// GetOpenmetricsLabelString creates a string that follows the openmetrics labels format
func GetOpenmetricsLabelString(t test.Test, c cluster.Cluster, labels map[string]string) string {
	return util.LabelMapToString(GetOpenmetricsLabelMap(t, c, labels))
}

// GetOpenmetricsLabelMap creates a map of label keys and values
// It takes roachtest parameters and create relevant labels.
func GetOpenmetricsLabelMap(
	t test.Test, c cluster.Cluster, labels map[string]string,
) map[string]string {
	defaultMap := map[string]string{
		"test-run-id": t.GetRunId(),
		"cloud":       c.Cloud().String(),
		"owner":       t.Owner(),
		"test":        t.Name(),
	}

	if roachtestflags.OpenmetricsLabels != "" {
		roachtestLabelMap, err := GetOpenmetricsLabelsFromString(roachtestflags.OpenmetricsLabels)
		if err == nil {
			maps.Copy(defaultMap, roachtestLabelMap)
		}
	}

	// If the tests have passed some custom labels, copy them to the map created above
	if labels != nil {
		maps.Copy(defaultMap, labels)
	}
	return defaultMap
}

func GetOpenmetricsGaugeType(metricName string) string {
	return fmt.Sprintf("# TYPE %s gauge\n", util.SanitizeMetricName(metricName))
}

func GetOpenmetricsLabelsFromString(labelString string) (map[string]string, error) {
	labelValues := strings.Split(labelString, ",")
	labels := make(map[string]string)
	for _, label := range labelValues {
		parts := strings.SplitN(label, "=", 2)
		if len(parts) != 2 {
			return nil, errors.Errorf("invalid histogram label %q", label)
		}
		labels[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}

	return labels, nil
}
