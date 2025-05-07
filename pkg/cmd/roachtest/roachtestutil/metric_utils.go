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
	// AdditionalLabels are used if any additional labels need to be added.
	// any label added here will override the global labels imported from the stats file
	AdditionalLabels []*Label
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
	// TotalCount is the sum of the counts of the observation
	TotalCount int64
	// TotalElapsed is the total elapsed time in milliseconds of the run
	TotalElapsed int64
	// Labels is the list of labels of the current histogram
	Labels []*Label
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
	// Elapsed is the average elapsed time of the whole run
	Elapsed MetricPoint
}

// GetWorkloadHistogramString creates a histogram flag string based on the roachtest to pass to workload binary
// This is used to make use of t.ExportOpenmetrics() method and create appropriate exporter
func GetWorkloadHistogramString(
	t test.Test, c cluster.Cluster, labels map[string]string, disableTempFile bool,
) string {
	var histogramArgs string
	if t.ExportOpenmetrics() {
		// Add openmetrics related labels and arguments
		histogramArgs = fmt.Sprintf(" --histogram-export-format='openmetrics' --histograms=%s/%s --openmetrics-labels='%s'",
			t.PerfArtifactsDir(), GetBenchmarkMetricsFileName(t), GetOpenmetricsLabelString(t, c, labels))
	} else {
		// Since default is json, no need to add --histogram-export-format flag in this case and also the labels
		histogramArgs = fmt.Sprintf(" --histograms=%s/%s", t.PerfArtifactsDir(), GetBenchmarkMetricsFileName(t))
	}

	if disableTempFile {
		histogramArgs += " --disable-temp-hist-file"
	}

	return histogramArgs
}

// GetWorkloadHistogramArgs makes disableTempFile false
func GetWorkloadHistogramArgs(t test.Test, c cluster.Cluster, labels map[string]string) string {
	return GetWorkloadHistogramString(t, c, labels, false)
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

// PostProcessMetrics is used to first convert the perfBuf into prometheus metric family,
// call the processFunction passed and then returns a byte buffer which has the metrics returned by process function
// and the initial metric bytes
func PostProcessMetrics(
	test string,
	processFunction func(string, *HistogramMetric) (AggregatedPerfMetrics, error),
	histogramMetrics *HistogramMetric,
) (AggregatedPerfMetrics, error) {
	if processFunction == nil {
		return nil, errors.New("process function is nil")
	}

	// Call the process function
	processedMetrics, err := processFunction(test, histogramMetrics)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to post process metrics")
	}

	return processedMetrics, nil
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

	labelMap := make(map[string]string)
	for _, label := range labels {
		labelMap[label.Name] = label.Value
	}
	timeMillis := timestamp.UnixMilli()
	for _, metric := range aggregatedMetrics {
		metricName := util.SanitizeMetricName(metric.Name)
		metricValue := float64(metric.Value)
		currentLabels := getAggregateLabels(labelMap, metric)
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

func getAggregateLabels(labelMap map[string]string, metric *AggregatedMetric) []*dto.LabelPair {
	aggregateMetricLabelMap := maps.Clone(labelMap)

	// we want to override the global labels with the aggregated metric labels
	for _, label := range metric.AdditionalLabels {
		aggregateMetricLabelMap[label.Name] = label.Value
	}

	var labels []*dto.LabelPair
	for key, val := range aggregateMetricLabelMap {
		labelName := util.SanitizeKey(key)
		// In case the label value already has surrounding quotes, we should trim them
		labelValue := util.SanitizeValue(strings.Trim(val, `"`))
		// remove empty labels
		if labelValue == "" {
			continue
		}
		labels = append(labels, &dto.LabelPair{
			Name:  &labelName,
			Value: &labelValue,
		})
	}

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

func GetHistogramMetrics(perfBuf *bytes.Buffer) (*HistogramMetric, []*Label, error) {
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
			currentMetric.Labels = labels
		}
		if err != nil {
			return nil, nil, err
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, nil, errors.Wrapf(err, "failed to scan metrics")
	}

	var summaries []*HistogramSummaryMetric

	totalElapsed := int64(0)
	for _, metric := range metricMap {
		count := int64(0)
		elapsed := int64(0)
		for _, value := range metric.Values {
			count += int64(value.Count)
			elapsed += int64(value.Elapsed)
		}
		metric.TotalCount = count
		metric.TotalElapsed = elapsed
		totalElapsed += metric.TotalElapsed
		summaries = append(summaries, metric)
	}
	// Take average of the elapsed time
	if len(summaries) != 0 {
		totalElapsed /= int64(len(summaries))
	}
	return &HistogramMetric{
		Summaries: summaries,
		Elapsed:   MetricPoint(totalElapsed),
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

	labels, err := GetLabels(matches[2])
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

func GetLabels(labels string) ([]*Label, error) {
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
		"owner":       t.Owner(),
		"test":        t.Name(),
	}

	// With --cluster flag, the cloud field in cluster spec is not populated.
	// To avoid panic, we check and only then add the cloud label
	// Since --cluster is not used in nightlies, this is not an issue.
	if cloud := c.Cloud(); cloud.IsSet() {
		defaultMap["cloud"] = cloud.String()
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
