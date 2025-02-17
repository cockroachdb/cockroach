// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/util"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram/exporter"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"
)

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
