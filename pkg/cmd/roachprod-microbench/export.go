// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package main

import (
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/model"
	"golang.org/x/perf/benchfmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func exportMetrics(dir string, writer io.Writer, timestamp string, labels map[string]string) error {
	packages, err := getPackagesFromLogs(dir)
	if err != nil {
		return err
	}

	// Construct the metric map from the logs.
	runID := "current"
	builder := model.NewBuilder()
	for _, pkg := range packages {
		path := filepath.Join(dir, getReportLogName(reportLogName, pkg))
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		reader := benchfmt.NewReader(file, path)
		err = builder.AddMetrics(runID, pkg+"/", reader)
		if err != nil {
			return err
		}
	}

	// Write metrics.
	metricMap := builder.ComputeMetricMap()
	labelsString := labelMapToString(labels)
	for _, metric := range metricMap {
		for benchmarkName, entry := range metric.BenchmarkEntries {
			summary := entry.Summaries[runID]
			fmt.Fprintf(writer, "%s_%s{%s} %f %s\n",
				sanitizeMetricLabel(benchmarkName),
				sanitizeMetricLabel(metric.Name),
				labelsString,
				summary.Center,
				timestamp,
			)
		}
	}

	return nil
}

func labelMapToString(labels map[string]string) string {
	var builder strings.Builder
	for key, value := range labels {
		if len(builder.String()) > 0 {
			builder.WriteString(",")
		}
		builder.WriteString(sanitizeMetricLabel(key))
		builder.WriteString("=\"")
		builder.WriteString(value)
		builder.WriteString("\"")
	}
	return builder.String()
}

func sanitizeMetricLabel(name string) string {
	return strings.ReplaceAll(name, "/", "_")
}
