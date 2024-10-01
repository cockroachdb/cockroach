// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/model"
	"golang.org/x/exp/maps"
	"golang.org/x/perf/benchfmt"
)

func exportMetrics(
	dir string, writer io.Writer, timestamp time.Time, labels map[string]string,
) error {
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
			fmt.Fprintf(writer, "%s_%s{%s} %f %d\n",
				sanitize(benchmarkName),
				sanitize(metric.Name),
				labelsString,
				summary.Center,
				timestamp.Unix(),
			)
		}
	}
	return nil
}

func labelMapToString(labels map[string]string) string {
	var builder strings.Builder
	keys := maps.Keys(labels)
	sort.Strings(keys)
	for _, key := range keys {
		value := labels[key]
		if len(builder.String()) > 0 {
			builder.WriteString(",")
		}
		builder.WriteString(sanitize(key))
		builder.WriteString("=\"")
		builder.WriteString(sanitize(value))
		builder.WriteString("\"")
	}
	return builder.String()
}

// sanitize replaces all invalid characters for metric labels with an
// underscore. The first character must be a letter or underscore or else it
// will also be replaced with an underscore.
func sanitize(input string) string {
	regex := regexp.MustCompile(`[^a-zA-Z0-9_]`)
	sanitized := regex.ReplaceAllString(input, "_")
	firstCharRegex := regexp.MustCompile(`^[^a-zA-Z_]`)
	sanitized = firstCharRegex.ReplaceAllString(sanitized, "_")
	return sanitized
}
