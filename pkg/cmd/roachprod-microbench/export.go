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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/model"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/util"
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
		//nolint:deferloop TODO(#137605)
		defer file.Close()
		reader := benchfmt.NewReader(file, path)
		err = builder.AddMetrics(runID, pkg+"/", reader)
		if err != nil {
			return err
		}
	}

	// Write metrics.
	metricMap := builder.ComputeMetricMap()
	labelsString := util.LabelMapToString(labels)
	for _, metric := range metricMap {
		for benchmarkName, entry := range metric.BenchmarkEntries {
			summary := entry.Summaries[runID]
			fmt.Fprintf(writer, "%s_%s{%s} %f %d\n",
				util.SanitizeKey(benchmarkName),
				util.SanitizeKey(metric.Name),
				labelsString,
				summary.Center,
				timestamp.Unix(),
			)
		}
	}
	return nil
}
