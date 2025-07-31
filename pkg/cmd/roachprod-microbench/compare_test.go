// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bytes"
	"fmt"
	"path"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/model"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
)

func metricsToText(metricMaps map[string]*model.MetricMap) string {
	buf := new(bytes.Buffer)
	packages := maps.Keys(metricMaps)
	sort.Strings(packages)

	for _, pkg := range packages {
		fmt.Fprintf(buf, "Package %s\n", pkg)
		metricMap := metricMaps[pkg]
		metricKeys := maps.Keys(*metricMap)
		sort.Strings(metricKeys)

		for _, metricKey := range metricKeys {
			fmt.Fprintf(buf, "Metric %s\n", metricKey)
			metric := (*metricMap)[metricKey]
			entryKeys := maps.Keys(metric.BenchmarkEntries)
			sort.Strings(entryKeys)

			for _, entryKey := range entryKeys {
				entry := metric.BenchmarkEntries[entryKey]
				centers := make([]float64, len(entry.Summaries))
				summaryKeys := maps.Keys(entry.Summaries)
				sort.Strings(summaryKeys)
				for i, key := range summaryKeys {
					centers[i] = entry.Summaries[key].Center
				}
				comparison := metric.ComputeComparison(entryKey, "baseline", "experiment")
				fmt.Fprintf(buf, "BenchmarkEntry %s %s %v %s\n",
					entryKey, comparison.FormattedDelta, centers, comparison.Distribution.String(),
				)
			}
		}
	}
	return buf.String()
}

func TestCompareBenchmarks(t *testing.T) {
	ddFilePath := path.Join(datapathutils.TestDataPath(t), "compare")
	datadriven.RunTest(t, ddFilePath, func(t *testing.T, d *datadriven.TestData) string {
		if d.Cmd != "compare" {
			d.Fatalf(t, "unknown command %s", d.Cmd)
		}
		oldDir := datapathutils.TestDataPath(t, "reports", d.CmdArgs[0].String())
		newDir := datapathutils.TestDataPath(t, "reports", d.CmdArgs[1].String())
		packages, err := getPackagesFromLogs(oldDir)
		require.NoError(t, err)
		c := &compare{
			compareConfig: compareConfig{
				experimentDir: newDir,
				baselineDir:   oldDir,
			},
			packages: packages,
		}
		metricMaps, err := c.readMetrics()
		require.NoError(t, err)
		return metricsToText(metricMaps)
	})
}
