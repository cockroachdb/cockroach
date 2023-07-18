// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/metrics"
	"github.com/cockroachdb/datadriven"
	"github.com/guptarohit/asciigraph"
	"github.com/stretchr/testify/require"
)

// TODO(kvoli): Upstream the scan implementations for the float64 and
// time.Duration types to the datadriven testing repository.
func scanArg(t *testing.T, d *datadriven.TestData, key string, dest interface{}) {
	var tmp string
	var err error
	switch dest := dest.(type) {
	case *time.Duration:
		d.ScanArgs(t, key, &tmp)
		*dest, err = time.ParseDuration(tmp)
		require.NoError(t, err)
	case *float64:
		d.ScanArgs(t, key, &tmp)
		*dest, err = strconv.ParseFloat(tmp, 64)
		require.NoError(t, err)
	case *[]int, *string, *int, *int64, *uint64, *bool:
		d.ScanArgs(t, key, dest)
	default:
		require.Fail(t, "unsupported type %T", dest)
	}
}

// scanIfExists looks up the first arg in CmdArgs array that matches the
// provided firstKey. If found, it scans the value into dest and returns true;
// Otherwise, it does nothing and returns false.
func scanIfExists(t *testing.T, d *datadriven.TestData, key string, dest interface{}) bool {
	if d.HasArg(key) {
		scanArg(t, d, key, dest)
		return true
	}
	return false
}

// scanThreshold looks up the first arg that matches with  "exact_bound",
// "upper_bound", or "lower_bound" in the CmdArgs array. If found, it creates a
// threshold struct from the located key-value pair. If no keys are found, a
// fatal error is triggered. Note that only one key should be specified at a
// time. If multiple keys are specified, the precedence order is exact_bound >
// upper_bound > lower_bound.
func scanThreshold(t *testing.T, d *datadriven.TestData) (th threshold) {
	if scanIfExists(t, d, "exact_bound", &th.value) {
		th.thresholdType = exactBound
		return th
	}
	if scanIfExists(t, d, "upper_bound", &th.value) {
		th.thresholdType = upperBound
		return th
	}
	scanArg(t, d, "lower_bound", &th.value)
	th.thresholdType = lowerBound
	return th
}

// loadClusterInfo creates a LoadedCluster from a matching ClusterInfo based on
// the given configNam, or panics if no match is found in existing
// configurations.
func loadClusterInfo(configName string) gen.LoadedCluster {
	clusterInfo := gen.GetClusterInfo(configName)
	return gen.LoadedCluster{
		Info: clusterInfo,
	}
}

// PlotAllHistory outputs stat plots for the provided asim history array into
// the given strings.Builder buf.
func plotAllHistory(runs []asim.History, buf *strings.Builder) {
	settings := defaultPlotSettings()
	stat, height, width := settings.stat, settings.height, settings.width
	for i := 0; i < len(runs); i++ {
		history := runs[i]
		ts := metrics.MakeTS(history.Recorded)
		statTS := ts[stat]
		buf.WriteString("\n")
		buf.WriteString(asciigraph.PlotMany(
			statTS,
			asciigraph.Caption(stat),
			asciigraph.Height(height),
			asciigraph.Width(width),
		))
		buf.WriteString("\n")
	}
}

// checkAssertions checks the given history and assertions, returning (bool,
// reason) indicating any failures and reasons if any assertions fail.
func checkAssertions(
	ctx context.Context, history asim.History, assertions []SimulationAssertion,
) (bool, string) {
	assertionFailures := []string{}
	failureExists := false
	for _, assertion := range assertions {
		if holds, reason := assertion.Assert(ctx, history); !holds {
			failureExists = true
			assertionFailures = append(assertionFailures, reason)
		}
	}
	if failureExists {
		return true, strings.Join(assertionFailures, "")
	}
	return false, ""
}
