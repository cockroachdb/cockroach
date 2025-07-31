// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/assertion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/gen"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func getNodeLivenessStatus(s string) livenesspb.NodeLivenessStatus {
	switch s {
	case "unknown":
		return livenesspb.NodeLivenessStatus_UNKNOWN
	case "dead":
		return livenesspb.NodeLivenessStatus_DEAD
	case "unavailable":
		return livenesspb.NodeLivenessStatus_UNAVAILABLE
	case "live":
		return livenesspb.NodeLivenessStatus_LIVE
	case "decommissioning":
		return livenesspb.NodeLivenessStatus_DECOMMISSIONING
	case "decommissioned":
		return livenesspb.NodeLivenessStatus_DECOMMISSIONED
	case "draining":
		return livenesspb.NodeLivenessStatus_DRAINING
	default:
		panic(fmt.Sprintf("unkown liveness status: %s", s))
	}
}

func scanArg(t *testing.T, d *datadriven.TestData, key string, dest interface{}) {
	var tmp string
	switch dest := dest.(type) {
	case *string, *int, *int64, *uint64, *bool, *time.Duration, *float64, *[]int, *[]float64:
		d.ScanArgs(t, key, dest)
	case *OutputFlags:
		var flagsTmp []string
		d.ScanArgs(t, key, &flagsTmp)
		*dest = dest.ScanFlags(flagsTmp)
	case *gen.PlacementType:
		d.ScanArgs(t, key, &tmp)
		*dest = gen.GetRangePlacementType(tmp)
	case *generatorType:
		d.ScanArgs(t, key, &tmp)
		*dest = getGeneratorType(tmp)
	case *clusterConfigType:
		d.ScanArgs(t, key, &tmp)
		*dest = getClusterConfigType(tmp)
	case *livenesspb.NodeLivenessStatus:
		d.ScanArgs(t, key, &tmp)
		*dest = getNodeLivenessStatus(tmp)
	case *eventSeriesType:
		d.ScanArgs(t, key, &tmp)
		*dest = getEventSeriesType(tmp)
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
func scanThreshold(t *testing.T, d *datadriven.TestData) (th assertion.Threshold) {
	if scanIfExists(t, d, "exact_bound", &th.Value) {
		th.ThresholdType = assertion.ExactBound
		return th
	}
	if scanIfExists(t, d, "upper_bound", &th.Value) {
		th.ThresholdType = assertion.UpperBound
		return th
	}
	scanArg(t, d, "lower_bound", &th.Value)
	th.ThresholdType = assertion.LowerBound
	return th
}
