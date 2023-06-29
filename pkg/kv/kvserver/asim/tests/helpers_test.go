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
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
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
