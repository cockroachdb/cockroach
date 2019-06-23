// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestWrapNumbersAsSafe tests the wrapNumbersAsSafe through ReportablesToSafeError.
func TestWrapNumbersAsSafe(t *testing.T) {
	defer leaktest.AfterTest(t)()

	reportables := []interface{}{
		uint(math.MaxUint32),
		uint8(math.MaxUint8),
		uint16(math.MaxUint16),
		uint32(math.MaxUint32),
		uint64(math.MaxUint64),

		int(math.MaxInt32),
		int8(math.MaxInt8),
		int16(math.MaxInt16),
		int32(math.MaxInt32),
		int64(math.MaxInt64),

		float32(math.MaxFloat32),
		float64(math.MaxFloat64),

		"unsafe-string",
		"123",
	}

	wrapNumbersAsSafe(reportables...)
	format := "some reportables"
	err := log.ReportablesToSafeError(0, format, reportables)

	expectedReportables := []string{
		fmt.Sprint(uint(math.MaxUint32)),
		fmt.Sprint(uint8(math.MaxUint8)),
		fmt.Sprint(uint16(math.MaxUint16)),
		fmt.Sprint(uint32(math.MaxUint32)),
		fmt.Sprint(uint64(math.MaxUint64)),

		fmt.Sprint(int(math.MaxInt32)),
		fmt.Sprint(int8(math.MaxInt8)),
		fmt.Sprint(int16(math.MaxInt16)),
		fmt.Sprint(int32(math.MaxInt32)),
		fmt.Sprint(int64(math.MaxInt64)),

		fmt.Sprint(float32(math.MaxFloat32)),
		fmt.Sprint(float64(math.MaxFloat64)),

		"string",
		"string",
	}

	expectedErrorStr := fmt.Sprintf("%s:%d: %s%s%s", "?", 0, format, " | ", strings.Join(expectedReportables, "; "))

	if expectedErrorStr != err.Error() {
		t.Fatalf("expected error to be %s but was %s", expectedErrorStr, err.Error())
	}

}
