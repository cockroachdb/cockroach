// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package humanizeutil_test

import (
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestHumanizeBytes verifies both IBytes and ParseBytes.
func TestBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		value       int64
		exp         string
		expNeg      string
		parseExp    int64
		parseErr    string
		parseErrNeg string
	}{
		{0, "0 B", "0 B", 0, "", ""},
		{1024, "1.0 KiB", "-1.0 KiB", 1024, "", ""},
		{1024 << 10, "1.0 MiB", "-1.0 MiB", 1024 << 10, "", ""},
		{1024 << 20, "1.0 GiB", "-1.0 GiB", 1024 << 20, "", ""},
		{1024 << 30, "1.0 TiB", "-1.0 TiB", 1024 << 30, "", ""},
		{1024 << 40, "1.0 PiB", "-1.0 PiB", 1024 << 40, "", ""},
		{1024 << 50, "1.0 EiB", "-1.0 EiB", 1024 << 50, "", ""},
		{int64(math.MaxInt64), "8.0 EiB", "-8.0 EiB", 0, "too large: 8.0 EiB", "too large: -8.0 EiB"},
	}

	for i, testCase := range testCases {
		// Test IBytes.
		if actual := humanizeutil.IBytes(testCase.value); actual != testCase.exp {
			t.Errorf("%d: IBytes(%d) actual:%s does not match expected:%s", i, testCase.value, actual, testCase.exp)
		}
		// Test negative IBytes.
		if actual := humanizeutil.IBytes(-testCase.value); actual != testCase.expNeg {
			t.Errorf("%d: IBytes(%d) actual:%s does not match expected:%s", i, -testCase.value, actual,
				testCase.expNeg)
		}
		// Test ParseBytes.
		if actual, err := humanizeutil.ParseBytes(testCase.exp); err != nil {
			if len(testCase.parseErr) > 0 {
				if testCase.parseErr != err.Error() {
					t.Errorf("%d: ParseBytes(%s) caused an incorrect error actual:%s, expected:%s", i, testCase.exp,
						err, testCase.parseErr)
				}
			} else {
				t.Errorf("%d: ParseBytes(%s) caused an unexpected error:%s", i, testCase.exp, err)
			}
		} else if actual != testCase.parseExp {
			t.Errorf("%d: ParseBytes(%s) actual:%d does not match expected:%d", i, testCase.exp, actual,
				testCase.parseExp)
		}
		// Test negative ParseBytes.
		if actual, err := humanizeutil.ParseBytes(testCase.expNeg); err != nil {
			if len(testCase.parseErrNeg) > 0 {
				if testCase.parseErrNeg != err.Error() {
					t.Errorf("%d: ParseBytes(%s) caused an incorrect error actual:%s, expected:%s", i, testCase.expNeg,
						err, testCase.parseErrNeg)
				}
			} else {
				t.Errorf("%d: ParseBytes(%s) caused an unexpected error:%s", i, testCase.expNeg, err)
			}
		} else if actual != -testCase.parseExp {
			t.Errorf("%d: ParseBytes(%s) actual:%d does not match expected:%d", i, testCase.expNeg, actual,
				-testCase.parseExp)
		}
	}

	// Some extra error cases for good measure.
	testFailCases := []struct {
		value    string
		expected string
	}{
		{"", "parsing \"\": invalid syntax"},   // our error
		{"1 ZB", "unhandled size name: zb"},    // humanize's error
		{"-1 ZB", "unhandled size name: zb"},   // humanize's error
		{"1 ZiB", "unhandled size name: zib"},  // humanize's error
		{"-1 ZiB", "unhandled size name: zib"}, // humanize's error
		{"100 EiB", "too large: 100 EiB"},      // humanize's error
		{"-100 EiB", "too large: 100 EiB"},     // humanize's error
		{"10 EiB", "too large: 10 EiB"},        // our error
		{"-10 EiB", "too large: -10 EiB"},      // our error
	}
	for i, testCase := range testFailCases {
		if _, err := humanizeutil.ParseBytes(testCase.value); err.Error() != testCase.expected {
			t.Errorf("%d: ParseBytes(%s) caused an incorrect error actual:%s, expected:%s", i, testCase.value, err,
				testCase.expected)
		}
	}
}
