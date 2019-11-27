// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package timeutil

import "testing"

func TestTimeZoneOffsetStringConversion(t *testing.T) {
	testCases := []struct {
		timezone   string
		offsetSecs int64
		ok         bool
	}{
		{`GMT+00:00`, 0, true},
		{`UTC-1:00:00`, -3600, true},
		{`UTC-1:0:00`, -3600, true},
		{`UTC+15:59:0`, 57540, true},
		{` GMT +167:59:00  `, 604740, true},
		{`GMT-15:59:59`, -57599, true},
		{`GMT-06:59`, -25140, true},
		{`GMT+167:59:00`, 604740, true},
		{`GMT+ 167: 59:0`, 604740, true},
		{`GMT+5`, 18000, true},
		{`UTC+5:9`, 5*60*60 + 9*60, true},
		{`UTC-5:9:1`, -(5*60*60 + 9*60 + 1), true},
		{`GMT-15:59:5Z`, 0, false},
		{`GMT-15:99:1`, 0, false},
		{`GMT+6:`, 0, false},
		{`GMT-6:00:`, 0, false},
		{`GMT+168:00:00`, 0, false},
		{`GMT-170:00:59`, 0, false},
	}

	for i, testCase := range testCases {
		offset, ok := TimeZoneOffsetStringConversion(testCase.timezone)
		if offset != testCase.offsetSecs || ok != testCase.ok {
			t.Errorf("%d: Expected offset: %d, success: %v for time %s, but got offset: %d, success: %v",
				i, testCase.offsetSecs, testCase.ok, testCase.timezone, offset, ok)
		}
	}
}
