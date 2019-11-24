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
		os int64
		ts string
	}{
		{0, `GMT+00:00`},
		{-3600, `UTC-1:00:00`},
		{57540, `UTC+15:59`},
		{-57599, `GMT-15:59:59`},
		{-25140, `GMT-06:59`},
		{21600, `GMT+6:00`},
		{18000, `GMT+5`},
		{-57600, `GMT-16`},
		{0, `GMT-15:59:5Z`},
		{0, `GMT+6:00:0`},
		{0, `GMT+16:59`},
		{0, `GMT+6:0:59`},
		{0, `GMT+04:4:59`},
		{0, `GMT-123:59`},
	}

	for i, testCase := range testCases {
		if o, _ := TimeZoneOffsetStringConversion(testCase.ts); o != testCase.os {
			t.Errorf("%d: Expected %d for time %s, but got %v", i, testCase.os, testCase.ts, o)
		}
	}
}
