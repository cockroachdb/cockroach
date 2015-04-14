// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Tracy (matt.r.tracy@gmail.com)

package ts

import (
	"bytes"
	"reflect"
	"testing"
)

func TestDataKeys(t *testing.T) {
	testCases := []struct {
		name        string
		source      string
		timestamp   int64
		resolution  Resolution
		expectedLen int
	}{
		{
			"test.metric",
			"testsource",
			0,
			Resolution10s,
			30,
		},
		{
			"test.no.source",
			"",
			1429114700000000000,
			Resolution10s,
			26,
		},
		{
			"",
			"",
			-1429114700000000000,
			Resolution10s,
			12,
		},
	}

	for i, tc := range testCases {
		encoded := MakeDataKey(tc.name, tc.source, tc.resolution, tc.timestamp)
		if !bytes.HasPrefix(encoded, keyDataPrefix) {
			t.Errorf("case %d, encoded key %v did not have time series data prefix", i, encoded)
		}
		if a, e := len(encoded), tc.expectedLen; a != e {
			t.Errorf("case %d, encoded length %d did not match expected %d", i, a, e)
		}

		// Normalize timestamp of test case; we expect MakeDataKey to
		// automatically truncate it to an exact multiple of the Resolution's
		// KeyDuration
		tc.timestamp = (tc.timestamp / tc.resolution.KeyDuration()) * tc.resolution.KeyDuration()

		d := tc
		d.name, d.source, d.resolution, d.timestamp = DecodeDataKey(encoded)
		if !reflect.DeepEqual(d, tc) {
			t.Errorf("case %d, decoded values %v did not match expected %v", i, d, tc)
		}
	}
}
