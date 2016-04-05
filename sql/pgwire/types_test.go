// Copyright 2016 The Cockroach Authors.
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
// permissions and limitations under the License.
//
// Author: Dan Harrison (daniel.harrison@gmail.com)

package pgwire

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util/leaktest"
)

// The assertions in this test should also be caught by the integration tests on
// various drivers.
func TestParseTs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var parseTsTests = []struct {
		strTimestamp string
		expected     time.Time
	}{
		// time.RFC3339Nano for github.com/lib/pq.
		{"2006-07-08T00:00:00.000000123Z", time.Date(2006, 7, 8, 0, 0, 0, 123, time.FixedZone("UTC", 0))},

		// The format accepted by pq.ParseTimestamp.
		{"2001-02-03 04:05:06.123-07", time.Date(2001, time.February, 3, 4, 5, 6, 123000000, time.FixedZone("", -7*60*60))},
	}

	for i, test := range parseTsTests {
		parsed, err := parseTs(test.strTimestamp)
		if err != nil {
			t.Errorf("%d could not parse [%s]: %v", i, test.strTimestamp, err)
			continue
		}
		if !parsed.Equal(test.expected) {
			t.Errorf("%d parsing [%s] got [%s] expected [%s]", i, test.strTimestamp, parsed, test.expected)
		}
	}
}

func TestTimestampRoundtrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts := time.Date(2006, 7, 8, 0, 0, 0, 123, time.FixedZone("UTC", 0))

	encoded := string(formatTs(ts))
	decoded, err := parseTs(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !ts.Equal(decoded) {
		t.Fatalf("timestamp did not roundtrip got [%s] expected [%s]", decoded, ts)
	}
}
