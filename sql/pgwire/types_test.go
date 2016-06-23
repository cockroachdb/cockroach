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

	"github.com/cockroachdb/pq/oid"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/metric"
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
	ts := time.Date(2006, 7, 8, 0, 0, 0, 123000, time.FixedZone("UTC", 0))

	parse := func(encoded []byte) time.Time {
		decoded, err := parseTs(string(encoded))
		if err != nil {
			t.Fatal(err)
		}
		return decoded.UTC()
	}

	if actual := parse(formatTs(ts, nil)); !ts.Equal(actual) {
		t.Fatalf("timestamp did not roundtrip got [%s] expected [%s]", actual, ts)
	}

	// Also check with a 0, positive, and negative offset.
	CET := time.FixedZone("Europe/Paris", 0)
	EST := time.FixedZone("America/New_York", 0)

	for _, tz := range []*time.Location{time.UTC, CET, EST} {
		if actual := parse(formatTs(ts, tz)); !ts.Equal(actual) {
			t.Fatalf("[%s]: timestamp did not roundtrip got [%s] expected [%s]", tz, actual, ts)
		}
	}
}

func BenchmarkWriteBinaryDecimal(b *testing.B) {
	buf := writeBuffer{bytecount: metric.NewCounter()}

	dec := new(parser.DDecimal)
	dec.SetString("-1728718718271827121233.1212121212")

	// Warm up the buffer.
	buf.writeBinaryDatum(dec)
	buf.wrapped.Reset()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		buf.writeBinaryDatum(dec)
		b.StopTimer()
		buf.wrapped.Reset()
	}
}

func BenchmarkDecodeBinaryDecimal(b *testing.B) {
	wbuf := writeBuffer{bytecount: metric.NewCounter()}

	expected := new(parser.DDecimal)
	expected.SetString("-1728718718271827121233.1212121212")
	wbuf.writeBinaryDatum(expected)

	rbuf := readBuffer{msg: wbuf.wrapped.Bytes()}

	plen, err := rbuf.getUint32()
	if err != nil {
		b.Fatal(err)
	}
	bytes, err := rbuf.getBytes(int(plen))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		got, err := decodeOidDatum(oid.T_numeric, formatBinary, bytes)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		} else if got.Compare(expected) != 0 {
			b.Fatalf("expected %s, got %s", expected, got)
		}
	}
}
