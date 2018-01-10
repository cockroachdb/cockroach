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

package pgwire

import (
	"bytes"
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/lib/pq/oid"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
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
		parsed, err := tree.ParseDTimestamp(test.strTimestamp, time.Nanosecond)
		if err != nil {
			t.Errorf("%d could not parse [%s]: %v", i, test.strTimestamp, err)
			continue
		}
		if !parsed.Time.Equal(test.expected) {
			t.Errorf("%d parsing [%s] got [%s] expected [%s]", i, test.strTimestamp, parsed, test.expected)
		}
	}
}

func TestTimestampRoundtrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts := time.Date(2006, 7, 8, 0, 0, 0, 123000, time.FixedZone("UTC", 0))

	parse := func(encoded []byte) time.Time {
		decoded, err := tree.ParseDTimestamp(string(encoded), time.Nanosecond)
		if err != nil {
			t.Fatal(err)
		}
		return decoded.UTC()
	}

	if actual := parse(formatTs(ts, nil, nil)); !ts.Equal(actual) {
		t.Fatalf("timestamp did not roundtrip got [%s] expected [%s]", actual, ts)
	}

	// Also check with a 0, positive, and negative offset.
	CET := time.FixedZone("Europe/Paris", 0)
	EST := time.FixedZone("America/New_York", 0)

	for _, tz := range []*time.Location{time.UTC, CET, EST} {
		if actual := parse(formatTs(ts, tz, nil)); !ts.Equal(actual) {
			t.Fatalf("[%s]: timestamp did not roundtrip got [%s] expected [%s]", tz, actual, ts)
		}
	}
}

func TestWriteBinaryArray(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Regression test for #20372. Ensure that writing twice to the same
	// writeBuffer is equivalent to writing to two different writeBuffers and
	// then concatenating the result.
	ary, _ := tree.ParseDArrayFromString(tree.NewTestingEvalContext(), "{1}", coltypes.Int)

	writeBuf1 := newWriteBuffer()
	writeBuf1.writeTextDatum(context.Background(), ary, time.UTC)
	writeBuf1.writeBinaryDatum(context.Background(), ary, time.UTC)

	writeBuf2 := newWriteBuffer()
	writeBuf2.writeTextDatum(context.Background(), ary, time.UTC)

	writeBuf3 := newWriteBuffer()
	writeBuf3.writeBinaryDatum(context.Background(), ary, time.UTC)

	concatted := bytes.Join([][]byte{writeBuf2.wrapped.Bytes(), writeBuf3.wrapped.Bytes()}, nil)

	if !reflect.DeepEqual(writeBuf1.wrapped.Bytes(), concatted) {
		t.Fatalf("expected %v, got %v", concatted, writeBuf1.wrapped.Bytes())
	}
}

func TestIntArrayRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	buf := newWriteBuffer()
	buf.bytecount = metric.NewCounter(metric.Metadata{})
	d := tree.NewDArray(types.Int)
	for i := 0; i < 10; i++ {
		if err := d.Append(tree.NewDInt(tree.DInt(i))); err != nil {
			t.Fatal(err)
		}
	}

	buf.writeTextDatum(context.Background(), d, time.UTC)

	b := buf.wrapped.Bytes()

	got, err := decodeOidDatum(oid.T__int8, formatText, b[4:])
	if err != nil {
		t.Fatal(err)
	}
	evalCtx := tree.NewTestingEvalContext()
	defer evalCtx.Stop(context.Background())
	if got.Compare(evalCtx, d) != 0 {
		t.Fatalf("expected %s, got %s", d, got)
	}
}

func benchmarkWriteType(b *testing.B, d tree.Datum, format formatCode) {
	ctx := context.Background()

	buf := newWriteBuffer()
	buf.bytecount = metric.NewCounter(metric.Metadata{Name: ""})

	writeMethod := buf.writeTextDatum
	if format == formatBinary {
		writeMethod = buf.writeBinaryDatum
	}

	// Warm up the buffer.
	writeMethod(ctx, d, nil)
	buf.wrapped.Reset()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Starting and stopping the timer in each loop iteration causes this
		// to take much longer. See http://stackoverflow.com/a/37624250/3435257.
		// buf.wrapped.Reset() should be fast enough to be negligible.
		writeMethod(ctx, d, nil)
		buf.wrapped.Reset()
	}
}

func benchmarkWriteBool(b *testing.B, format formatCode) {
	benchmarkWriteType(b, tree.DBoolTrue, format)
}

func benchmarkWriteInt(b *testing.B, format formatCode) {
	benchmarkWriteType(b, tree.NewDInt(1234), format)
}

func benchmarkWriteFloat(b *testing.B, format formatCode) {
	benchmarkWriteType(b, tree.NewDFloat(12.34), format)
}

func benchmarkWriteDecimal(b *testing.B, format formatCode) {
	dec := new(tree.DDecimal)
	s := "-1728718718271827121233.1212121212"
	if err := dec.SetString(s); err != nil {
		b.Fatalf("could not set %q on decimal", format)
	}
	benchmarkWriteType(b, dec, formatText)
}

func benchmarkWriteBytes(b *testing.B, format formatCode) {
	benchmarkWriteType(b, tree.NewDBytes("testing"), format)
}

func benchmarkWriteString(b *testing.B, format formatCode) {
	benchmarkWriteType(b, tree.NewDString("testing"), format)
}

func benchmarkWriteDate(b *testing.B, format formatCode) {
	d, err := tree.ParseDDate("2010-09-28", time.UTC)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkWriteType(b, d, format)
}

func benchmarkWriteTimestamp(b *testing.B, format formatCode) {
	ts, err := tree.ParseDTimestamp("2010-09-28 12:00:00.1", time.Microsecond)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkWriteType(b, ts, format)
}

func benchmarkWriteTimestampTZ(b *testing.B, format formatCode) {
	tstz, err := tree.ParseDTimestampTZ("2010-09-28 12:00:00.1", time.UTC, time.Microsecond)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkWriteType(b, tstz, format)
}

func benchmarkWriteInterval(b *testing.B, format formatCode) {
	i, err := tree.ParseDInterval("PT12H2M")
	if err != nil {
		b.Fatal(err)
	}
	benchmarkWriteType(b, i, format)
}

func benchmarkWriteTuple(b *testing.B, format formatCode) {
	i := tree.NewDInt(1234)
	f := tree.NewDFloat(12.34)
	s := tree.NewDString("testing")
	t := tree.NewDTuple(i, f, s)
	benchmarkWriteType(b, t, format)
}

func benchmarkWriteArray(b *testing.B, format formatCode) {
	a := tree.NewDArray(types.Int)
	for i := 0; i < 3; i++ {
		if err := a.Append(tree.NewDInt(tree.DInt(1234))); err != nil {
			b.Fatal(err)
		}
	}
	benchmarkWriteType(b, a, format)
}

func BenchmarkWriteTextBool(b *testing.B) {
	benchmarkWriteBool(b, formatText)
}
func BenchmarkWriteBinaryBool(b *testing.B) {
	benchmarkWriteBool(b, formatBinary)
}

func BenchmarkWriteTextInt(b *testing.B) {
	benchmarkWriteInt(b, formatText)
}
func BenchmarkWriteBinaryInt(b *testing.B) {
	benchmarkWriteInt(b, formatBinary)
}

func BenchmarkWriteTextFloat(b *testing.B) {
	benchmarkWriteFloat(b, formatText)
}
func BenchmarkWriteBinaryFloat(b *testing.B) {
	benchmarkWriteFloat(b, formatBinary)
}

func BenchmarkWriteTextDecimal(b *testing.B) {
	benchmarkWriteDecimal(b, formatText)
}
func BenchmarkWriteBinaryDecimal(b *testing.B) {
	benchmarkWriteDecimal(b, formatBinary)
}

func BenchmarkWriteTextBytes(b *testing.B) {
	benchmarkWriteBytes(b, formatText)
}
func BenchmarkWriteBinaryBytes(b *testing.B) {
	benchmarkWriteBytes(b, formatBinary)
}

func BenchmarkWriteTextString(b *testing.B) {
	benchmarkWriteString(b, formatText)
}
func BenchmarkWriteBinaryString(b *testing.B) {
	benchmarkWriteString(b, formatBinary)
}

func BenchmarkWriteTextDate(b *testing.B) {
	benchmarkWriteDate(b, formatText)
}
func BenchmarkWriteBinaryDate(b *testing.B) {
	benchmarkWriteDate(b, formatBinary)
}

func BenchmarkWriteTextTimestamp(b *testing.B) {
	benchmarkWriteTimestamp(b, formatText)
}
func BenchmarkWriteBinaryTimestamp(b *testing.B) {
	benchmarkWriteTimestamp(b, formatBinary)
}

func BenchmarkWriteTextTimestampTZ(b *testing.B) {
	benchmarkWriteTimestampTZ(b, formatText)
}
func BenchmarkWriteBinaryTimestampTZ(b *testing.B) {
	benchmarkWriteTimestampTZ(b, formatBinary)
}

func BenchmarkWriteTextInterval(b *testing.B) {
	benchmarkWriteInterval(b, formatText)
}

func BenchmarkWriteTextTuple(b *testing.B) {
	benchmarkWriteTuple(b, formatText)
}

func BenchmarkWriteTextArray(b *testing.B) {
	benchmarkWriteArray(b, formatText)
}

func BenchmarkDecodeBinaryDecimal(b *testing.B) {
	wbuf := newWriteBuffer()
	wbuf.bytecount = metric.NewCounter(metric.Metadata{})

	expected := new(tree.DDecimal)
	s := "-1728718718271827121233.1212121212"
	if err := expected.SetString(s); err != nil {
		b.Fatalf("could not set %q on decimal", s)
	}
	wbuf.writeBinaryDatum(context.Background(), expected, nil /* sessionLoc */)

	rbuf := pgwirebase.ReadBuffer{Msg: wbuf.wrapped.Bytes()}

	plen, err := rbuf.GetUint32()
	if err != nil {
		b.Fatal(err)
	}
	bytes, err := rbuf.GetBytes(int(plen))
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		got, err := decodeOidDatum(oid.T_numeric, formatBinary, bytes)
		b.StopTimer()
		evalCtx := tree.NewTestingEvalContext()
		defer evalCtx.Stop(context.Background())
		if err != nil {
			b.Fatal(err)
		} else if got.Compare(evalCtx, expected) != 0 {
			b.Fatalf("expected %s, got %s", expected, got)
		}
	}
}
