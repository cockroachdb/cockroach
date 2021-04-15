// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgwire

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// The assertions in this test should also be caught by the integration tests on
// various drivers.
func TestParseTs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var parseTsTests = []struct {
		strTimestamp string
		expected     time.Time
	}{
		// time.RFC3339Nano for github.com/lib/pq.
		{"2006-07-08T00:00:00.000000123Z", time.Date(2006, 7, 8, 0, 0, 0, 123, time.FixedZone("UTC", 0))},

		// The format accepted by pq.ParseTimestamp.
		{"2001-02-03 04:05:06.123-07", time.Date(2001, time.February, 3, 4, 5, 6, 123000000, time.FixedZone("", 0))},
	}

	for i, test := range parseTsTests {
		parsed, _, err := tree.ParseDTimestamp(nil, test.strTimestamp, time.Nanosecond)
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
	defer log.Scope(t).Close(t)
	ts := time.Date(2006, 7, 8, 0, 0, 0, 123000, time.FixedZone("UTC", 0))

	parse := func(encoded []byte) time.Time {
		decoded, _, err := tree.ParseDTimestamp(nil, string(encoded), time.Nanosecond)
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
	defer log.Scope(t).Close(t)
	// Regression test for #20372. Ensure that writing twice to the same
	// writeBuffer is equivalent to writing to two different writeBuffers and
	// then concatenating the result.
	st := cluster.MakeTestingClusterSettings()
	ary, _, _ := tree.ParseDArrayFromString(tree.NewTestingEvalContext(st), "{1}", types.Int)

	defaultConv, defaultLoc := makeTestingConvCfg()

	writeBuf1 := newWriteBuffer(nil /* bytecount */)
	writeBuf1.writeTextDatum(context.Background(), ary, defaultConv, defaultLoc, nil /* t */)
	writeBuf1.writeBinaryDatum(context.Background(), ary, time.UTC, nil /* t */)

	writeBuf2 := newWriteBuffer(nil /* bytecount */)
	writeBuf2.writeTextDatum(context.Background(), ary, defaultConv, defaultLoc, nil /* t */)

	writeBuf3 := newWriteBuffer(nil /* bytecount */)
	writeBuf3.writeBinaryDatum(context.Background(), ary, defaultLoc, nil /* t */)

	concatted := bytes.Join([][]byte{writeBuf2.wrapped.Bytes(), writeBuf3.wrapped.Bytes()}, nil)

	if !reflect.DeepEqual(writeBuf1.wrapped.Bytes(), concatted) {
		t.Fatalf("expected %v, got %v", concatted, writeBuf1.wrapped.Bytes())
	}
}

func TestIntArrayRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	buf := newWriteBuffer(nil /* bytecount */)
	buf.bytecount = metric.NewCounter(metric.Metadata{})
	d := tree.NewDArray(types.Int)
	for i := 0; i < 10; i++ {
		if err := d.Append(tree.NewDInt(tree.DInt(i))); err != nil {
			t.Fatal(err)
		}
	}

	defaultConv, defaultLoc := makeTestingConvCfg()
	buf.writeTextDatum(context.Background(), d, defaultConv, defaultLoc, nil /* t */)

	b := buf.wrapped.Bytes()

	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	got, err := pgwirebase.DecodeDatum(evalCtx, types.IntArray, pgwirebase.FormatText, b[4:])
	if err != nil {
		t.Fatal(err)
	}
	if got.Compare(evalCtx, d) != 0 {
		t.Fatalf("expected %s, got %s", d, got)
	}
}

func TestFloatConversion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testData := []struct {
		val              float64
		extraFloatDigits int
		expected         string
	}{
		{123.4567890123456789, 0, "123.456789012346"},
		{123.4567890123456789, 1, "123.4567890123457"},
		{123.4567890123456789, 2, "123.45678901234568"},
		{123.4567890123456789, 3, "123.45678901234568"},
		{123.4567890123456789, 100, "123.45678901234568"}, // values above 3 clamp to work like 3
		{123.4567890123456789, -10, "123.46"},
		{123.4567890123456789, -13, "1.2e+02"},
		{123.4567890123456789, -15, "1e+02"},
		{123.4567890123456789, -100, "1e+02"}, // values below -15 clamp to work like -15
	}

	for _, test := range testData {
		t.Run(fmt.Sprintf("%g/%d", test.val, test.extraFloatDigits), func(t *testing.T) {
			buf := newWriteBuffer(nil /* bytecount */)
			buf.bytecount = metric.NewCounter(metric.Metadata{})

			defaultConv, defaultLoc := makeTestingConvCfg()
			defaultConv.ExtraFloatDigits = int32(test.extraFloatDigits)

			d := tree.NewDFloat(tree.DFloat(test.val))
			buf.writeTextDatum(context.Background(), d, defaultConv, defaultLoc, nil /* t */)
			b := buf.wrapped.Bytes()

			got := string(b[4:])
			if test.expected != got {
				t.Fatalf("got %q, expected %q", got, test.expected)
			}
		})
	}
}

func TestByteArrayRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng := rand.New(rand.NewSource(timeutil.Now().Unix()))
	randValues := make(tree.Datums, 0, 11)
	randValues = append(randValues, tree.NewDBytes(tree.DBytes("\x00abc\\\n")))
	for i := 0; i < 10; i++ {
		d := randgen.RandDatum(rng, types.Bytes, false /* nullOK */)
		randValues = append(randValues, d)
	}

	for _, be := range []sessiondatapb.BytesEncodeFormat{
		sessiondatapb.BytesEncodeHex,
		sessiondatapb.BytesEncodeEscape,
	} {
		t.Run(be.String(), func(t *testing.T) {
			for i, d := range randValues {
				t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
					t.Logf("byte array: %q", d.String())

					buf := newWriteBuffer(nil /* bytecount */)
					buf.bytecount = metric.NewCounter(metric.Metadata{})

					defaultConv, defaultLoc := makeTestingConvCfg()
					defaultConv.BytesEncodeFormat = be
					buf.writeTextDatum(context.Background(), d, defaultConv, defaultLoc, nil /* t */)
					b := buf.wrapped.Bytes()
					t.Logf("encoded: %v (%q)", b, b)

					evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
					defer evalCtx.Stop(context.Background())
					got, err := pgwirebase.DecodeDatum(evalCtx, types.Bytes, pgwirebase.FormatText, b[4:])
					if err != nil {
						t.Fatal(err)
					}
					if _, ok := got.(*tree.DBytes); !ok {
						t.Fatalf("parse does not return DBytes, got %T", got)
					}
					if got.Compare(evalCtx, d) != 0 {
						t.Fatalf("expected %s, got %s", d, got)
					}
				})
			}
		})
	}
}

func TestCanWriteAllDatums(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng := rand.New(rand.NewSource(timeutil.Now().Unix()))

	defaultConv, defaultLoc := makeTestingConvCfg()

	for _, typ := range types.Scalar {
		buf := newWriteBuffer(nil /* bytecount */)

		for i := 0; i < 10; i++ {
			d := randgen.RandDatum(rng, typ, true)

			buf.writeTextDatum(context.Background(), d, defaultConv, defaultLoc, typ)
			if buf.err != nil {
				t.Fatalf("got %s while attempting to write datum %s as text", buf.err, d)
			}

			buf.writeBinaryDatum(context.Background(), d, defaultLoc, d.ResolvedType())
			if buf.err != nil {
				t.Fatalf("got %s while attempting to write datum %s as binary", buf.err, d)
			}
		}
	}
}

func benchmarkWriteType(b *testing.B, d tree.Datum, format pgwirebase.FormatCode) {
	ctx := context.Background()

	buf := newWriteBuffer(nil /* bytecount */)
	buf.bytecount = metric.NewCounter(metric.Metadata{Name: ""})

	writeMethod := func(ctx context.Context, d tree.Datum, loc *time.Location) {
		defaultConv, _ := makeTestingConvCfg()
		buf.writeTextDatum(ctx, d, defaultConv, loc, d.ResolvedType())
	}
	if format == pgwirebase.FormatBinary {
		writeMethod = func(ctx context.Context, d tree.Datum, loc *time.Location) {
			buf.writeBinaryDatum(ctx, d, loc, d.ResolvedType())
		}
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
		if buf.err != nil {
			b.Fatal(buf.err)
		}
		buf.wrapped.Reset()
	}
}

func benchmarkWriteBool(b *testing.B, format pgwirebase.FormatCode) {
	benchmarkWriteType(b, tree.DBoolTrue, format)
}

func benchmarkWriteInt(b *testing.B, format pgwirebase.FormatCode) {
	benchmarkWriteType(b, tree.NewDInt(1234), format)
}

func benchmarkWriteFloat(b *testing.B, format pgwirebase.FormatCode) {
	benchmarkWriteType(b, tree.NewDFloat(12.34), format)
}

func benchmarkWriteDecimal(b *testing.B, format pgwirebase.FormatCode) {
	dec := new(tree.DDecimal)
	s := "-1728718718271827121233.1212121212"
	if err := dec.SetString(s); err != nil {
		b.Fatalf("could not set %q on decimal", format)
	}
	benchmarkWriteType(b, dec, pgwirebase.FormatText)
}

func benchmarkWriteBytes(b *testing.B, format pgwirebase.FormatCode) {
	benchmarkWriteType(b, tree.NewDBytes("testing"), format)
}

func benchmarkWriteUUID(b *testing.B, format pgwirebase.FormatCode) {
	u := uuid.MakeV4()
	benchmarkWriteType(b, tree.NewDUuid(tree.DUuid{UUID: u}), format)
}

func benchmarkWriteString(b *testing.B, format pgwirebase.FormatCode) {
	benchmarkWriteType(b, tree.NewDString("testing"), format)
}

func benchmarkWriteDate(b *testing.B, format pgwirebase.FormatCode) {
	d, _, err := tree.ParseDDate(nil, "2010-09-28")
	if err != nil {
		b.Fatal(err)
	}
	benchmarkWriteType(b, d, format)
}

func benchmarkWriteTimestamp(b *testing.B, format pgwirebase.FormatCode) {
	ts, _, err := tree.ParseDTimestamp(nil, "2010-09-28 12:00:00.1", time.Microsecond)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkWriteType(b, ts, format)
}

func benchmarkWriteTimestampTZ(b *testing.B, format pgwirebase.FormatCode) {
	tstz, _, err := tree.ParseDTimestampTZ(nil, "2010-09-28 12:00:00.1", time.Microsecond)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkWriteType(b, tstz, format)
}

func benchmarkWriteInterval(b *testing.B, format pgwirebase.FormatCode) {
	i, err := tree.ParseDInterval("PT12H2M")
	if err != nil {
		b.Fatal(err)
	}
	benchmarkWriteType(b, i, format)
}

func benchmarkWriteTuple(b *testing.B, format pgwirebase.FormatCode) {
	i := tree.NewDInt(1234)
	f := tree.NewDFloat(12.34)
	s := tree.NewDString("testing")
	typ := types.MakeTuple([]*types.T{types.Int, types.Float, types.String})
	t := tree.NewDTuple(typ, i, f, s)
	benchmarkWriteType(b, t, format)
}

func benchmarkWriteArray(b *testing.B, format pgwirebase.FormatCode) {
	a := tree.NewDArray(types.Int)
	for i := 0; i < 3; i++ {
		if err := a.Append(tree.NewDInt(tree.DInt(1234))); err != nil {
			b.Fatal(err)
		}
	}
	benchmarkWriteType(b, a, format)
}

func BenchmarkWriteTextBool(b *testing.B) {
	benchmarkWriteBool(b, pgwirebase.FormatText)
}
func BenchmarkWriteBinaryBool(b *testing.B) {
	benchmarkWriteBool(b, pgwirebase.FormatBinary)
}

func BenchmarkWriteTextInt(b *testing.B) {
	benchmarkWriteInt(b, pgwirebase.FormatText)
}
func BenchmarkWriteBinaryInt(b *testing.B) {
	benchmarkWriteInt(b, pgwirebase.FormatBinary)
}

func BenchmarkWriteTextFloat(b *testing.B) {
	benchmarkWriteFloat(b, pgwirebase.FormatText)
}
func BenchmarkWriteBinaryFloat(b *testing.B) {
	benchmarkWriteFloat(b, pgwirebase.FormatBinary)
}

func BenchmarkWriteTextDecimal(b *testing.B) {
	benchmarkWriteDecimal(b, pgwirebase.FormatText)
}
func BenchmarkWriteBinaryDecimal(b *testing.B) {
	benchmarkWriteDecimal(b, pgwirebase.FormatBinary)
}

func BenchmarkWriteTextBytes(b *testing.B) {
	benchmarkWriteBytes(b, pgwirebase.FormatText)
}
func BenchmarkWriteBinaryBytes(b *testing.B) {
	benchmarkWriteBytes(b, pgwirebase.FormatBinary)
}

func BenchmarkWriteTextUUID(b *testing.B) {
	benchmarkWriteUUID(b, pgwirebase.FormatText)
}
func BenchmarkWriteBinaryUUID(b *testing.B) {
	benchmarkWriteUUID(b, pgwirebase.FormatBinary)
}

func BenchmarkWriteTextString(b *testing.B) {
	benchmarkWriteString(b, pgwirebase.FormatText)
}
func BenchmarkWriteBinaryString(b *testing.B) {
	benchmarkWriteString(b, pgwirebase.FormatBinary)
}

func BenchmarkWriteTextDate(b *testing.B) {
	benchmarkWriteDate(b, pgwirebase.FormatText)
}
func BenchmarkWriteBinaryDate(b *testing.B) {
	benchmarkWriteDate(b, pgwirebase.FormatBinary)
}

func BenchmarkWriteTextTimestamp(b *testing.B) {
	benchmarkWriteTimestamp(b, pgwirebase.FormatText)
}
func BenchmarkWriteBinaryTimestamp(b *testing.B) {
	benchmarkWriteTimestamp(b, pgwirebase.FormatBinary)
}

func BenchmarkWriteTextTimestampTZ(b *testing.B) {
	benchmarkWriteTimestampTZ(b, pgwirebase.FormatText)
}
func BenchmarkWriteBinaryTimestampTZ(b *testing.B) {
	benchmarkWriteTimestampTZ(b, pgwirebase.FormatBinary)
}

func BenchmarkWriteTextInterval(b *testing.B) {
	benchmarkWriteInterval(b, pgwirebase.FormatText)
}

func BenchmarkWriteTextTuple(b *testing.B) {
	benchmarkWriteTuple(b, pgwirebase.FormatText)
}

func BenchmarkWriteTextArray(b *testing.B) {
	benchmarkWriteArray(b, pgwirebase.FormatText)
}

func BenchmarkDecodeBinaryDecimal(b *testing.B) {
	wbuf := newWriteBuffer(nil /* bytecount */)
	wbuf.bytecount = metric.NewCounter(metric.Metadata{})

	expected := new(tree.DDecimal)
	s := "-1728718718271827121233.1212121212"
	if err := expected.SetString(s); err != nil {
		b.Fatalf("could not set %q on decimal", s)
	}
	wbuf.writeBinaryDatum(context.Background(), expected, nil /* sessionLoc */, nil /* t */)

	rbuf := pgwirebase.MakeReadBuffer()
	rbuf.Msg = wbuf.wrapped.Bytes()

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
		evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
		defer evalCtx.Stop(context.Background())
		b.StartTimer()
		got, err := pgwirebase.DecodeDatum(evalCtx, types.Decimal, pgwirebase.FormatBinary, bytes)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		} else if got.Compare(evalCtx, expected) != 0 {
			b.Fatalf("expected %s, got %s", expected, got)
		}
	}
}
