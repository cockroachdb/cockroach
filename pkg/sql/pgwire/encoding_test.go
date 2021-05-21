// Copyright 2018 The Cockroach Authors.
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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/lib/pq/oid"
)

type encodingTest struct {
	SQL          string
	Datum        tree.Datum
	Oid          oid.Oid
	T            *types.T
	Text         string
	TextAsBinary []byte
	Binary       []byte
}

func readEncodingTests(t testing.TB) []*encodingTest {
	var tests []*encodingTest
	f, err := os.Open(filepath.Join("testdata", "encodings.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewDecoder(f).Decode(&tests); err != nil {
		t.Fatal(err)
	}
	f.Close()

	ctx := context.Background()
	sema := tree.MakeSemaContext()
	evalCtx := tree.MakeTestingEvalContext(nil)

	for _, tc := range tests {
		// Convert the SQL expression to a Datum.
		stmt, err := parser.ParseOne(fmt.Sprintf("SELECT %s", tc.SQL))
		if err != nil {
			t.Fatal(err)
		}
		selectStmt, ok := stmt.AST.(*tree.Select)
		if !ok {
			t.Fatal("not select")
		}
		selectClause, ok := selectStmt.Select.(*tree.SelectClause)
		if !ok {
			t.Fatal("not select clause")
		}
		if len(selectClause.Exprs) != 1 {
			t.Fatal("expected 1 expr")
		}
		expr := selectClause.Exprs[0].Expr
		te, err := expr.TypeCheck(ctx, &sema, types.Any)
		if err != nil {
			t.Fatal(err)
		}
		d, err := te.Eval(&evalCtx)
		if err != nil {
			t.Fatal(err)
		}
		tc.Datum = d

		// Annotate with the type.
		tc.T = types.OidToType[tc.Oid]
		if tc.T == nil {
			t.Fatalf("unknown Oid %d not found in the OidToType map", tc.Oid)
		}
		// If we type checked the expression and got a collated string, we need
		// to override the type accordingly. If we don't do it, then the datum
		// and the type would diverge (we would have tree.DCollatedString and
		// the type of types.StringFamily).
		if actualType := d.ResolvedType(); actualType.Family() == types.CollatedStringFamily {
			tc.T = types.MakeCollatedString(tc.T, actualType.Locale())
		}

		// Populate specific type information based on OID and the specific test
		// case.
		switch tc.T.Oid() {
		case oid.T_bpchar:
			// The width of a bpchar type is fixed and equal to the length of the
			// Text string returned by postgres.
			tc.T.InternalType.Width = int32(len(tc.Text))
		case oid.T_record:
			tupleExpr := te.(*tree.Tuple)
			typs := make([]*types.T, len(tupleExpr.Exprs))
			for i := range tupleExpr.Exprs {
				typs[i] = tupleExpr.Exprs[i].(tree.TypedExpr).ResolvedType()
			}
			tc.T = types.MakeTuple(typs)
		}
	}

	return tests
}

// TestEncodings uses testdata/encodings.json to test expected pgwire encodings
// and ensure they are identical to what Postgres produces. Regenerate that
// file by:
//   Starting a postgres server on :5432 then running:
//   cd pkg/cmd/generate-binary; go run main.go > ../../sql/pgwire/testdata/encodings.json
func TestEncodings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := readEncodingTests(t)
	buf := newWriteBuffer(metric.NewCounter(metric.Metadata{}))

	verifyLen := func(t *testing.T) []byte {
		t.Helper()
		b := buf.wrapped.Bytes()
		if len(b) < 4 {
			t.Fatal("short buffer")
		}
		n := binary.BigEndian.Uint32(b)
		// The first 4 bytes are the length prefix.
		data := b[4:]
		if len(data) != int(n) {
			t.Logf("%v", b)
			t.Errorf("expected %d bytes, got %d", n, len(data))
		}
		return data
	}

	conv, loc := makeTestingConvCfg()
	ctx := context.Background()
	evalCtx := tree.MakeTestingEvalContext(nil)

	type writeFunc func(tree.Datum, *types.T)
	type testCase struct {
		name    string
		writeFn writeFunc
	}

	writeTextDatum := func(d tree.Datum, t *types.T) {
		buf.writeTextDatum(ctx, d, conv, loc, t)
	}
	writeBinaryDatum := func(d tree.Datum, t *types.T) {
		buf.writeBinaryDatum(ctx, d, time.UTC, t)
	}
	convertToVec := func(d tree.Datum, t *types.T) coldata.Vec {
		vec := coldata.NewMemColumn(t, 1 /* length */, coldataext.NewExtendedColumnFactory(&evalCtx))
		converter := colconv.GetDatumToPhysicalFn(t)
		coldata.SetValueAt(vec, converter(d), 0 /* rowIdx */)
		return vec
	}
	writeTextColumnarElement := func(d tree.Datum, t *types.T) {
		buf.writeTextColumnarElement(ctx, convertToVec(d, t), 0 /* idx */, conv, loc)
	}
	writeBinaryColumnarElement := func(d tree.Datum, t *types.T) {
		buf.writeBinaryColumnarElement(ctx, convertToVec(d, t), 0 /* idx */, loc)
	}
	t.Run("encode", func(t *testing.T) {
		for _, test := range tests {
			for _, tc := range []testCase{
				{
					name:    "datum",
					writeFn: writeTextDatum,
				},
				{
					name:    "columnar",
					writeFn: writeTextColumnarElement,
				},
			} {
				t.Run(fmt.Sprintf("%s/%s", pgwirebase.FormatText, tc.name), func(t *testing.T) {
					d := test.Datum
					buf.reset()
					buf.textFormatter.Buffer.Reset()
					tc.writeFn(d, test.T)
					if buf.err != nil {
						t.Fatal(buf.err)
					}
					got := verifyLen(t)
					if !bytes.Equal(got, test.TextAsBinary) {
						t.Errorf("unexpected text encoding:\n\t%q found,\n\t%q expected", got, test.TextAsBinary)
					}
				})
			}
		}
		for _, test := range tests {
			for _, tc := range []testCase{
				{
					name:    "datum",
					writeFn: writeBinaryDatum,
				},
				{
					name:    "columnar",
					writeFn: writeBinaryColumnarElement,
				},
			} {
				t.Run(fmt.Sprintf("%s/%s", pgwirebase.FormatBinary, tc.name), func(t *testing.T) {
					d := test.Datum
					buf.reset()
					tc.writeFn(d, test.T)
					if buf.err != nil {
						t.Fatal(buf.err)
					}
					got := verifyLen(t)
					if !bytes.Equal(got, test.Binary) {
						t.Errorf("unexpected binary encoding:\n\t%v found,\n\t%v expected", got, test.Binary)
					}
				})
			}
		}
	})
	t.Run("decode", func(t *testing.T) {
		for _, tc := range tests {
			switch tc.Datum.(type) {
			case *tree.DFloat:
				// Skip floats because postgres rounds them different than Go.
				continue
			case *tree.DTuple:
				// Unsupported.
				continue
			case *tree.DCollatedString:
				// Decoding collated strings is unsupported by this test. The encoded
				// value is the same as a normal string, so decoding it turns it into
				// a DString.
				continue
			}
			for code, value := range map[pgwirebase.FormatCode][]byte{
				pgwirebase.FormatText:   tc.TextAsBinary,
				pgwirebase.FormatBinary: tc.Binary,
			} {
				d, err := pgwirebase.DecodeDatum(
					&evalCtx,
					types.OidToType[tc.Oid],
					code,
					value,
				)
				if err != nil {
					t.Fatal(err)
				}
				// Text decoding returns a string for some kinds of arrays. If that's
				// the case, manually do the conversion to array.
				darr, isdarr := tc.Datum.(*tree.DArray)
				if isdarr && d.ResolvedType().Family() == types.StringFamily {
					d, _, err = tree.ParseDArrayFromString(&evalCtx, string(value), darr.ParamTyp)
					if err != nil {
						t.Fatal(err)
					}
				}
				if d.Compare(&evalCtx, tc.Datum) != 0 {
					t.Fatalf("%v != %v", d, tc.Datum)
				}
			}
		}
	})
}

// TestExoticNumericEncodings goes through specific, legal pgwire encodings
// that Postgres itself would usually choose to not produce, which therefore
// would not be covered by TestEncodings. Of course, being valid encodings
// they'd still be accepted and correctly parsed by Postgres.
func TestExoticNumericEncodings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		Value    *apd.Decimal
		Encoding []byte
	}{
		{apd.New(0, 0), []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{apd.New(0, 0), []byte{0, 1, 0, 0, 0, 0, 0, 0, 0, 0}},
		{apd.New(10000, 0), []byte{0, 2, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0}},
		{apd.New(10001, 0), []byte{0, 2, 0, 1, 0, 0, 0, 0, 0, 1, 0, 1}},
		{apd.New(1000000, 0), []byte{0, 2, 0, 1, 0, 0, 0, 0, 0, 100, 0, 0}},
		{apd.New(1000001, 0), []byte{0, 2, 0, 1, 0, 0, 0, 0, 0, 100, 0, 1}},
		{apd.New(100000000, 0), []byte{0, 1, 0, 2, 0, 0, 0, 0, 0, 1}},
		{apd.New(100000000, 0), []byte{0, 2, 0, 2, 0, 0, 0, 0, 0, 1, 0, 0}},
		{apd.New(100000000, 0), []byte{0, 3, 0, 2, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0}},
		{apd.New(100000001, 0), []byte{0, 3, 0, 2, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1}},
		// Elixir/Postgrex combinations.
		{apd.New(1234, 0), []byte{0, 2, 0, 0, 0, 0, 0, 0, 0x4, 0xd2, 0, 0}},
		{apd.New(12340, -1), []byte{0, 2, 0, 0, 0, 0, 0, 1, 0x4, 0xd2, 0, 0}},
		{apd.New(1234123400, -2), []byte{0, 3, 0, 1, 0, 0, 0, 2, 0x4, 0xd2, 0x4, 0xd2, 0, 0}},
		{apd.New(12340000, 0), []byte{0, 3, 0, 1, 0, 0, 0, 0, 0x4, 0xd2, 0, 0, 0, 0}},
		{apd.New(123400000, -1), []byte{0, 3, 0, 1, 0, 0, 0, 1, 0x4, 0xd2, 0, 0, 0, 0}},
		{apd.New(12341234000000, -2), []byte{0, 4, 0, 2, 0, 0, 0, 2, 0x4, 0xd2, 0x4, 0xd2, 0, 0, 0, 0}},
		// Postgrex inspired -- even more trailing zeroes!
		{apd.New(0, 0), []byte{0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{apd.New(1234123400, -2), []byte{0, 4, 0, 1, 0, 0, 0, 2, 0x4, 0xd2, 0x4, 0xd2, 0, 0, 0, 0}},
	}

	evalCtx := tree.MakeTestingEvalContext(nil)
	for i, c := range testCases {
		t.Run(fmt.Sprintf("%d_%s", i, c.Value), func(t *testing.T) {
			d, err := pgwirebase.DecodeDatum(
				&evalCtx,
				types.Decimal,
				pgwirebase.FormatBinary,
				c.Encoding,
			)
			if err != nil {
				t.Fatal(err)
			}

			expected := &tree.DDecimal{Decimal: *c.Value}
			if d.Compare(&evalCtx, expected) != 0 {
				t.Fatalf("%v != %v", d, expected)
			}
		})
	}
}

func BenchmarkEncodings(b *testing.B) {
	tests := readEncodingTests(b)
	buf := newWriteBuffer(metric.NewCounter(metric.Metadata{}))
	conv, loc := makeTestingConvCfg()
	ctx := context.Background()

	for _, tc := range tests {
		b.Run(tc.SQL, func(b *testing.B) {
			d := tc.Datum

			b.Run("text", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					buf.reset()
					buf.textFormatter.Buffer.Reset()
					buf.writeTextDatum(ctx, d, conv, loc, tc.T)
				}
			})
			b.Run("binary", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					buf.reset()
					buf.writeBinaryDatum(ctx, d, time.UTC, tc.T)
				}
			})
		})
	}
}

func TestEncodingErrorCounts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	buf := newWriteBuffer(metric.NewCounter(metric.Metadata{}))
	d, _ := tree.ParseDDecimal("Inf")
	buf.writeBinaryDatum(context.Background(), d, nil, d.ResolvedType())
	if count := telemetry.GetRawFeatureCounts()["pgwire.#32489.binary_decimal_infinity"]; count != 1 {
		t.Fatalf("expected 1 encoding error, got %d", count)
	}
}
