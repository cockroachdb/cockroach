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
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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

		// Populate specific type information based on OID and the specific test
		// case.
		switch tc.T.Oid() {
		case oid.T_bpchar:
			// The width of a bpchar type is fixed and equal to the length of the
			// Text string returned by postgres.
			tc.T.InternalType.Width = int32(len(tc.Text))
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

	var conv sessiondata.DataConversionConfig
	ctx := context.Background()
	evalCtx := tree.MakeTestingEvalContext(nil)
	t.Run("encode", func(t *testing.T) {
		t.Run(pgwirebase.FormatText.String(), func(t *testing.T) {
			for _, tc := range tests {
				d := tc.Datum

				buf.reset()
				buf.textFormatter.Buffer.Reset()
				buf.writeTextDatum(ctx, d, conv, tc.T)
				if buf.err != nil {
					t.Fatal(buf.err)
				}
				got := verifyLen(t)
				if !bytes.Equal(got, tc.TextAsBinary) {
					t.Errorf("unexpected text encoding:\n\t%q found,\n\t%q expected", got, tc.Text)
				}
			}
		})
		t.Run(pgwirebase.FormatBinary.String(), func(t *testing.T) {
			for _, tc := range tests {
				d := tc.Datum
				buf.reset()
				buf.writeBinaryDatum(ctx, d, time.UTC, tc.T)
				if buf.err != nil {
					t.Fatal(buf.err)
				}
				got := verifyLen(t)
				if !bytes.Equal(got, tc.Binary) {
					t.Errorf("unexpected binary encoding:\n\t%v found,\n\t%v expected", got, tc.Binary)
				}
			}
		})
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
			}
			for code, value := range map[pgwirebase.FormatCode][]byte{
				pgwirebase.FormatText:   tc.TextAsBinary,
				pgwirebase.FormatBinary: tc.Binary,
			} {
				d, err := pgwirebase.DecodeOidDatum(nil, tc.Oid, code, value)
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
			d, err := pgwirebase.DecodeOidDatum(nil, oid.T_numeric, pgwirebase.FormatBinary, c.Encoding)
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
	var conv sessiondata.DataConversionConfig
	ctx := context.Background()

	for _, tc := range tests {
		b.Run(tc.SQL, func(b *testing.B) {
			d := tc.Datum

			b.Run("text", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					buf.reset()
					buf.textFormatter.Buffer.Reset()
					buf.writeTextDatum(ctx, d, conv, tc.T)
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

	buf := newWriteBuffer(metric.NewCounter(metric.Metadata{}))
	d, _ := tree.ParseDDecimal("Inf")
	buf.writeBinaryDatum(context.Background(), d, nil, d.ResolvedType())
	if count := telemetry.GetRawFeatureCounts()["pgwire.#32489.binary_decimal_infinity"]; count != 1 {
		t.Fatalf("expected 1 encoding error, got %d", count)
	}
}
