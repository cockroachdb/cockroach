// Copyright 2018 The Cockroach Authors.
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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

type encodingTest struct {
	SQL          string
	Datum        tree.Datum
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

	sema := tree.MakeSemaContext(false)
	evalCtx := tree.MakeTestingEvalContext(nil)

	for _, tc := range tests {
		// Convert the SQL expression to a Datum.
		stmt, err := parser.ParseOne(fmt.Sprintf("SELECT %s", tc.SQL))
		if err != nil {
			t.Fatal(err)
		}
		selectStmt, ok := stmt.(*tree.Select)
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
		te, err := expr.TypeCheck(&sema, types.Any)
		if err != nil {
			t.Fatal(err)
		}
		d, err := te.Eval(&evalCtx)
		if err != nil {
			t.Fatal(err)
		}
		tc.Datum = d
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
	for _, tc := range tests {
		t.Run(tc.SQL, func(t *testing.T) {
			d := tc.Datum
			t.Log(tc.SQL)
			t.Log(d)

			t.Run("encode", func(t *testing.T) {
				t.Run(pgwirebase.FormatText.String(), func(t *testing.T) {
					buf.reset()
					buf.textFormatter.Buffer.Reset()
					buf.writeTextDatum(ctx, d, conv)
					if buf.err != nil {
						t.Fatal(buf.err)
					}
					got := verifyLen(t)
					if !bytes.Equal(got, tc.TextAsBinary) {
						t.Errorf("unexpected text encoding:\n\t%q found,\n\t%q expected", got, tc.Text)
					}
				})
				t.Run(pgwirebase.FormatBinary.String(), func(t *testing.T) {
					buf.reset()
					buf.writeBinaryDatum(ctx, d, time.UTC)
					if buf.err != nil {
						t.Fatal(buf.err)
					}
					got := verifyLen(t)
					if !bytes.Equal(got, tc.Binary) {
						t.Errorf("unexpected binary encoding:\n\t%v found,\n\t%v expected", got, tc.Binary)
					}
				})
			})
			t.Run("decode", func(t *testing.T) {
				switch tc.Datum.(type) {
				case *tree.DFloat:
					// Skip floats because postgres rounds them different than Go.
					t.Skip()
				case *tree.DTuple, *tree.DBitArray:
					// Unsupported.
					t.Skip()
				}
				id := tc.Datum.ResolvedType().Oid()
				var d tree.Datum
				var err error
				for code, value := range map[pgwirebase.FormatCode][]byte{
					pgwirebase.FormatText:   tc.TextAsBinary,
					pgwirebase.FormatBinary: tc.Binary,
				} {
					t.Logf("code: %s, value: %q (%[2]s)", code, value)
					t.Run(code.String(), func(t *testing.T) {
						if darr, ok := tc.Datum.(*tree.DArray); ok && code == pgwirebase.FormatText {
							var typ coltypes.T
							typ, err = coltypes.DatumTypeToColumnType(darr.ParamTyp)
							if err != nil {
								t.Fatal(err)
							}
							d, err = tree.ParseDArrayFromString(&evalCtx, string(value), typ)
						} else {
							d, err = pgwirebase.DecodeOidDatum(nil, id, code, value)
						}
						if err != nil {
							t.Fatal(err)
						}
						if d.Compare(&evalCtx, tc.Datum) != 0 {
							t.Fatalf("%v != %v", d, tc.Datum)
						}
					})
				}
			})
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
				b.StopTimer()
				for i := 0; i < b.N; i++ {
					buf.reset()
					buf.textFormatter.Buffer.Reset()
					b.StartTimer()
					buf.writeTextDatum(ctx, d, conv)
					b.StopTimer()
				}
			})
			b.Run("binary", func(b *testing.B) {
				b.StopTimer()
				for i := 0; i < b.N; i++ {
					buf.reset()
					b.StartTimer()
					buf.writeBinaryDatum(ctx, d, time.UTC)
					b.StopTimer()
				}
			})
		})
	}
}

func TestEncodingErrorCounts(t *testing.T) {
	defer leaktest.AfterTest(t)()

	buf := newWriteBuffer(metric.NewCounter(metric.Metadata{}))
	d, _ := tree.ParseDDecimal("Inf")
	buf.writeBinaryDatum(context.Background(), d, nil)
	if count := telemetry.GetFeatureCounts()["pgwire.#32489.binary_decimal_infinity"]; count != 1 {
		t.Fatalf("expected 1 encoding error, got %d", count)
	}
}
