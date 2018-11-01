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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// TestEncodings uses testdata/encodings.json to test expected pgwire encodings
// and ensure they are identical to what Postgres produces. Regenerate that
// file by:
//   Starting a postgres server on :5432 then running:
//   cd pkg/cmd/generate-binary; go run main.go > ../../sql/pgwire/testdata/encodings.json
func TestEncodings(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var tests []struct {
		SQL    string
		Binary []byte
	}
	f, err := os.Open(filepath.Join("testdata", "encodings.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewDecoder(f).Decode(&tests); err != nil {
		t.Fatal(err)
	}
	f.Close()
	buf := newWriteBuffer(metric.NewCounter(metric.Metadata{}))
	sema := tree.MakeSemaContext(false)
	evalCtx := tree.MakeTestingEvalContext(nil)
	ctx := context.Background()
	for _, tc := range tests {
		t.Run(tc.SQL, func(t *testing.T) {
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

			{
				buf.wrapped.Reset()
				buf.writeBinaryDatum(ctx, d, time.UTC)
				if buf.err != nil {
					t.Fatal(buf.err)
				}
				// The first 4 bytes are a length prefix that we don't care about.
				got := buf.wrapped.Bytes()[4:]
				if !bytes.Equal(got, tc.Binary) {
					t.Errorf("unexpected binary encoding:\n\t%v found,\n\t%v expected", got, tc.Binary)
				}
			}
		})
	}
}
