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

package tree

import (
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// TestParseDatumStringAs tests that datums are roundtrippable between
// printing with FmtParseDatums and ParseDatumStringAs.
func TestParseDatumStringAs(t *testing.T) {
	tests := []struct {
		typ   types.T
		exprs []string
	}{
		{
			typ: types.Bool,
			exprs: []string{
				"true",
				"false",
			},
		},
		{
			typ: types.Bytes,
			exprs: []string{
				`\x`,
				`\x00`,
				`\xff`,
				`\xffff`,
				fmt.Sprintf(`\x%x`, "abc"),
			},
		},
		{
			typ: types.Date,
			exprs: []string{
				"2001-01-01",
			},
		},
		{
			typ: types.Decimal,
			exprs: []string{
				"0.0",
				"1.0",
				"-1.0",
				strconv.FormatFloat(math.MaxFloat64, 'G', -1, 64),
				strconv.FormatFloat(math.SmallestNonzeroFloat64, 'G', -1, 64),
				strconv.FormatFloat(-math.MaxFloat64, 'G', -1, 64),
				strconv.FormatFloat(-math.SmallestNonzeroFloat64, 'G', -1, 64),
				"1E+1000",
				"1E-1000",
				"Infinity",
				"-Infinity",
				"NaN",
			},
		},
		{
			typ: types.Float,
			exprs: []string{
				"0.0",
				"-0.0",
				"1.0",
				"-1.0",
				strconv.FormatFloat(math.MaxFloat64, 'g', -1, 64),
				strconv.FormatFloat(math.SmallestNonzeroFloat64, 'g', -1, 64),
				strconv.FormatFloat(-math.MaxFloat64, 'g', -1, 64),
				strconv.FormatFloat(-math.SmallestNonzeroFloat64, 'g', -1, 64),
				"+Inf",
				"-Inf",
				"NaN",
			},
		},
		{
			typ: types.INet,
			exprs: []string{
				"127.0.0.1",
			},
		},
		{
			typ: types.Int,
			exprs: []string{
				"1",
				"0",
				"-1",
				strconv.Itoa(math.MaxInt64),
				strconv.Itoa(math.MinInt64),
			},
		},
		{
			typ: types.Interval,
			exprs: []string{
				"1h",
				"-1m",
				"2y3mon",
			},
		},
		{
			typ: types.JSON,
			exprs: []string{
				"{}",
				"[]",
				"null",
				"1",
				"1.0",
				`""`,
			},
		},
		{
			typ: types.String,
			exprs: []string{
				"",
				"abc",
				"abc\x00",
			},
		},
		{
			typ: types.Time,
			exprs: []string{
				"01:02:03",
				"02:03:04.123456",
			},
		},
		{
			typ: types.Timestamp,
			exprs: []string{
				"2001-01-01 01:02:03+00:00",
				"2001-01-01 02:03:04.123456+00:00",
			},
		},
		{
			typ: types.TimestampTZ,
			exprs: []string{
				"2001-01-01 01:02:03+00:00",
				"2001-01-01 02:03:04.123456+00:00",
			},
		},
		{
			typ: types.UUID,
			exprs: []string{
				uuid.MakeV4().String(),
			},
		},
	}

	evalCtx := NewTestingEvalContext(nil)
	for _, test := range tests {
		t.Run(test.typ.String(), func(t *testing.T) {
			for _, s := range test.exprs {
				t.Run(fmt.Sprintf("%q", s), func(t *testing.T) {
					d, err := ParseDatumStringAs(test.typ, s, evalCtx)
					if err != nil {
						t.Fatal(err)
					}
					if !d.ResolvedType().Identical(test.typ) {
						t.Fatalf("unexpected type: %s", d.ResolvedType())
					}
					ds := AsStringWithFlags(d, FmtParseDatums)
					if s != ds {
						t.Fatalf("unexpected string: %q, expected: %q", ds, s)
					}
				})
			}
		})
	}
}
