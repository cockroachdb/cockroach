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
	tests := map[types.T][]string{
		types.Bool: {
			"true",
			"false",
		},
		types.Bytes: {
			`\x`,
			`\x00`,
			`\xff`,
			`\xffff`,
			fmt.Sprintf(`\x%x`, "abc"),
		},
		types.Date: {
			"2001-01-01",
		},
		types.Decimal: {
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
		types.Float: {
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
		types.INet: {
			"127.0.0.1",
		},
		types.Int: {
			"1",
			"0",
			"-1",
			strconv.Itoa(math.MaxInt64),
			strconv.Itoa(math.MinInt64),
		},
		types.Interval: {
			"01:00:00",
			"-00:01:00",
			"2 years 3 mons",
		},
		types.JSON: {
			"{}",
			"[]",
			"null",
			"1",
			"1.0",
			`""`,
		},
		types.String: {
			"",
			"abc",
			"abc\x00",
		},
		types.Time: {
			"01:02:03",
			"02:03:04.123456",
		},
		types.Timestamp: {
			"2001-01-01 01:02:03+00:00",
			"2001-01-01 02:03:04.123456+00:00",
		},
		types.TimestampTZ: {
			"2001-01-01 01:02:03+00:00",
			"2001-01-01 02:03:04.123456+00:00",
		},
		types.UUID: {
			uuid.MakeV4().String(),
		},
	}
	evalCtx := NewTestingEvalContext(nil)
	for typ, exprs := range tests {
		t.Run(typ.String(), func(t *testing.T) {
			for _, s := range exprs {
				t.Run(fmt.Sprintf("%q", s), func(t *testing.T) {
					d, err := ParseDatumStringAs(typ, s, evalCtx)
					if err != nil {
						t.Fatal(err)
					}
					if d.ResolvedType() != typ {
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
