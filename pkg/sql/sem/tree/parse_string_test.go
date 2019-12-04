// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// TestParseDatumStringAs tests that datums are roundtrippable between
// printing with FmtExport and ParseDatumStringAs.
func TestParseDatumStringAs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := map[*types.T][]string{
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
		types.MakeInterval(types.IntervalTypeMetadata{}): {
			"01:02:03",
			"02:03:04",
			"-00:01:00",
			"2 years 3 mons",
		},
		types.MakeInterval(types.IntervalTypeMetadata{Precision: 3, PrecisionIsSet: true}): {
			"01:02:03",
			"02:03:04.123",
		},
		types.MakeInterval(types.IntervalTypeMetadata{Precision: 6, PrecisionIsSet: true}): {
			"01:02:03",
			"02:03:04.123456",
		},
		types.Jsonb: {
			"{}",
			"[]",
			"null",
			"1",
			"1.0",
			`""`,
			`"abc"`,
			`"ab\u0000c"`,
			`"ab\u0001c"`,
			`"ab⚣ cd"`,
		},
		types.String: {
			"",
			"abc",
			"abc\x00",
			"ab⚣ cd",
		},
		types.Timestamp: {
			"2001-01-01 01:02:03+00:00",
			"2001-01-01 02:03:04.123456+00:00",
		},
		types.MakeTimestamp(0): {
			"2001-01-01 01:02:03+00:00",
			"2001-01-01 02:03:04+00:00",
		},
		types.MakeTimestamp(3): {
			"2001-01-01 01:02:03+00:00",
			"2001-01-01 02:03:04.123+00:00",
		},
		types.MakeTimestamp(6): {
			"2001-01-01 01:02:03+00:00",
			"2001-01-01 02:03:04.123456+00:00",
		},
		types.TimestampTZ: {
			"2001-01-01 01:02:03+00:00",
			"2001-01-01 02:03:04.123456+00:00",
		},
		types.MakeTimestampTZ(0): {
			"2001-01-01 01:02:03+00:00",
			"2001-01-01 02:03:04+00:00",
		},
		types.MakeTimestampTZ(3): {
			"2001-01-01 01:02:03+00:00",
			"2001-01-01 02:03:04.123+00:00",
		},
		types.MakeTimestampTZ(6): {
			"2001-01-01 01:02:03+00:00",
			"2001-01-01 02:03:04.123456+00:00",
		},
		types.Time: {
			"01:02:03",
			"02:03:04.123456",
		},
		types.MakeTime(0): {
			"01:02:03",
			"02:03:04",
		},
		types.MakeTime(3): {
			"01:02:03",
			"02:03:04.123",
		},
		types.MakeTime(6): {
			"01:02:03",
			"02:03:04.123456",
		},
		types.TimeTZ: {
			"01:02:03+00:00:00",
			"01:02:03+11:00:00",
			"01:02:03+11:00:00",
			"01:02:03-11:00:00",
			"02:03:04.123456+11:00:00",
		},
		types.MakeTimeTZ(0): {
			"01:02:03+03:30:00",
		},
		types.MakeTimeTZ(3): {
			"01:02:03+03:30:00",
			"02:03:04.123+03:30:00",
		},
		types.MakeTimeTZ(6): {
			"01:02:03+03:30:00",
			"02:03:04.123456+03:30:00",
		},
		types.Uuid: {
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
					if d.ResolvedType().Family() != typ.Family() {
						t.Fatalf("unexpected type: %s", d.ResolvedType())
					}
					ds := AsStringWithFlags(d, FmtExport)
					if s != ds {
						t.Fatalf("unexpected string: %q, expected: %q", ds, s)
					}
				})
			}
		})
	}
}
