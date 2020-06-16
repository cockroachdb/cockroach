// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cmpconn

import (
	"math/big"
	"strings"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/errors"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/jackc/pgx/pgtype"
)

// CompareVals returns an error if a and b differ, specifying what the
// difference is. It is designed to compare SQL results from a query
// executed on two different servers or configurations (i.e., cockroach and
// postgres). Postgres and Cockroach have subtle differences in their result
// types and OIDs. This function is aware of those and is able to correctly
// compare those values.
func CompareVals(a, b []interface{}) error {
	if len(a) != len(b) {
		return errors.Errorf("size difference: %d != %d", len(a), len(b))
	}
	if len(a) == 0 {
		return nil
	}
	if diff := cmp.Diff(a, b, cmpOptions...); diff != "" {
		return errors.Newf("unexpected diff:\n%s", diff)
	}
	return nil
}

var (
	cmpOptions = []cmp.Option{
		cmp.Transformer("", func(x []interface{}) []interface{} {
			out := make([]interface{}, len(x))
			for i, v := range x {
				switch t := v.(type) {
				case *pgtype.TextArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = ""
					}
				case *pgtype.BPCharArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = ""
					}
				case *pgtype.VarcharArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = ""
					}
				case *pgtype.Int8Array:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.Int8Array{}
					}
				case *pgtype.Float8Array:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.Float8Array{}
					}
				case *pgtype.UUIDArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.UUIDArray{}
					}
				case *pgtype.ByteaArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.ByteaArray{}
					}
				case *pgtype.InetArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.InetArray{}
					}
				case *pgtype.TimestampArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.TimestampArray{}
					}
				case *pgtype.BoolArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.BoolArray{}
					}
				case *pgtype.DateArray:
					if t.Status == pgtype.Present && len(t.Elements) == 0 {
						v = &pgtype.BoolArray{}
					}
				case *pgtype.Varbit:
					if t.Status == pgtype.Present {
						s, _ := t.EncodeText(nil, nil)
						v = string(s)
					}
				case *pgtype.Bit:
					vb := pgtype.Varbit(*t)
					v = &vb
				case *pgtype.Interval:
					if t.Status == pgtype.Present {
						v = duration.DecodeDuration(int64(t.Months), int64(t.Days), t.Microseconds*1000)
					}
				case string:
					// Postgres sometimes adds spaces to the end of a string.
					t = strings.TrimSpace(t)
					v = strings.Replace(t, "T00:00:00+00:00", "T00:00:00Z", 1)
					v = strings.Replace(t, ":00+00:00", ":00", 1)
				case *pgtype.Numeric:
					if t.Status == pgtype.Present {
						v = apd.NewWithBigInt(t.Int, t.Exp)
					}
				case int64:
					v = apd.New(t, 0)
				}
				out[i] = v
			}
			return out
		}),

		cmpopts.EquateEmpty(),
		cmpopts.EquateNaNs(),
		cmpopts.EquateApprox(0.00001, 0),
		cmp.Comparer(func(x, y *big.Int) bool {
			return x.Cmp(y) == 0
		}),
		cmp.Comparer(func(x, y *apd.Decimal) bool {
			var a, b, min, sub apd.Decimal
			a.Abs(x)
			b.Abs(y)
			if a.Cmp(&b) > 1 {
				min.Set(&b)
			} else {
				min.Set(&a)
			}
			ctx := tree.DecimalCtx
			_, _ = ctx.Mul(&min, &min, decimalCloseness)
			_, _ = ctx.Sub(&sub, x, y)
			sub.Abs(&sub)
			return sub.Cmp(&min) <= 0
		}),
		cmp.Comparer(func(x, y duration.Duration) bool {
			return x.Compare(y) == 0
		}),
	}
	decimalCloseness = apd.New(1, -6)
)
