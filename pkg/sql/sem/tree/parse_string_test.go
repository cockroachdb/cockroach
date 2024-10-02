// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree_test

import (
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/stretchr/testify/require"
)

func vecRow(v *coldata.Vec, i int) any {
	switch v.CanonicalTypeFamily() {
	case types.BoolFamily:
		return v.Bool()[i]
	case types.BytesFamily:
		return v.Bytes().Get(i)
	case types.IntFamily:
		return v.Int64()[i]
	case types.DecimalFamily:
		return v.Decimal().Get(i)
	case types.FloatFamily:
		return v.Float64()[i]
	case types.JsonFamily:
		return v.JSON().Get(i)
	case types.TimestampTZFamily:
		return v.Timestamp()[i]
	case types.IntervalFamily:
		return v.Interval().Get(i)
	default:
		return v.Datum().Get(i)
	}
}

// TestParseStringTypeGamut is a sanity test for ParseAndRequireStringHandler
// that tests that all the basic scalar types parse to the same value as
// ParseAndRequireString.
func TestParseStringTypeGamut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	factory := coldataext.NewExtendedColumnFactory(evalCtx)
	b := coldata.NewMemBatchWithCapacity(types.Scalar, 2, factory)
	vecHandlers := make([]tree.ValueHandler, len(types.Scalar))
	rng, _ := randutil.NewTestRand()
	for i, typ := range types.Scalar {
		d := randgen.RandDatum(rng, typ, false)
		s := d.String()
		// ParseAndRequireString doesn't like things wrapped in single quotes
		// (its raw data not SQL).
		if s[0] == '\'' {
			s = s[1 : len(s)-1]
		}
		d, _, err1 := tree.ParseAndRequireString(typ, s, evalCtx)
		vecHandlers[i] = coldataext.MakeVecHandler(b.ColVec(i))
		err2 := tree.ParseAndRequireStringHandler(typ, s, evalCtx, vecHandlers[i], &evalCtx.ParseHelper)
		require.Equal(t, err1, err2)
		if err1 == nil {
			if d.ResolvedType().Family() == types.FloatFamily {
				// NaN's can't be equal.
				if math.IsNaN(float64(*d.(*tree.DFloat))) {
					continue
				}
			}
			converter := colconv.GetDatumToPhysicalFn(typ)
			coldata.SetValueAt(b.ColVec(i), converter(d), 1 /* rowIdx */)
			// ParseAndRequireStringHandler set the first row and second was converted datum,
			// test that they are equal.
			require.Equal(t, vecRow(b.ColVec(i), 0), vecRow(b.ColVec(i), 1))
		}
	}
}

// TestParseStringHandlerErrors tests that bogus strings return the same errors from
// ParseAndRequireString and ParseAndRequireStringHandler for vector types.
func TestParseStringHandlerErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	for _, tc := range []struct {
		val string
		t   *types.T
	}{
		{"", types.Bool},
		{"o", types.Bool},
		{`\xa`, types.Bytes},
		{"", types.Date},
		{"423136fs", types.Date},
		{"", types.Decimal},
		{"not a decimal", types.Decimal},
		{"", types.Float},
		{"abc", types.Float},
		{"", types.Int},
		{"abc", types.Int},
		{"", types.Json},
		{"}{", types.Json},
		{"", types.TimestampTZ},
		{"not a timestamp", types.TimestampTZ},
		{"", types.Timestamp},
		{"not a timestamp", types.Timestamp},
		{"", types.Interval},
		{"Pasdf", types.Interval},
		{"", types.Uuid},
		{"notuuid", types.Uuid},
	} {
		_, _, err1 := tree.ParseAndRequireString(tc.t, tc.val, evalCtx)
		require.Errorf(t, err1, "parsing `%s` as `%v` didn't error as expected", tc.val, tc.t)
		vh := &anyHandler{}
		err2 := tree.ParseAndRequireStringHandler(tc.t, tc.val, evalCtx, vh, &evalCtx.ParseHelper)
		require.Equal(t, err1.Error(), err2.Error())
	}
}

type anyHandler struct {
	val any
	dec apd.Decimal
}

var _ tree.ValueHandler = (*anyHandler)(nil)

func (a *anyHandler) Len() int                     { return 0 }
func (a *anyHandler) Null()                        { a.val = nil }
func (a *anyHandler) Date(d pgdate.Date)           { a.val = d }
func (a *anyHandler) Datum(d tree.Datum)           { a.val = d }
func (a *anyHandler) Bool(b bool)                  { a.val = b }
func (a *anyHandler) Bytes(b []byte)               { a.val = b }
func (a *anyHandler) Decimal() *apd.Decimal        { return &a.dec }
func (a *anyHandler) Float(f float64)              { a.val = f }
func (a *anyHandler) Int16(i int16)                { a.val = i }
func (a *anyHandler) Int32(i int32)                { a.val = i }
func (a *anyHandler) Int(i int64)                  { a.val = i }
func (a *anyHandler) Duration(d duration.Duration) { a.val = d }
func (a *anyHandler) JSON(j json.JSON)             { a.val = j }
func (a *anyHandler) String(s string)              { a.val = s }
func (a *anyHandler) TimestampTZ(t time.Time)      { a.val = t }
func (a *anyHandler) Reset()                       {}

type benchCase struct {
	typ *types.T
	str string
}

var benchCases []benchCase = []benchCase{
	{types.Date, "1996-03-13"},
	{types.Bool, "true"},
	{types.Decimal, "21168.23"},
	{types.Float, "0.04"},
	{types.Int, "155190"},
	{types.Interval, "1h"},
	{types.Json, `{"a": "b"}`},
	{types.String, "asdf"},
	{types.TimestampTZ, "2000-05-05 10:00:00+03"},
}

func BenchmarkParseString(b *testing.B) {
	evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	factory := coldataext.NewExtendedColumnFactory(evalCtx)
	numRows := 1000
	var typs = make([]*types.T, len(benchCases))
	for i, tc := range benchCases {
		typs[i] = tc.typ
	}
	for _, tc := range benchCases {
		b.Run(tc.typ.Name(), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _, err := tree.ParseAndRequireString(tc.typ, tc.str, evalCtx)
				require.NoError(b, err)
			}
		})
	}
	for col, tc := range benchCases {
		b.Run("vec/"+tc.typ.Name(), func(b *testing.B) {
			var vhs = make([]tree.ValueHandler, len(benchCases))
			ba := coldata.NewMemBatchWithCapacity(typs, numRows, factory)
			for i := range benchCases {
				vhs[i] = coldataext.MakeVecHandler(ba.ColVec(i))
			}
			b.ResetTimer()
			rowCount := 0
			for i := 0; i < b.N; i++ {
				err := tree.ParseAndRequireStringHandler(tc.typ, tc.str, evalCtx, vhs[col], &evalCtx.ParseHelper)
				require.NoError(b, err)
				rowCount++
				if rowCount == numRows {
					vhs[col].Reset()
					rowCount = 0
				}
			}
		})
	}
}
