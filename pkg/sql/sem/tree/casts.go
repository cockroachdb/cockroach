// Copyright 2020 The Cockroach Authors.
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
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/lib/pq/oid"
)

type castInfo struct {
	from       types.Family
	to         types.Family
	volatility Volatility

	// Telemetry counter; set by init().
	counter telemetry.Counter

	// If set, the volatility of this cast is not cross-checked against postgres.
	// Use this with caution.
	ignoreVolatilityCheck bool
}

// validCasts lists all valid explicit casts.
//
// This list must be kept in sync with the capabilities of PerformCast.
//
// Each cast defines a volatility:
//
//  - immutable casts yield the same result on the same arguments in whatever
//    context they are evaluated.
//
//  - stable casts can yield a different result depending on the evaluation context:
//    - session settings (e.g. bytes encoding format)
//    - current timezone
//    - current time (e.g. 'now'::string).
//
// TODO(radu): move the PerformCast code for each cast into functions defined
// within each cast.
//
var validCasts = []castInfo{
	// Casts to BitFamily.
	{from: types.UnknownFamily, to: types.BitFamily, volatility: VolatilityImmutable},
	{from: types.BitFamily, to: types.BitFamily, volatility: VolatilityImmutable},
	{from: types.IntFamily, to: types.BitFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.BitFamily, volatility: VolatilityImmutable},
	{from: types.CollatedStringFamily, to: types.BitFamily, volatility: VolatilityImmutable},

	// Casts to BoolFamily.
	{from: types.UnknownFamily, to: types.BoolFamily, volatility: VolatilityImmutable},
	{from: types.BoolFamily, to: types.BoolFamily, volatility: VolatilityImmutable},
	{from: types.IntFamily, to: types.BoolFamily, volatility: VolatilityImmutable},
	{from: types.FloatFamily, to: types.BoolFamily, volatility: VolatilityImmutable},
	{from: types.DecimalFamily, to: types.BoolFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.BoolFamily, volatility: VolatilityImmutable},
	{from: types.CollatedStringFamily, to: types.BoolFamily, volatility: VolatilityImmutable},

	// Casts to IntFamily.
	{from: types.UnknownFamily, to: types.IntFamily, volatility: VolatilityImmutable},
	{from: types.BoolFamily, to: types.IntFamily, volatility: VolatilityImmutable},
	{from: types.IntFamily, to: types.IntFamily, volatility: VolatilityImmutable},
	{from: types.FloatFamily, to: types.IntFamily, volatility: VolatilityImmutable},
	{from: types.DecimalFamily, to: types.IntFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.IntFamily, volatility: VolatilityImmutable},
	{from: types.CollatedStringFamily, to: types.IntFamily, volatility: VolatilityImmutable},
	{from: types.TimestampFamily, to: types.IntFamily, volatility: VolatilityImmutable},
	{from: types.TimestampTZFamily, to: types.IntFamily, volatility: VolatilityImmutable},
	{from: types.DateFamily, to: types.IntFamily, volatility: VolatilityImmutable},
	{from: types.IntervalFamily, to: types.IntFamily, volatility: VolatilityImmutable},
	{from: types.OidFamily, to: types.IntFamily, volatility: VolatilityImmutable},
	{from: types.BitFamily, to: types.IntFamily, volatility: VolatilityImmutable},

	// Casts to FloatFamily.
	{from: types.UnknownFamily, to: types.FloatFamily, volatility: VolatilityImmutable},
	{from: types.BoolFamily, to: types.FloatFamily, volatility: VolatilityImmutable},
	{from: types.IntFamily, to: types.FloatFamily, volatility: VolatilityImmutable},
	{from: types.FloatFamily, to: types.FloatFamily, volatility: VolatilityImmutable},
	{from: types.DecimalFamily, to: types.FloatFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.FloatFamily, volatility: VolatilityImmutable},
	{from: types.CollatedStringFamily, to: types.FloatFamily, volatility: VolatilityImmutable},
	{from: types.TimestampFamily, to: types.FloatFamily, volatility: VolatilityImmutable},
	{from: types.TimestampTZFamily, to: types.FloatFamily, volatility: VolatilityImmutable},
	{from: types.DateFamily, to: types.FloatFamily, volatility: VolatilityImmutable},
	{from: types.IntervalFamily, to: types.FloatFamily, volatility: VolatilityImmutable},

	// Casts to GeographyFamily.
	{from: types.UnknownFamily, to: types.GeographyFamily, volatility: VolatilityImmutable},
	{from: types.BytesFamily, to: types.GeographyFamily, volatility: VolatilityImmutable},
	{from: types.JsonFamily, to: types.GeographyFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.GeographyFamily, volatility: VolatilityImmutable},
	{from: types.CollatedStringFamily, to: types.GeographyFamily, volatility: VolatilityImmutable},
	{from: types.GeographyFamily, to: types.GeographyFamily, volatility: VolatilityImmutable},
	{from: types.GeometryFamily, to: types.GeographyFamily, volatility: VolatilityImmutable},

	// Casts to GeometryFamily.
	{from: types.UnknownFamily, to: types.GeometryFamily, volatility: VolatilityImmutable},
	{from: types.BytesFamily, to: types.GeometryFamily, volatility: VolatilityImmutable},
	{from: types.JsonFamily, to: types.GeometryFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.GeometryFamily, volatility: VolatilityImmutable},
	{from: types.CollatedStringFamily, to: types.GeometryFamily, volatility: VolatilityImmutable},
	{from: types.GeographyFamily, to: types.GeometryFamily, volatility: VolatilityImmutable},
	{from: types.GeometryFamily, to: types.GeometryFamily, volatility: VolatilityImmutable},

	// Casts to DecimalFamily.
	{from: types.UnknownFamily, to: types.DecimalFamily, volatility: VolatilityImmutable},
	{from: types.BoolFamily, to: types.DecimalFamily, volatility: VolatilityImmutable},
	{from: types.IntFamily, to: types.DecimalFamily, volatility: VolatilityImmutable},
	{from: types.FloatFamily, to: types.DecimalFamily, volatility: VolatilityImmutable},
	{from: types.DecimalFamily, to: types.DecimalFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.DecimalFamily, volatility: VolatilityImmutable},
	{from: types.CollatedStringFamily, to: types.DecimalFamily, volatility: VolatilityImmutable},
	{from: types.TimestampFamily, to: types.DecimalFamily, volatility: VolatilityImmutable},
	{from: types.TimestampTZFamily, to: types.DecimalFamily, volatility: VolatilityImmutable},
	{from: types.DateFamily, to: types.DecimalFamily, volatility: VolatilityImmutable},
	{from: types.IntervalFamily, to: types.DecimalFamily, volatility: VolatilityImmutable},

	// Casts to StringFamily.
	{from: types.UnknownFamily, to: types.StringFamily, volatility: VolatilityImmutable},
	{from: types.BoolFamily, to: types.StringFamily, volatility: VolatilityImmutable},
	{from: types.IntFamily, to: types.StringFamily, volatility: VolatilityImmutable},
	{from: types.FloatFamily, to: types.StringFamily, volatility: VolatilityStable},
	{from: types.DecimalFamily, to: types.StringFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.StringFamily, volatility: VolatilityImmutable},
	{from: types.CollatedStringFamily, to: types.StringFamily, volatility: VolatilityImmutable},
	{from: types.BitFamily, to: types.StringFamily, volatility: VolatilityImmutable},
	{from: types.ArrayFamily, to: types.StringFamily, volatility: VolatilityStable},
	{from: types.TupleFamily, to: types.StringFamily, volatility: VolatilityImmutable},
	{from: types.GeometryFamily, to: types.StringFamily, volatility: VolatilityImmutable},
	{from: types.GeographyFamily, to: types.StringFamily, volatility: VolatilityImmutable},
	{from: types.BytesFamily, to: types.StringFamily, volatility: VolatilityStable},
	{from: types.TimestampFamily, to: types.StringFamily, volatility: VolatilityImmutable},
	{from: types.TimestampTZFamily, to: types.StringFamily, volatility: VolatilityStable},
	{from: types.IntervalFamily, to: types.StringFamily, volatility: VolatilityImmutable},
	{from: types.UuidFamily, to: types.StringFamily, volatility: VolatilityImmutable},
	{from: types.DateFamily, to: types.StringFamily, volatility: VolatilityImmutable},
	{from: types.TimeFamily, to: types.StringFamily, volatility: VolatilityImmutable},
	{from: types.TimeTZFamily, to: types.StringFamily, volatility: VolatilityImmutable},
	{from: types.OidFamily, to: types.StringFamily, volatility: VolatilityImmutable},
	{from: types.INetFamily, to: types.StringFamily, volatility: VolatilityImmutable},
	{from: types.JsonFamily, to: types.StringFamily, volatility: VolatilityImmutable},
	{from: types.EnumFamily, to: types.StringFamily, volatility: VolatilityImmutable},

	// Casts to CollatedStringFamily.
	{from: types.UnknownFamily, to: types.CollatedStringFamily, volatility: VolatilityImmutable},
	{from: types.BoolFamily, to: types.CollatedStringFamily, volatility: VolatilityImmutable},
	{from: types.IntFamily, to: types.CollatedStringFamily, volatility: VolatilityImmutable},
	{from: types.FloatFamily, to: types.CollatedStringFamily, volatility: VolatilityStable},
	{from: types.DecimalFamily, to: types.CollatedStringFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.CollatedStringFamily, volatility: VolatilityImmutable},
	{from: types.CollatedStringFamily, to: types.CollatedStringFamily, volatility: VolatilityImmutable},
	{from: types.BitFamily, to: types.CollatedStringFamily, volatility: VolatilityImmutable},
	{from: types.ArrayFamily, to: types.CollatedStringFamily, volatility: VolatilityStable},
	{from: types.TupleFamily, to: types.CollatedStringFamily, volatility: VolatilityImmutable},
	{from: types.GeometryFamily, to: types.CollatedStringFamily, volatility: VolatilityImmutable},
	{from: types.GeographyFamily, to: types.CollatedStringFamily, volatility: VolatilityImmutable},
	{from: types.BytesFamily, to: types.CollatedStringFamily, volatility: VolatilityStable},
	{from: types.TimestampFamily, to: types.CollatedStringFamily, volatility: VolatilityImmutable},
	{from: types.TimestampTZFamily, to: types.CollatedStringFamily, volatility: VolatilityStable},
	{from: types.IntervalFamily, to: types.CollatedStringFamily, volatility: VolatilityImmutable},
	{from: types.UuidFamily, to: types.CollatedStringFamily, volatility: VolatilityImmutable},
	{from: types.DateFamily, to: types.CollatedStringFamily, volatility: VolatilityImmutable},
	{from: types.TimeFamily, to: types.CollatedStringFamily, volatility: VolatilityImmutable},
	{from: types.TimeTZFamily, to: types.CollatedStringFamily, volatility: VolatilityImmutable},
	{from: types.OidFamily, to: types.CollatedStringFamily, volatility: VolatilityImmutable},
	{from: types.INetFamily, to: types.CollatedStringFamily, volatility: VolatilityImmutable},
	{from: types.JsonFamily, to: types.CollatedStringFamily, volatility: VolatilityImmutable},
	{from: types.EnumFamily, to: types.CollatedStringFamily, volatility: VolatilityImmutable},

	// Casts to BytesFamily.
	{from: types.UnknownFamily, to: types.BytesFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.BytesFamily, volatility: VolatilityImmutable},
	{from: types.CollatedStringFamily, to: types.BytesFamily, volatility: VolatilityImmutable},
	{from: types.BytesFamily, to: types.BytesFamily, volatility: VolatilityImmutable},
	{from: types.UuidFamily, to: types.BytesFamily, volatility: VolatilityImmutable},
	{from: types.GeometryFamily, to: types.BytesFamily, volatility: VolatilityImmutable},
	{from: types.GeographyFamily, to: types.BytesFamily, volatility: VolatilityImmutable},

	// Casts to DateFamily.
	{from: types.UnknownFamily, to: types.DateFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.DateFamily, volatility: VolatilityStable},
	{from: types.CollatedStringFamily, to: types.DateFamily, volatility: VolatilityStable},
	{from: types.DateFamily, to: types.DateFamily, volatility: VolatilityImmutable},
	{from: types.TimestampFamily, to: types.DateFamily, volatility: VolatilityImmutable},
	{from: types.TimestampTZFamily, to: types.DateFamily, volatility: VolatilityStable},
	{from: types.IntFamily, to: types.DateFamily, volatility: VolatilityImmutable},

	// Casts to TimeFamily.
	{from: types.UnknownFamily, to: types.TimeFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.TimeFamily, volatility: VolatilityStable},
	{from: types.CollatedStringFamily, to: types.TimeFamily, volatility: VolatilityStable},
	{from: types.TimeFamily, to: types.TimeFamily, volatility: VolatilityImmutable},
	{from: types.TimeTZFamily, to: types.TimeFamily, volatility: VolatilityImmutable},
	{from: types.TimestampFamily, to: types.TimeFamily, volatility: VolatilityImmutable},
	{from: types.TimestampTZFamily, to: types.TimeFamily, volatility: VolatilityStable},
	{from: types.IntervalFamily, to: types.TimeFamily, volatility: VolatilityImmutable},

	// Casts to TimeTZFamily.
	{from: types.UnknownFamily, to: types.TimeTZFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.TimeTZFamily, volatility: VolatilityStable},
	{from: types.CollatedStringFamily, to: types.TimeTZFamily, volatility: VolatilityStable},
	{from: types.TimeFamily, to: types.TimeTZFamily, volatility: VolatilityStable},
	{from: types.TimeTZFamily, to: types.TimeTZFamily, volatility: VolatilityImmutable},
	{from: types.TimestampTZFamily, to: types.TimeTZFamily, volatility: VolatilityStable},

	// Casts to TimestampFamily.
	{from: types.UnknownFamily, to: types.TimestampFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.TimestampFamily, volatility: VolatilityStable},
	{from: types.CollatedStringFamily, to: types.TimestampFamily, volatility: VolatilityStable},
	{from: types.DateFamily, to: types.TimestampFamily, volatility: VolatilityImmutable},
	{from: types.TimestampFamily, to: types.TimestampFamily, volatility: VolatilityImmutable},
	{from: types.TimestampTZFamily, to: types.TimestampFamily, volatility: VolatilityStable},
	{from: types.IntFamily, to: types.TimestampFamily, volatility: VolatilityImmutable},

	// Casts to TimestampTZFamily.
	{from: types.UnknownFamily, to: types.TimestampTZFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.TimestampTZFamily, volatility: VolatilityStable},
	{from: types.CollatedStringFamily, to: types.TimestampTZFamily, volatility: VolatilityStable},
	{from: types.DateFamily, to: types.TimestampTZFamily, volatility: VolatilityStable},
	{from: types.TimestampFamily, to: types.TimestampTZFamily, volatility: VolatilityStable},
	{from: types.TimestampTZFamily, to: types.TimestampTZFamily, volatility: VolatilityImmutable},
	{from: types.IntFamily, to: types.TimestampTZFamily, volatility: VolatilityImmutable},

	// Casts to IntervalFamily.
	{from: types.UnknownFamily, to: types.IntervalFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.IntervalFamily, volatility: VolatilityImmutable},
	{from: types.CollatedStringFamily, to: types.IntervalFamily, volatility: VolatilityImmutable},
	{from: types.IntFamily, to: types.IntervalFamily, volatility: VolatilityImmutable},
	{from: types.TimeFamily, to: types.IntervalFamily, volatility: VolatilityImmutable},
	{from: types.IntervalFamily, to: types.IntervalFamily, volatility: VolatilityImmutable},
	{from: types.FloatFamily, to: types.IntervalFamily, volatility: VolatilityImmutable},
	{from: types.DecimalFamily, to: types.IntervalFamily, volatility: VolatilityImmutable},

	// Casts to OidFamily.
	{from: types.UnknownFamily, to: types.OidFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.OidFamily, volatility: VolatilityStable},
	{from: types.CollatedStringFamily, to: types.OidFamily, volatility: VolatilityStable},
	{from: types.IntFamily, to: types.OidFamily, volatility: VolatilityStable, ignoreVolatilityCheck: true},
	{from: types.OidFamily, to: types.OidFamily, volatility: VolatilityStable},

	// Casts to UuidFamily.
	{from: types.UnknownFamily, to: types.UuidFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.UuidFamily, volatility: VolatilityImmutable},
	{from: types.CollatedStringFamily, to: types.UuidFamily, volatility: VolatilityImmutable},
	{from: types.BytesFamily, to: types.UuidFamily, volatility: VolatilityImmutable},
	{from: types.UuidFamily, to: types.UuidFamily, volatility: VolatilityImmutable},

	// Casts to INetFamily.
	{from: types.UnknownFamily, to: types.INetFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.INetFamily, volatility: VolatilityImmutable},
	{from: types.CollatedStringFamily, to: types.INetFamily, volatility: VolatilityImmutable},
	{from: types.INetFamily, to: types.INetFamily, volatility: VolatilityImmutable},

	// Casts to ArrayFamily.
	{from: types.UnknownFamily, to: types.ArrayFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.ArrayFamily, volatility: VolatilityStable},

	// Casts to JsonFamily.
	{from: types.UnknownFamily, to: types.JsonFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.JsonFamily, volatility: VolatilityImmutable},
	{from: types.JsonFamily, to: types.JsonFamily, volatility: VolatilityImmutable},
	{from: types.GeometryFamily, to: types.JsonFamily, volatility: VolatilityImmutable},
	{from: types.GeographyFamily, to: types.JsonFamily, volatility: VolatilityImmutable},

	// Casts to EnumFamily.
	{from: types.UnknownFamily, to: types.EnumFamily, volatility: VolatilityImmutable},
	{from: types.StringFamily, to: types.EnumFamily, volatility: VolatilityImmutable},
	{from: types.EnumFamily, to: types.EnumFamily, volatility: VolatilityImmutable},
	{from: types.BytesFamily, to: types.EnumFamily, volatility: VolatilityImmutable},

	// Casts to TupleFamily.
	{from: types.UnknownFamily, to: types.TupleFamily, volatility: VolatilityImmutable},
}

type castsMapKey struct {
	from, to types.Family
}

var castsMap map[castsMapKey]*castInfo

func init() {
	castsMap = make(map[castsMapKey]*castInfo, len(validCasts))
	for i := range validCasts {
		c := &validCasts[i]

		// Initialize counter.
		c.counter = sqltelemetry.CastOpCounter(c.from.Name(), c.to.Name())

		key := castsMapKey{from: c.from, to: c.to}
		castsMap[key] = c
	}
}

// lookupCast returns the information for a valid cast.
// Returns nil if this is not a valid cast.
func lookupCast(from, to types.Family) *castInfo {
	return castsMap[castsMapKey{from: from, to: to}]
}

// LookupCastVolatility returns the volatility of a valid cast.
func LookupCastVolatility(from, to *types.T) (_ Volatility, ok bool) {
	fromFamily := from.Family()
	toFamily := to.Family()
	// Special case for casting between arrays.
	if fromFamily == types.ArrayFamily && toFamily == types.ArrayFamily {
		return LookupCastVolatility(from.ArrayContents(), to.ArrayContents())
	}
	cast := lookupCast(fromFamily, toFamily)
	if cast == nil {
		return 0, false
	}
	return cast.volatility, true
}

// PerformCast performs a cast from the provided Datum to the specified
// types.T.
func PerformCast(ctx *EvalContext, d Datum, t *types.T) (Datum, error) {
	switch t.Family() {
	case types.BitFamily:
		switch v := d.(type) {
		case *DBitArray:
			if t.Width() == 0 || v.BitLen() == uint(t.Width()) {
				return d, nil
			}
			var a DBitArray
			switch t.Oid() {
			case oid.T_varbit:
				// VARBITs do not have padding attached.
				a.BitArray = v.BitArray.Clone()
				if uint(t.Width()) < a.BitArray.BitLen() {
					a.BitArray = a.BitArray.ToWidth(uint(t.Width()))
				}
			default:
				a.BitArray = v.BitArray.Clone().ToWidth(uint(t.Width()))
			}
			return &a, nil
		case *DInt:
			return NewDBitArrayFromInt(int64(*v), uint(t.Width()))
		case *DString:
			res, err := bitarray.Parse(string(*v))
			if err != nil {
				return nil, err
			}
			if t.Width() > 0 {
				res = res.ToWidth(uint(t.Width()))
			}
			return &DBitArray{BitArray: res}, nil
		case *DCollatedString:
			res, err := bitarray.Parse(v.Contents)
			if err != nil {
				return nil, err
			}
			if t.Width() > 0 {
				res = res.ToWidth(uint(t.Width()))
			}
			return &DBitArray{BitArray: res}, nil
		}

	case types.BoolFamily:
		switch v := d.(type) {
		case *DBool:
			return d, nil
		case *DInt:
			return MakeDBool(*v != 0), nil
		case *DFloat:
			return MakeDBool(*v != 0), nil
		case *DDecimal:
			return MakeDBool(v.Sign() != 0), nil
		case *DString:
			return ParseDBool(string(*v))
		case *DCollatedString:
			return ParseDBool(v.Contents)
		}

	case types.IntFamily:
		var res *DInt
		switch v := d.(type) {
		case *DBitArray:
			res = v.AsDInt(uint(t.Width()))
		case *DBool:
			if *v {
				res = NewDInt(1)
			} else {
				res = DZero
			}
		case *DInt:
			// TODO(knz): enforce the coltype width here.
			res = v
		case *DFloat:
			f := float64(*v)
			// Use `<=` and `>=` here instead of just `<` and `>` because when
			// math.MaxInt64 and math.MinInt64 are converted to float64s, they are
			// rounded to numbers with larger absolute values. Note that the first
			// next FP value after and strictly greater than float64(math.MinInt64)
			// is -9223372036854774784 (= float64(math.MinInt64)+513) and the first
			// previous value and strictly smaller than float64(math.MaxInt64)
			// is 9223372036854774784 (= float64(math.MaxInt64)-513), and both are
			// convertible to int without overflow.
			if math.IsNaN(f) || f <= float64(math.MinInt64) || f >= float64(math.MaxInt64) {
				return nil, ErrIntOutOfRange
			}
			res = NewDInt(DInt(f))
		case *DDecimal:
			d := ctx.getTmpDec()
			_, err := DecimalCtx.RoundToIntegralValue(d, &v.Decimal)
			if err != nil {
				return nil, err
			}
			i, err := d.Int64()
			if err != nil {
				return nil, ErrIntOutOfRange
			}
			res = NewDInt(DInt(i))
		case *DString:
			var err error
			if res, err = ParseDInt(string(*v)); err != nil {
				return nil, err
			}
		case *DCollatedString:
			var err error
			if res, err = ParseDInt(v.Contents); err != nil {
				return nil, err
			}
		case *DTimestamp:
			res = NewDInt(DInt(v.Unix()))
		case *DTimestampTZ:
			res = NewDInt(DInt(v.Unix()))
		case *DDate:
			// TODO(mjibson): This cast is unsupported by postgres. Should we remove ours?
			if !v.IsFinite() {
				return nil, ErrIntOutOfRange
			}
			res = NewDInt(DInt(v.UnixEpochDays()))
		case *DInterval:
			iv, ok := v.AsInt64()
			if !ok {
				return nil, ErrIntOutOfRange
			}
			res = NewDInt(DInt(iv))
		case *DOid:
			res = &v.DInt
		}
		if res != nil {
			return res, nil
		}

	case types.EnumFamily:
		switch v := d.(type) {
		case *DString:
			return MakeDEnumFromLogicalRepresentation(t, string(*v))
		case *DBytes:
			return MakeDEnumFromPhysicalRepresentation(t, []byte(*v))
		case *DEnum:
			return d, nil
		}

	case types.FloatFamily:
		switch v := d.(type) {
		case *DBool:
			if *v {
				return NewDFloat(1), nil
			}
			return NewDFloat(0), nil
		case *DInt:
			return NewDFloat(DFloat(*v)), nil
		case *DFloat:
			return d, nil
		case *DDecimal:
			f, err := v.Float64()
			if err != nil {
				return nil, ErrFloatOutOfRange
			}
			return NewDFloat(DFloat(f)), nil
		case *DString:
			return ParseDFloat(string(*v))
		case *DCollatedString:
			return ParseDFloat(v.Contents)
		case *DTimestamp:
			micros := float64(v.Nanosecond() / int(time.Microsecond))
			return NewDFloat(DFloat(float64(v.Unix()) + micros*1e-6)), nil
		case *DTimestampTZ:
			micros := float64(v.Nanosecond() / int(time.Microsecond))
			return NewDFloat(DFloat(float64(v.Unix()) + micros*1e-6)), nil
		case *DDate:
			// TODO(mjibson): This cast is unsupported by postgres. Should we remove ours?
			if !v.IsFinite() {
				return nil, ErrFloatOutOfRange
			}
			return NewDFloat(DFloat(float64(v.UnixEpochDays()))), nil
		case *DInterval:
			return NewDFloat(DFloat(v.AsFloat64())), nil
		}

	case types.DecimalFamily:
		var dd DDecimal
		var err error
		unset := false
		switch v := d.(type) {
		case *DBool:
			if *v {
				dd.SetInt64(1)
			}
		case *DInt:
			dd.SetInt64(int64(*v))
		case *DDate:
			// TODO(mjibson): This cast is unsupported by postgres. Should we remove ours?
			if !v.IsFinite() {
				return nil, errDecOutOfRange
			}
			dd.SetInt64(v.UnixEpochDays())
		case *DFloat:
			_, err = dd.SetFloat64(float64(*v))
		case *DDecimal:
			// Small optimization to avoid copying into dd in normal case.
			if t.Precision() == 0 {
				return d, nil
			}
			dd.Set(&v.Decimal)
		case *DString:
			err = dd.SetString(string(*v))
		case *DCollatedString:
			err = dd.SetString(v.Contents)
		case *DTimestamp:
			val := &dd.Coeff
			val.SetInt64(v.Unix())
			val.Mul(val, big10E6)
			micros := v.Nanosecond() / int(time.Microsecond)
			val.Add(val, big.NewInt(int64(micros)))
			dd.Exponent = -6
		case *DTimestampTZ:
			val := &dd.Coeff
			val.SetInt64(v.Unix())
			val.Mul(val, big10E6)
			micros := v.Nanosecond() / int(time.Microsecond)
			val.Add(val, big.NewInt(int64(micros)))
			dd.Exponent = -6
		case *DInterval:
			v.AsBigInt(&dd.Coeff)
			dd.Exponent = -9
		default:
			unset = true
		}
		if err != nil {
			return nil, err
		}
		if !unset {
			// dd.Coeff must be positive. If it was set to a negative value
			// above, transfer the sign to dd.Negative.
			if dd.Coeff.Sign() < 0 {
				dd.Negative = true
				dd.Coeff.Abs(&dd.Coeff)
			}
			err = LimitDecimalWidth(&dd.Decimal, int(t.Precision()), int(t.Scale()))
			return &dd, err
		}

	case types.StringFamily, types.CollatedStringFamily:
		var s string
		switch t := d.(type) {
		case *DBitArray:
			s = t.BitArray.String()
		case *DFloat:
			s = strconv.FormatFloat(float64(*t), 'g',
				ctx.SessionData.DataConversion.GetFloatPrec(), 64)
		case *DBool, *DInt, *DDecimal:
			s = d.String()
		case *DTimestamp, *DDate, *DTime, *DTimeTZ, *DGeography, *DGeometry:
			s = AsStringWithFlags(d, FmtBareStrings)
		case *DTimestampTZ:
			// Convert to context timezone for correct display.
			ts, err := MakeDTimestampTZ(t.In(ctx.GetLocation()), time.Microsecond)
			if err != nil {
				return nil, err
			}
			s = AsStringWithFlags(
				ts,
				FmtBareStrings,
			)
		case *DTuple:
			s = AsStringWithFlags(d, FmtPgwireText)
		case *DArray:
			s = AsStringWithFlags(d, FmtPgwireText)
		case *DInterval:
			// When converting an interval to string, we need a string representation
			// of the duration (e.g. "5s") and not of the interval itself (e.g.
			// "INTERVAL '5s'").
			s = t.ValueAsString()
		case *DUuid:
			s = t.UUID.String()
		case *DIPAddr:
			s = AsStringWithFlags(d, FmtBareStrings)
		case *DString:
			s = string(*t)
		case *DCollatedString:
			s = t.Contents
		case *DBytes:
			s = lex.EncodeByteArrayToRawBytes(string(*t),
				ctx.SessionData.DataConversion.BytesEncodeFormat, false /* skipHexPrefix */)
		case *DOid:
			s = t.String()
		case *DJSON:
			s = t.JSON.String()
		case *DEnum:
			s = t.LogicalRep
		}
		switch t.Family() {
		case types.StringFamily:
			if t.Oid() == oid.T_name {
				return NewDName(s), nil
			}

			// bpchar types truncate trailing whitespace.
			if t.Oid() == oid.T_bpchar {
				s = strings.TrimRight(s, " ")
			}

			// If the string type specifies a limit we truncate to that limit:
			//   'hello'::CHAR(2) -> 'he'
			// This is true of all the string type variants.
			if t.Width() > 0 {
				s = util.TruncateString(s, int(t.Width()))
			}
			return NewDString(s), nil
		case types.CollatedStringFamily:
			// bpchar types truncate trailing whitespace.
			if t.Oid() == oid.T_bpchar {
				s = strings.TrimRight(s, " ")
			}
			// Ditto truncation like for TString.
			if t.Width() > 0 {
				s = util.TruncateString(s, int(t.Width()))
			}
			return NewDCollatedString(s, t.Locale(), &ctx.CollationEnv)
		}

	case types.BytesFamily:
		switch t := d.(type) {
		case *DString:
			return ParseDByte(string(*t))
		case *DCollatedString:
			return NewDBytes(DBytes(t.Contents)), nil
		case *DUuid:
			return NewDBytes(DBytes(t.GetBytes())), nil
		case *DBytes:
			return d, nil
		case *DGeography:
			return NewDBytes(DBytes(t.Geography.EWKB())), nil
		case *DGeometry:
			return NewDBytes(DBytes(t.Geometry.EWKB())), nil
		}

	case types.UuidFamily:
		switch t := d.(type) {
		case *DString:
			return ParseDUuidFromString(string(*t))
		case *DCollatedString:
			return ParseDUuidFromString(t.Contents)
		case *DBytes:
			return ParseDUuidFromBytes([]byte(*t))
		case *DUuid:
			return d, nil
		}

	case types.INetFamily:
		switch t := d.(type) {
		case *DString:
			return ParseDIPAddrFromINetString(string(*t))
		case *DCollatedString:
			return ParseDIPAddrFromINetString(t.Contents)
		case *DIPAddr:
			return d, nil
		}

	case types.GeographyFamily:
		switch d := d.(type) {
		case *DString:
			return ParseDGeography(string(*d))
		case *DCollatedString:
			return ParseDGeography(d.Contents)
		case *DGeography:
			if err := geo.GeospatialTypeFitsColumnMetadata(
				d.Geography,
				t.InternalType.GeoMetadata.SRID,
				t.InternalType.GeoMetadata.ShapeType,
			); err != nil {
				return nil, err
			}
			return d, nil
		case *DGeometry:
			g, err := d.AsGeography()
			if err != nil {
				return nil, err
			}
			if err := geo.GeospatialTypeFitsColumnMetadata(
				g,
				t.InternalType.GeoMetadata.SRID,
				t.InternalType.GeoMetadata.ShapeType,
			); err != nil {
				return nil, err
			}
			return &DGeography{g}, nil
		case *DJSON:
			t, err := d.AsText()
			if err != nil {
				return nil, err
			}
			g, err := geo.ParseGeographyFromGeoJSON([]byte(*t))
			if err != nil {
				return nil, err
			}
			return &DGeography{g}, nil
		case *DBytes:
			g, err := geo.ParseGeographyFromEWKB(geopb.EWKB(*d))
			if err != nil {
				return nil, err
			}
			return &DGeography{g}, nil
		}
	case types.GeometryFamily:
		switch d := d.(type) {
		case *DString:
			return ParseDGeometry(string(*d))
		case *DCollatedString:
			return ParseDGeometry(d.Contents)
		case *DGeometry:
			if err := geo.GeospatialTypeFitsColumnMetadata(
				d.Geometry,
				t.InternalType.GeoMetadata.SRID,
				t.InternalType.GeoMetadata.ShapeType,
			); err != nil {
				return nil, err
			}
			return d, nil
		case *DGeography:
			if err := geo.GeospatialTypeFitsColumnMetadata(
				d.Geography,
				t.InternalType.GeoMetadata.SRID,
				t.InternalType.GeoMetadata.ShapeType,
			); err != nil {
				return nil, err
			}
			g, err := d.AsGeometry()
			if err != nil {
				return nil, err
			}
			return &DGeometry{g}, nil
		case *DJSON:
			t, err := d.AsText()
			if err != nil {
				return nil, err
			}
			g, err := geo.ParseGeometryFromGeoJSON([]byte(*t))
			if err != nil {
				return nil, err
			}
			return &DGeometry{g}, nil
		case *DBytes:
			g, err := geo.ParseGeometryFromEWKB(geopb.EWKB(*d))
			if err != nil {
				return nil, err
			}
			return &DGeometry{g}, nil
		}

	case types.DateFamily:
		switch d := d.(type) {
		case *DString:
			res, _, err := ParseDDate(ctx, string(*d))
			return res, err
		case *DCollatedString:
			res, _, err := ParseDDate(ctx, d.Contents)
			return res, err
		case *DDate:
			return d, nil
		case *DInt:
			// TODO(mjibson): This cast is unsupported by postgres. Should we remove ours?
			t, err := pgdate.MakeDateFromUnixEpoch(int64(*d))
			return NewDDate(t), err
		case *DTimestampTZ:
			return NewDDateFromTime(d.Time.In(ctx.GetLocation()))
		case *DTimestamp:
			return NewDDateFromTime(d.Time)
		}

	case types.TimeFamily:
		roundTo := TimeFamilyPrecisionToRoundDuration(t.Precision())
		switch d := d.(type) {
		case *DString:
			res, _, err := ParseDTime(ctx, string(*d), roundTo)
			return res, err
		case *DCollatedString:
			res, _, err := ParseDTime(ctx, d.Contents, roundTo)
			return res, err
		case *DTime:
			return d.Round(roundTo), nil
		case *DTimeTZ:
			return MakeDTime(d.TimeOfDay.Round(roundTo)), nil
		case *DTimestamp:
			return MakeDTime(timeofday.FromTime(d.Time).Round(roundTo)), nil
		case *DTimestampTZ:
			// Strip time zone. Times don't carry their location.
			stripped, err := d.stripTimeZone(ctx)
			if err != nil {
				return nil, err
			}
			return MakeDTime(timeofday.FromTime(stripped.Time).Round(roundTo)), nil
		case *DInterval:
			return MakeDTime(timeofday.Min.Add(d.Duration).Round(roundTo)), nil
		}

	case types.TimeTZFamily:
		roundTo := TimeFamilyPrecisionToRoundDuration(t.Precision())
		switch d := d.(type) {
		case *DString:
			res, _, err := ParseDTimeTZ(ctx, string(*d), roundTo)
			return res, err
		case *DCollatedString:
			res, _, err := ParseDTimeTZ(ctx, d.Contents, roundTo)
			return res, err
		case *DTime:
			return NewDTimeTZFromLocation(timeofday.TimeOfDay(*d).Round(roundTo), ctx.GetLocation()), nil
		case *DTimeTZ:
			return d.Round(roundTo), nil
		case *DTimestampTZ:
			return NewDTimeTZFromTime(d.Time.In(ctx.GetLocation()).Round(roundTo)), nil
		}

	case types.TimestampFamily:
		roundTo := TimeFamilyPrecisionToRoundDuration(t.Precision())
		// TODO(knz): Timestamp from float, decimal.
		switch d := d.(type) {
		case *DString:
			res, _, err := ParseDTimestamp(ctx, string(*d), roundTo)
			return res, err
		case *DCollatedString:
			res, _, err := ParseDTimestamp(ctx, d.Contents, roundTo)
			return res, err
		case *DDate:
			t, err := d.ToTime()
			if err != nil {
				return nil, err
			}
			return MakeDTimestamp(t, roundTo)
		case *DInt:
			return MakeDTimestamp(timeutil.Unix(int64(*d), 0), roundTo)
		case *DTimestamp:
			return d.Round(roundTo)
		case *DTimestampTZ:
			// Strip time zone. Timestamps don't carry their location.
			stripped, err := d.stripTimeZone(ctx)
			if err != nil {
				return nil, err
			}
			return stripped.Round(roundTo)
		}

	case types.TimestampTZFamily:
		roundTo := TimeFamilyPrecisionToRoundDuration(t.Precision())
		// TODO(knz): TimestampTZ from float, decimal.
		switch d := d.(type) {
		case *DString:
			res, _, err := ParseDTimestampTZ(ctx, string(*d), roundTo)
			return res, err
		case *DCollatedString:
			res, _, err := ParseDTimestampTZ(ctx, d.Contents, roundTo)
			return res, err
		case *DDate:
			t, err := d.ToTime()
			if err != nil {
				return nil, err
			}
			_, before := t.Zone()
			_, after := t.In(ctx.GetLocation()).Zone()
			return MakeDTimestampTZ(t.Add(time.Duration(before-after)*time.Second), roundTo)
		case *DTimestamp:
			_, before := d.Time.Zone()
			_, after := d.Time.In(ctx.GetLocation()).Zone()
			return MakeDTimestampTZ(d.Time.Add(time.Duration(before-after)*time.Second), roundTo)
		case *DInt:
			return MakeDTimestampTZ(timeutil.Unix(int64(*d), 0), roundTo)
		case *DTimestampTZ:
			return d.Round(roundTo)
		}

	case types.IntervalFamily:
		itm, err := t.IntervalTypeMetadata()
		if err != nil {
			return nil, err
		}
		switch v := d.(type) {
		case *DString:
			return ParseDIntervalWithTypeMetadata(string(*v), itm)
		case *DCollatedString:
			return ParseDIntervalWithTypeMetadata(v.Contents, itm)
		case *DInt:
			return NewDInterval(duration.FromInt64(int64(*v)), itm), nil
		case *DFloat:
			return NewDInterval(duration.FromFloat64(float64(*v)), itm), nil
		case *DTime:
			return NewDInterval(duration.MakeDuration(int64(*v)*1000, 0, 0), itm), nil
		case *DDecimal:
			d := ctx.getTmpDec()
			dnanos := v.Decimal
			dnanos.Exponent += 9
			// We need HighPrecisionCtx because duration values can contain
			// upward of 35 decimal digits and DecimalCtx only provides 25.
			_, err := HighPrecisionCtx.Quantize(d, &dnanos, 0)
			if err != nil {
				return nil, err
			}
			if dnanos.Negative {
				d.Coeff.Neg(&d.Coeff)
			}
			dv, ok := duration.FromBigInt(&d.Coeff)
			if !ok {
				return nil, errDecOutOfRange
			}
			return NewDInterval(dv, itm), nil
		case *DInterval:
			return NewDInterval(v.Duration, itm), nil
		}
	case types.JsonFamily:
		switch v := d.(type) {
		case *DString:
			return ParseDJSON(string(*v))
		case *DJSON:
			return v, nil
		case *DGeography:
			j, err := geo.EWKBToGeoJSON(v.Geography.EWKB(), -1, geo.EWKBToGeoJSONFlagZero)
			if err != nil {
				return nil, err
			}
			return ParseDJSON(string(j))
		case *DGeometry:
			j, err := geo.EWKBToGeoJSON(v.Geometry.EWKB(), -1, geo.EWKBToGeoJSONFlagZero)
			if err != nil {
				return nil, err
			}
			return ParseDJSON(string(j))
		}
	case types.ArrayFamily:
		switch v := d.(type) {
		case *DString:
			res, _, err := ParseDArrayFromString(ctx, string(*v), t.ArrayContents())
			return res, err
		case *DArray:
			dcast := NewDArray(t.ArrayContents())
			for _, e := range v.Array {
				ecast := DNull
				if e != DNull {
					var err error
					ecast, err = PerformCast(ctx, e, t.ArrayContents())
					if err != nil {
						return nil, err
					}
				}

				if err := dcast.Append(ecast); err != nil {
					return nil, err
				}
			}
			return dcast, nil
		}
	case types.OidFamily:
		switch v := d.(type) {
		case *DOid:
			switch t.Oid() {
			case oid.T_oid:
				return &DOid{semanticType: t, DInt: v.DInt}, nil
			case oid.T_regtype:
				// Mapping an oid to a regtype is easy: we have a hardcoded map.
				typ, ok := types.OidToType[oid.Oid(v.DInt)]
				ret := &DOid{semanticType: t, DInt: v.DInt}
				if !ok {
					return ret, nil
				}
				ret.name = typ.PGName()
				return ret, nil
			default:
				oid, err := queryOid(ctx, t, v)
				if err != nil {
					oid = NewDOid(v.DInt)
					oid.semanticType = t
				}
				return oid, nil
			}
		case *DInt:
			switch t.Oid() {
			case oid.T_oid:
				return &DOid{semanticType: t, DInt: *v}, nil
			default:
				tmpOid := NewDOid(*v)
				oid, err := queryOid(ctx, t, tmpOid)
				if err != nil {
					oid = tmpOid
					oid.semanticType = t
				}
				return oid, nil
			}
		case *DString:
			s := string(*v)
			// Trim whitespace and unwrap outer quotes if necessary.
			// This is required to mimic postgres.
			s = strings.TrimSpace(s)
			origS := s
			if len(s) > 1 && s[0] == '"' && s[len(s)-1] == '"' {
				s = s[1 : len(s)-1]
			}

			switch t.Oid() {
			case oid.T_oid:
				i, err := ParseDInt(s)
				if err != nil {
					return nil, err
				}
				return &DOid{semanticType: t, DInt: *i}, nil
			case oid.T_regproc, oid.T_regprocedure:
				// Trim procedure type parameters, e.g. `max(int)` becomes `max`.
				// Postgres only does this when the cast is ::regprocedure, but we're
				// going to always do it.
				// We additionally do not yet implement disambiguation based on type
				// parameters: we return the match iff there is exactly one.
				s = pgSignatureRegexp.ReplaceAllString(s, "$1")
				// Resolve function name.
				substrs := strings.Split(s, ".")
				if len(substrs) > 3 {
					// A fully qualified function name in pg's dialect can contain
					// at most 3 parts: db.schema.funname.
					// For example mydb.pg_catalog.max().
					// Anything longer is always invalid.
					return nil, pgerror.Newf(pgcode.Syntax,
						"invalid function name: %s", s)
				}
				name := UnresolvedName{NumParts: len(substrs)}
				for i := 0; i < len(substrs); i++ {
					name.Parts[i] = substrs[len(substrs)-1-i]
				}
				funcDef, err := name.ResolveFunction(ctx.SessionData.SearchPath)
				if err != nil {
					return nil, err
				}
				return queryOid(ctx, t, NewDString(funcDef.Name))
			case oid.T_regtype:
				parsedTyp, err := ctx.Planner.ParseType(s)
				if err == nil {
					return &DOid{
						semanticType: t,
						DInt:         DInt(parsedTyp.Oid()),
						name:         parsedTyp.SQLStandardName(),
					}, nil
				}
				// Fall back to searching pg_type, since we don't provide syntax for
				// every postgres type that we understand OIDs for.
				// Trim type modifiers, e.g. `numeric(10,3)` becomes `numeric`.
				s = pgSignatureRegexp.ReplaceAllString(s, "$1")
				dOid, missingTypeErr := queryOid(ctx, t, NewDString(s))
				if missingTypeErr == nil {
					return dOid, missingTypeErr
				}
				// Fall back to some special cases that we support for compatibility
				// only. Client use syntax like 'sometype'::regtype to produce the oid
				// for a type that they want to search a catalog table for. Since we
				// don't support that type, we return an artificial OID that will never
				// match anything.
				switch s {
				// We don't support triggers, but some tools search for them
				// specifically.
				case "trigger":
				default:
					return nil, missingTypeErr
				}
				return &DOid{
					semanticType: t,
					// Types we don't support get OID -1, so they won't match anything
					// in catalogs.
					DInt: -1,
					name: s,
				}, nil

			case oid.T_regclass:
				tn, err := ctx.Planner.ParseQualifiedTableName(origS)
				if err != nil {
					return nil, err
				}
				id, err := ctx.Planner.ResolveTableName(ctx.Ctx(), tn)
				if err != nil {
					return nil, err
				}
				return &DOid{
					semanticType: t,
					DInt:         DInt(id),
					name:         tn.ObjectName.String(),
				}, nil
			default:
				return queryOid(ctx, t, NewDString(s))
			}
		}
	}

	return nil, pgerror.Newf(
		pgcode.CannotCoerce, "invalid cast: %s -> %s", d.ResolvedType(), t)
}
