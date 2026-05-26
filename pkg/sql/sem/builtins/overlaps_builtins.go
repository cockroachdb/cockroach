// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/errors"
)

// overlapsOverloadVolatility is to define a certain overload for the overlaps
// builtin, and also the function's volatility for this overload.
type overlapsOverloadVolatility struct {
	paramTypes []*types.T
	volatility volatility.V
}

var (
	validOverlapsOverloadVolatility = []overlapsOverloadVolatility{
		{[]*types.T{types.Timestamp, types.Timestamp, types.Timestamp, types.Timestamp}, volatility.Immutable},
		{[]*types.T{types.Timestamp, types.Interval, types.Timestamp, types.Interval}, volatility.Immutable},
		{[]*types.T{types.TimeTZ, types.TimeTZ, types.TimeTZ, types.TimeTZ}, volatility.Immutable},
		{[]*types.T{types.TimeTZ, types.Interval, types.TimeTZ, types.Interval}, volatility.Immutable},
		{[]*types.T{types.Time, types.Time, types.Time, types.Time}, volatility.Immutable},
		{[]*types.T{types.Time, types.Interval, types.Time, types.Interval}, volatility.Immutable},
		{[]*types.T{types.TimestampTZ, types.TimestampTZ, types.TimestampTZ, types.TimestampTZ}, volatility.Immutable},
		{[]*types.T{types.TimestampTZ, types.Interval, types.TimestampTZ, types.Interval}, volatility.Stable},
		{[]*types.T{types.Date, types.Date, types.Date, types.Date}, volatility.Immutable},
		{[]*types.T{types.Date, types.Interval, types.Date, types.Interval}, volatility.Immutable},
	}
)

func init() {
	for k, v := range overlapsBuiltins {
		const enforceClass = true
		registerBuiltin(k, v, tree.NormalClass, enforceClass)
	}
}

var overlapsBuiltins = map[string]builtinDefinition{
	"overlaps": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryDateAndTime,
		},
		makeOverlapsOverloads()...,
	),
}

// makeOverlapsOverloads returns a list of tree.Overload for the
// overlaps builtin function.
func makeOverlapsOverloads() []tree.Overload {
	var res []tree.Overload
	for _, in := range validOverlapsOverloadVolatility {
		params := in.paramTypes
		s1, e1, s2, e2 := params[0], params[1], params[2], params[3]
		res = append(res,
			tree.Overload{
				Types: tree.ParamTypes{
					{Name: "s1", Typ: s1},
					{Name: "e1", Typ: e1},
					{Name: "s1", Typ: s2},
					{Name: "e2", Typ: e2},
				},
				ReturnType: tree.FixedReturnType(types.Bool),
				Fn:         overlapsBuiltinFunc,
				Info:       "Returns if two time periods (defined by their endpoints) overlap.",
				Volatility: in.volatility,
			},
		)
	}
	return res
}

// overlapsBuiltinFunc checks if two time periods overlaps, and returns possible
// error.
func overlapsBuiltinFunc(
	ctx context.Context, evalCtx *eval.Context, args tree.Datums,
) (tree.Datum, error) {
	var err error
	s1, e1, s2, e2 := args[0], args[1], args[2], args[3]
	s1, e1, err = normalizeTimePeriodToEndpoints(evalCtx, s1, e1)
	if err != nil {
		return nil, err
	}
	s2, e2, err = normalizeTimePeriodToEndpoints(evalCtx, s2, e2)
	if err != nil {
		return nil, err
	}
	return evalOverlaps(ctx, evalCtx, s1, e1, s2, e2)
}

// evalOverlaps checks if two intervals overlap, return a bool
// and a possible error. The passed `args` parameter consists of
// [start/end_of_interval_1, end/start_of_interval_2, start/end_of_interval_2,
// end/start_of_interval_2]. When a pair of values is provided, either the start
// or the end can be written first; OVERLAPS automatically takes the earlier
// value of the pair as the start.
// Each interval is considered to represent the half-open
// interval start <= time < end, unless start and end are equal in which case
// it represents that single time instant.
// `s` represents `interval start`, `e` represents `interval end`.
// `1/2` represents interval 1/2.
func evalOverlaps(
	ctx context.Context,
	evalCtx *eval.Context,
	s1 tree.Datum,
	e1 tree.Datum,
	s2 tree.Datum,
	e2 tree.Datum,
) (tree.Datum, error) {
	compS1E1, err := s1.Compare(ctx, evalCtx, e1)
	if err != nil {
		return nil, err
	}
	if compS1E1 > 0 {
		s1, e1 = e1, s1
	}

	compS2E2, err := s2.Compare(ctx, evalCtx, e2)
	if err != nil {
		return nil, err
	}
	if compS2E2 > 0 {
		s2, e2 = e2, s2
	}

	compS1S2, err := s1.Compare(ctx, evalCtx, s2)
	if err != nil {
		return nil, err
	}
	switch compS1S2 {

	// Case s1 > s2.
	case 1:
		compS1E2, err := s1.Compare(ctx, evalCtx, e2)
		if err != nil {
			return nil, err
		}
		if compS1E2 < 0 {
			return tree.DBoolTrue, nil
		}

		// We had s1 <= e1 above, and we just found s1 >= e2, hence e1 >= e2.
		return tree.DBoolFalse, nil

	// Case s1 < s2.
	case -1:
		compS2E1, err := s2.Compare(ctx, evalCtx, e1)
		if err != nil {
			return nil, err
		}
		if compS2E1 < 0 {
			return tree.DBoolTrue, nil
		}

		// We had s2 <= e2 above, and we just found s2 >= e1, hence e2 >= e1.
		return tree.DBoolFalse, nil

	// Case s1 == s2.
	default:
		return tree.DBoolTrue, nil
	}
}

// normalizeTimePeriodToEndpoints is to normalize a time period to a
// representation of two endpoints.
func normalizeTimePeriodToEndpoints(
	evalCtx *eval.Context, s tree.Datum, e tree.Datum,
) (tree.Datum, tree.Datum, error) {
	var err error
	switch eType := e.(type) {
	case *tree.DDate, *tree.DTimestamp, *tree.DTimestampTZ, *tree.DTime, *tree.DTimeTZ:
	case *tree.DInterval:
		// If the time period is represented as with a start time and an interval,
		// add the interval's duration to the start time to get the end time.
		var et tree.Datum

		switch sType := s.(type) {
		case *tree.DDate:
			startTime, err := sType.ToTime()
			if err != nil {
				return nil, nil, err
			}
			et, err = tree.MakeDTimestamp(duration.Add(startTime, eType.Duration), time.Microsecond)
			if err != nil {
				return nil, nil, err
			}
		case *tree.DTimestamp:
			et, err = tree.MakeDTimestamp(duration.Add(sType.Time, eType.Duration), time.Microsecond)
			if err != nil {
				return nil, nil, err
			}
		case *tree.DTimestampTZ:
			endTime := duration.Add(sType.Time.In(evalCtx.GetLocation()), eType.Duration)
			et, err = tree.MakeDTimestampTZ(endTime, time.Microsecond)
			if err != nil {
				return nil, nil, err
			}
		case *tree.DTime:
			startTime := timeofday.TimeOfDay(*sType)
			et = tree.MakeDTime(startTime.Add(eType.Duration))
		case *tree.DTimeTZ:
			et = tree.NewDTimeTZFromOffset(sType.Add(eType.Duration), sType.OffsetSecs)
		}
		return s, et, err
	default:
		return nil, nil, errors.AssertionFailedf("overlaps does not support the current overload")
	}
	return s, e, nil
}
