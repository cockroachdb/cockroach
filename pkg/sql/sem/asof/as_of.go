// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package asof

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// FollowerReadTimestampFunctionName is the name of the function which can be
// used with AOST clauses to generate a timestamp likely to be safe for follower
// reads.
const FollowerReadTimestampFunctionName = "follower_read_timestamp"

// FollowerReadTimestampExperimentalFunctionName is the name of the old
// "experimental_" function, which we keep for backwards compatibility.
const FollowerReadTimestampExperimentalFunctionName = "experimental_follower_read_timestamp"

// WithMinTimestampFunctionName is the name of the function that can be used
// with AOST clauses to generate a bounded staleness at a fixed timestamp.
const WithMinTimestampFunctionName = "with_min_timestamp"

// WithMaxStalenessFunctionName is the name of the function that can be used
// with AOST clauses to generate a bounded staleness at a maximum interval.
const WithMaxStalenessFunctionName = "with_max_staleness"

// IsFollowerReadTimestampFunction determines whether the AS OF SYSTEM TIME
// clause contains a simple invocation of the follower_read_timestamp function.
func IsFollowerReadTimestampFunction(asOf tree.AsOfClause, searchPath sessiondata.SearchPath) bool {
	return resolveAsOfFuncType(asOf, searchPath) == asOfFuncTypeFollowerRead
}

type asOfFuncType int

const (
	asOfFuncTypeInvalid asOfFuncType = iota
	asOfFuncTypeFollowerRead
	asOfFuncTypeBoundedStaleness
)

func resolveAsOfFuncType(asOf tree.AsOfClause, searchPath sessiondata.SearchPath) asOfFuncType {
	fe, ok := asOf.Expr.(*tree.FuncExpr)
	if !ok {
		return asOfFuncTypeInvalid
	}
	def, err := fe.Func.Resolve(searchPath)
	if err != nil {
		return asOfFuncTypeInvalid
	}
	switch def.Name {
	case FollowerReadTimestampFunctionName, FollowerReadTimestampExperimentalFunctionName:
		return asOfFuncTypeFollowerRead
	case WithMinTimestampFunctionName, WithMaxStalenessFunctionName:
		return asOfFuncTypeBoundedStaleness
	}
	return asOfFuncTypeInvalid
}

type evalAsOfTimestampOptions struct {
	allowBoundedStaleness bool
}

// EvalAsOfTimestampOption is an option to pass into EvalAsOfTimestamp.
type EvalAsOfTimestampOption func(o evalAsOfTimestampOptions) evalAsOfTimestampOptions

// EvalAsOfTimestampOptionAllowBoundedStaleness signifies EvalAsOfTimestamp
// should not error if a bounded staleness query is found.
var EvalAsOfTimestampOptionAllowBoundedStaleness EvalAsOfTimestampOption = func(
	o evalAsOfTimestampOptions,
) evalAsOfTimestampOptions {
	o.allowBoundedStaleness = true
	return o
}

// EvalAsOfTimestamp evaluates the timestamp argument to an AS OF SYSTEM TIME query.
func EvalAsOfTimestamp(
	ctx context.Context,
	asOf tree.AsOfClause,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
	opts ...EvalAsOfTimestampOption,
) (tree.AsOfSystemTime, error) {
	o := evalAsOfTimestampOptions{}
	for _, f := range opts {
		o = f(o)
	}

	newInvalidExprError := func() error {
		var optFuncs string
		if o.allowBoundedStaleness {
			optFuncs = fmt.Sprintf(
				", %s, %s,",
				WithMinTimestampFunctionName,
				WithMaxStalenessFunctionName,
			)
		}
		return errors.Errorf(
			"AS OF SYSTEM TIME: only constant expressions%s or %s are allowed",
			optFuncs,
			FollowerReadTimestampFunctionName,
		)
	}

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	scalarProps := &semaCtx.Properties
	defer scalarProps.Restore(*scalarProps)
	scalarProps.Require("AS OF SYSTEM TIME", tree.RejectSpecial|tree.RejectSubqueries)

	var ret tree.AsOfSystemTime

	// In order to support the follower reads feature we permit this expression
	// to be a simple invocation of the follower_read_timestamp function.
	// Over time we could expand the set of allowed functions or expressions.
	// All non-function expressions must be const and must TypeCheck into a
	// string.
	var te tree.TypedExpr
	if asOfFuncExpr, ok := asOf.Expr.(*tree.FuncExpr); ok {
		switch resolveAsOfFuncType(asOf, semaCtx.SearchPath) {
		case asOfFuncTypeFollowerRead:
		case asOfFuncTypeBoundedStaleness:
			if !o.allowBoundedStaleness {
				return tree.AsOfSystemTime{}, newInvalidExprError()
			}
			ret.BoundedStaleness = true

			// Determine the value of the "nearest_only" argument.
			if len(asOfFuncExpr.Exprs) == 2 {
				nearestOnlyExpr, err := asOfFuncExpr.Exprs[1].TypeCheck(ctx, semaCtx, types.Bool)
				if err != nil {
					return tree.AsOfSystemTime{}, err
				}
				nearestOnlyEval, err := eval.Expr(evalCtx, nearestOnlyExpr)
				if err != nil {
					return tree.AsOfSystemTime{}, err
				}
				nearestOnly, ok := nearestOnlyEval.(*tree.DBool)
				if !ok {
					return tree.AsOfSystemTime{}, pgerror.Newf(
						pgcode.InvalidParameterValue,
						"%s: expected bool argument for nearest_only",
						asOfFuncExpr.Func.String(),
					)
				}
				ret.NearestOnly = bool(*nearestOnly)
			}
		default:
			return tree.AsOfSystemTime{}, newInvalidExprError()
		}
		var err error
		te, err = asOf.Expr.TypeCheck(ctx, semaCtx, types.TimestampTZ)
		if err != nil {
			return tree.AsOfSystemTime{}, err
		}
	} else {
		var err error
		te, err = asOf.Expr.TypeCheck(ctx, semaCtx, types.String)
		if err != nil {
			return tree.AsOfSystemTime{}, err
		}
		if !eval.IsConst(evalCtx, te) {
			return tree.AsOfSystemTime{}, newInvalidExprError()
		}
	}

	d, err := eval.Expr(evalCtx, te)
	if err != nil {
		return tree.AsOfSystemTime{}, err
	}

	stmtTimestamp := evalCtx.GetStmtTimestamp()
	ret.Timestamp, err = DatumToHLC(evalCtx, stmtTimestamp, d)
	if err != nil {
		return tree.AsOfSystemTime{}, errors.Wrap(err, "AS OF SYSTEM TIME")
	}
	return ret, nil
}

// DatumToHLC performs the conversion from a Datum to an HLC timestamp.
func DatumToHLC(
	evalCtx *eval.Context, stmtTimestamp time.Time, d tree.Datum,
) (hlc.Timestamp, error) {
	ts := hlc.Timestamp{}
	var convErr error
	switch d := d.(type) {
	case *tree.DString:
		s := string(*d)
		// Parse synthetic flag.
		syn := false
		if strings.HasSuffix(s, "?") {
			s = s[:len(s)-1]
			syn = true
		}
		// Attempt to parse as timestamp.
		if dt, _, err := tree.ParseDTimestamp(evalCtx, s, time.Nanosecond); err == nil {
			ts.WallTime = dt.Time.UnixNano()
			ts.Synthetic = syn
			break
		}
		// Attempt to parse as a decimal.
		if dec, _, err := apd.NewFromString(s); err == nil {
			ts, convErr = tree.DecimalToHLC(dec)
			ts.Synthetic = syn
			break
		}
		// Attempt to parse as an interval.
		if iv, err := tree.ParseDInterval(evalCtx.GetIntervalStyle(), s); err == nil {
			if (iv.Duration == duration.Duration{}) {
				convErr = errors.Errorf("interval value %v too small, absolute value must be >= %v", d, time.Microsecond)
			}
			ts.WallTime = duration.Add(stmtTimestamp, iv.Duration).UnixNano()
			ts.Synthetic = syn
			break
		}
		convErr = errors.Errorf("value is neither timestamp, decimal, nor interval")
	case *tree.DTimestamp:
		ts.WallTime = d.UnixNano()
	case *tree.DTimestampTZ:
		ts.WallTime = d.UnixNano()
	case *tree.DInt:
		ts.WallTime = int64(*d)
	case *tree.DDecimal:
		ts, convErr = tree.DecimalToHLC(&d.Decimal)
	case *tree.DInterval:
		ts.WallTime = duration.Add(stmtTimestamp, d.Duration).UnixNano()
	default:
		convErr = errors.WithSafeDetails(
			errors.Errorf("expected timestamp, decimal, or interval, got %s", d.ResolvedType()),
			"go type: %T", d)
	}
	if convErr != nil {
		return ts, convErr
	}
	zero := hlc.Timestamp{}
	if ts.EqOrdering(zero) {
		return ts, errors.Errorf("zero timestamp is invalid")
	} else if ts.Less(zero) {
		return ts, errors.Errorf("timestamp before 1970-01-01T00:00:00Z is invalid")
	}
	return ts, nil
}
