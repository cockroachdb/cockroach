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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/normalize"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
func IsFollowerReadTimestampFunction(
	ctx context.Context, asOf tree.AsOfClause, searchPath tree.SearchPath,
) bool {
	return resolveFuncType(ctx, asOf, searchPath) == funcTypeFollowerRead
}

type funcType int

const (
	funcTypeInvalid funcType = iota
	funcTypeFollowerRead
	funcTypeBoundedStaleness
)

func resolveFuncType(
	ctx context.Context, asOf tree.AsOfClause, searchPath tree.SearchPath,
) funcType {
	fe, ok := asOf.Expr.(*tree.FuncExpr)
	if !ok {
		return funcTypeInvalid
	}
	// We don't need a resolver here because we do not allow user-defined
	// functions to be called in AOST clauses. Only the limited set of functions
	// with names matching below are allowed. If a user defined a user-defined
	// function with the same name, we'll assume references to the function
	// within an AOST clause refer to the built-in overload.
	def, err := fe.Func.Resolve(ctx, searchPath, nil /* resolver */)
	if err != nil {
		return funcTypeInvalid
	}
	switch def.Name {
	case FollowerReadTimestampFunctionName, FollowerReadTimestampExperimentalFunctionName:
		return funcTypeFollowerRead
	case WithMinTimestampFunctionName, WithMaxStalenessFunctionName:
		return funcTypeBoundedStaleness
	}
	return funcTypeInvalid
}

type evalOptions struct {
	allowBoundedStaleness bool
}

// EvalOption is an option to pass into Eval.
type EvalOption func(o evalOptions) evalOptions

// OptionAllowBoundedStaleness signifies Eval
// should not error if a bounded staleness query is found.
var OptionAllowBoundedStaleness EvalOption = func(
	o evalOptions,
) evalOptions {
	o.allowBoundedStaleness = true
	return o
}

// Eval evaluates the timestamp argument to an AS OF SYSTEM TIME query.
func Eval(
	ctx context.Context,
	asOf tree.AsOfClause,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
	opts ...EvalOption,
) (eval.AsOfSystemTime, error) {
	o := evalOptions{}
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

	var ret eval.AsOfSystemTime

	// In order to support the follower reads feature we permit this expression
	// to be a simple invocation of the follower_read_timestamp function.
	// Over time we could expand the set of allowed functions or expressions.
	// All non-function expressions must be const and must TypeCheck into a
	// string.
	var te tree.TypedExpr
	if asOfFuncExpr, ok := asOf.Expr.(*tree.FuncExpr); ok {
		switch resolveFuncType(ctx, asOf, semaCtx.SearchPath) {
		case funcTypeFollowerRead:
		case funcTypeBoundedStaleness:
			if !o.allowBoundedStaleness {
				return eval.AsOfSystemTime{}, newInvalidExprError()
			}
			ret.BoundedStaleness = true

			// Determine the value of the "nearest_only" argument.
			if len(asOfFuncExpr.Exprs) == 2 {
				nearestOnlyExpr, err := asOfFuncExpr.Exprs[1].TypeCheck(ctx, semaCtx, types.Bool)
				if err != nil {
					return eval.AsOfSystemTime{}, err
				}
				nearestOnlyEval, err := eval.Expr(ctx, evalCtx, nearestOnlyExpr)
				if err != nil {
					return eval.AsOfSystemTime{}, err
				}
				nearestOnly, ok := nearestOnlyEval.(*tree.DBool)
				if !ok {
					return eval.AsOfSystemTime{}, pgerror.Newf(
						pgcode.InvalidParameterValue,
						"%s: expected bool argument for nearest_only",
						asOfFuncExpr.Func.String(),
					)
				}
				ret.NearestOnly = bool(*nearestOnly)
			}
		default:
			return eval.AsOfSystemTime{}, newInvalidExprError()
		}
		var err error
		te, err = asOf.Expr.TypeCheck(ctx, semaCtx, types.TimestampTZ)
		if err != nil {
			return eval.AsOfSystemTime{}, err
		}
	} else {
		var err error
		te, err = asOf.Expr.TypeCheck(ctx, semaCtx, types.String)
		if err != nil {
			return eval.AsOfSystemTime{}, err
		}
		if !eval.IsConst(evalCtx, te) {
			return eval.AsOfSystemTime{}, newInvalidExprError()
		}
	}
	var err error
	if te, err = normalize.Expr(ctx, evalCtx, te); err != nil {
		return eval.AsOfSystemTime{}, err
	}
	d, err := eval.Expr(ctx, evalCtx, te)
	if err != nil {
		return eval.AsOfSystemTime{}, err
	}

	stmtTimestamp := evalCtx.GetStmtTimestamp()
	ret.Timestamp, err = DatumToHLC(evalCtx, stmtTimestamp, d, AsOf)
	if err != nil {
		return eval.AsOfSystemTime{}, errors.Wrap(err, "AS OF SYSTEM TIME")
	}
	return ret, nil
}

// DatumToHLCUsage specifies which statement DatumToHLC() is used for.
type DatumToHLCUsage int64

const (
	// AsOf is when the DatumToHLC() is used for an AS OF SYSTEM TIME statement.
	// In this case, if the interval is not synthetic, its value has to be negative
	// and last longer than a nanosecond.
	AsOf DatumToHLCUsage = iota
	// Split is when the DatumToHLC() is used for a SPLIT statement.
	// In this case, if the interval is not synthetic, its value has to be positive
	// and last longer than a nanosecond.
	Split

	// ReplicationCutover is when the DatumToHLC() is used for an
	// ALTER TENANT ... COMPLETE REPLICATION statement.
	ReplicationCutover
)

// DatumToHLC performs the conversion from a Datum to an HLC timestamp.
func DatumToHLC(
	evalCtx *eval.Context, stmtTimestamp time.Time, d tree.Datum, usage DatumToHLCUsage,
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
		if dt, _, err := tree.ParseDTimestampTZ(evalCtx, s, time.Nanosecond); err == nil {
			ts.WallTime = dt.Time.UnixNano()
			ts.Synthetic = syn
			break
		}
		// Attempt to parse as a decimal.
		if dec, _, err := apd.NewFromString(s); err == nil {
			ts, convErr = hlc.DecimalToHLC(dec)
			ts.Synthetic = syn
			break
		}
		// Attempt to parse as an interval.
		if iv, err := tree.ParseDInterval(evalCtx.GetIntervalStyle(), s); err == nil {
			if (iv.Duration == duration.Duration{}) {
				convErr = errors.Errorf("interval value %v too small, absolute value must be >= %v", d, time.Microsecond)
			} else if (usage == AsOf && iv.Duration.Compare(duration.Duration{}) > 0 && !syn) {
				convErr = errors.Errorf("interval value %v too large, AS OF interval must be <= -%v", d, time.Microsecond)
			} else if (usage == Split && iv.Duration.Compare(duration.Duration{}) < 0) {
				// Do we need to consider if the timestamp is synthetic (see
				// hlc.Timestamp.Synthetic), as for AS OF stmt?
				convErr = errors.Errorf("interval value %v too small, SPLIT AT interval must be >= %v", d, time.Microsecond)
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
		ts, convErr = hlc.DecimalToHLC(&d.Decimal)
	case *tree.DInterval:
		if (usage == AsOf && d.Duration.Compare(duration.Duration{}) > 0) {
			convErr = errors.Errorf("interval value %v too large, AS OF interval must be <= -%v", d, time.Microsecond)
		} else if (usage == Split && d.Duration.Compare(duration.Duration{}) < 0) {
			convErr = errors.Errorf("interval value %v too small, SPLIT interval must be >= %v", d, time.Microsecond)
		}
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
