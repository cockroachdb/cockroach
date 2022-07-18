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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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
func IsFollowerReadTimestampFunction(asOf AsOfClause, searchPath sessiondata.SearchPath) bool {
	return resolveAsOfFuncType(asOf, searchPath) == asOfFuncTypeFollowerRead
}

type asOfFuncType int

const (
	asOfFuncTypeInvalid asOfFuncType = iota
	asOfFuncTypeFollowerRead
	asOfFuncTypeBoundedStaleness
)

func resolveAsOfFuncType(asOf AsOfClause, searchPath sessiondata.SearchPath) asOfFuncType {
	fe, ok := asOf.Expr.(*FuncExpr)
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

// AsOfSystemTime represents the result from the AS OF SYSTEM TIME clause.
type AsOfSystemTime struct {
	// Timestamp is the HLC timestamp evaluated from the AS OF SYSTEM TIME clause.
	Timestamp hlc.Timestamp
	// BoundedStaleness is true if the AS OF SYSTEM TIME clause specifies bounded
	// staleness should be used. If true, Timestamp specifies an (inclusive) lower
	// bound to read from - data can be read from a time later than Timestamp. If
	// false, data is returned at the exact Timestamp specified.
	BoundedStaleness bool
	// If this is a bounded staleness read, ensures we only read from the nearest
	// replica. The query will error if this constraint could not be satisfied.
	NearestOnly bool
	// If this is a bounded staleness read with nearest_only=True, this is set when
	// we failed to satisfy a bounded staleness read with a nearby replica as we
	// have no followers with an up-to-date schema.
	// This is be zero if there is no maximum bound.
	// In non-zero, we want a read t where Timestamp <= t < MaxTimestampBound.
	MaxTimestampBound hlc.Timestamp
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
	asOf AsOfClause,
	semaCtx *SemaContext,
	evalCtx *EvalContext,
	opts ...EvalAsOfTimestampOption,
) (AsOfSystemTime, error) {
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
	scalarProps.Require("AS OF SYSTEM TIME", RejectSpecial|RejectSubqueries)

	var ret AsOfSystemTime

	// In order to support the follower reads feature we permit this expression
	// to be a simple invocation of the follower_read_timestamp function.
	// Over time we could expand the set of allowed functions or expressions.
	// All non-function expressions must be const and must TypeCheck into a
	// string.
	var te TypedExpr
	if asOfFuncExpr, ok := asOf.Expr.(*FuncExpr); ok {
		switch resolveAsOfFuncType(asOf, semaCtx.SearchPath) {
		case asOfFuncTypeFollowerRead:
		case asOfFuncTypeBoundedStaleness:
			if !o.allowBoundedStaleness {
				return AsOfSystemTime{}, newInvalidExprError()
			}
			ret.BoundedStaleness = true

			// Determine the value of the "nearest_only" argument.
			if len(asOfFuncExpr.Exprs) == 2 {
				nearestOnlyExpr, err := asOfFuncExpr.Exprs[1].TypeCheck(ctx, semaCtx, types.Bool)
				if err != nil {
					return AsOfSystemTime{}, err
				}
				nearestOnlyEval, err := nearestOnlyExpr.Eval(evalCtx)
				if err != nil {
					return AsOfSystemTime{}, err
				}
				nearestOnly, ok := nearestOnlyEval.(*DBool)
				if !ok {
					return AsOfSystemTime{}, pgerror.Newf(
						pgcode.InvalidParameterValue,
						"%s: expected bool argument for nearest_only",
						asOfFuncExpr.Func.String(),
					)
				}
				ret.NearestOnly = bool(*nearestOnly)
			}
		default:
			return AsOfSystemTime{}, newInvalidExprError()
		}
		var err error
		te, err = asOf.Expr.TypeCheck(ctx, semaCtx, types.TimestampTZ)
		if err != nil {
			return AsOfSystemTime{}, err
		}
	} else {
		var err error
		te, err = asOf.Expr.TypeCheck(ctx, semaCtx, types.String)
		if err != nil {
			return AsOfSystemTime{}, err
		}
		if !IsConst(evalCtx, te) {
			return AsOfSystemTime{}, newInvalidExprError()
		}
	}

	d, err := te.Eval(evalCtx)
	if err != nil {
		return AsOfSystemTime{}, err
	}

	stmtTimestamp := evalCtx.GetStmtTimestamp()
	ret.Timestamp, err = DatumToHLC(evalCtx, stmtTimestamp, d)
	if err != nil {
		return AsOfSystemTime{}, errors.Wrap(err, "AS OF SYSTEM TIME")
	}
	return ret, nil
}

// DatumToHLC performs the conversion from a Datum to an HLC timestamp.
func DatumToHLC(evalCtx *EvalContext, stmtTimestamp time.Time, d Datum) (hlc.Timestamp, error) {
	ts := hlc.Timestamp{}
	var convErr error
	switch d := d.(type) {
	case *DString:
		s := string(*d)
		// Parse synthetic flag.
		syn := false
		if strings.HasSuffix(s, "?") {
			s = s[:len(s)-1]
			syn = true
		}
		// Attempt to parse as timestamp.
		if dt, _, err := ParseDTimestampTZ(evalCtx, s, time.Nanosecond); err == nil {
			ts.WallTime = dt.Time.UnixNano()
			ts.Synthetic = syn
			break
		}
		// Attempt to parse as a decimal.
		if dec, _, err := apd.NewFromString(s); err == nil {
			ts, convErr = DecimalToHLC(dec)
			ts.Synthetic = syn
			break
		}
		// Attempt to parse as an interval.
		if iv, err := ParseDInterval(evalCtx.GetIntervalStyle(), s); err == nil {
			if (iv.Duration == duration.Duration{}) {
				convErr = errors.Errorf("interval value %v too small, absolute value must be >= %v", d, time.Microsecond)
			}
			ts.WallTime = duration.Add(stmtTimestamp, iv.Duration).UnixNano()
			ts.Synthetic = syn
			break
		}
		convErr = errors.Errorf("value is neither timestamp, decimal, nor interval")
	case *DTimestamp:
		ts.WallTime = d.UnixNano()
	case *DTimestampTZ:
		ts.WallTime = d.UnixNano()
	case *DInt:
		ts.WallTime = int64(*d)
	case *DDecimal:
		ts, convErr = DecimalToHLC(&d.Decimal)
	case *DInterval:
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

// DecimalToHLC performs the conversion from an inputted DECIMAL datum for an
// AS OF SYSTEM TIME query to an HLC timestamp.
func DecimalToHLC(d *apd.Decimal) (hlc.Timestamp, error) {
	if d.Negative {
		return hlc.Timestamp{}, pgerror.Newf(pgcode.Syntax, "cannot be negative")
	}
	var integral, fractional apd.Decimal
	d.Modf(&integral, &fractional)
	timestamp, err := integral.Int64()
	if err != nil {
		return hlc.Timestamp{}, pgerror.Wrapf(err, pgcode.Syntax, "converting timestamp to integer") // should never happen
	}
	if fractional.IsZero() {
		// there is no logical portion to this clock
		return hlc.Timestamp{WallTime: timestamp}, nil
	}

	var logical apd.Decimal
	multiplier := apd.New(1, 10)
	condition, err := apd.BaseContext.Mul(&logical, &fractional, multiplier)
	if err != nil {
		return hlc.Timestamp{}, pgerror.Wrapf(err, pgcode.Syntax, "determining value of logical clock")
	}
	if _, err := condition.GoError(apd.DefaultTraps); err != nil {
		return hlc.Timestamp{}, pgerror.Wrapf(err, pgcode.Syntax, "determining value of logical clock")
	}

	counter, err := logical.Int64()
	if err != nil {
		return hlc.Timestamp{}, pgerror.Newf(pgcode.Syntax, "logical part has too many digits")
	}
	if counter > 1<<31 {
		return hlc.Timestamp{}, pgerror.Newf(pgcode.Syntax, "logical clock too large: %d", counter)
	}
	return hlc.Timestamp{
		WallTime: timestamp,
		Logical:  int32(counter),
	}, nil
}

// ParseHLC parses a string representation of an `hlc.Timestamp`.
// This differs from hlc.ParseTimestamp in that it parses the decimal
// serialization of an hlc timestamp as opposed to the string serialization
// performed by hlc.Timestamp.String().
//
// This function is used to parse:
//
//   1580361670629466905.0000000001
//
// hlc.ParseTimestamp() would be used to parse:
//
//   1580361670.629466905,1
//
func ParseHLC(s string) (hlc.Timestamp, error) {
	dec, _, err := apd.NewFromString(s)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	return DecimalToHLC(dec)
}
