// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package paramparse provides utilities for parsing storage paramaters.
package paramparse

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/errors"
)

// UnresolvedNameToStrVal converts an unresolved name to a string value.
// Special rule for SET: because SET doesn't apply in the context
// of a table, SET ... = IDENT really means SET ... = 'IDENT'.
func UnresolvedNameToStrVal(expr tree.Expr) tree.Expr {
	if s, ok := expr.(*tree.UnresolvedName); ok {
		return tree.NewStrVal(tree.AsStringWithFlags(s, tree.FmtBareIdentifiers))
	}
	return expr
}

// DatumAsFloat transforms a tree.TypedExpr containing a Datum into a float.
func DatumAsFloat(
	ctx context.Context, evalCtx *eval.Context, name string, value tree.TypedExpr,
) (float64, error) {
	val, err := eval.Expr(ctx, evalCtx, value)
	if err != nil {
		return 0, err
	}
	switch v := eval.UnwrapDatum(ctx, evalCtx, val).(type) {
	case *tree.DString:
		return strconv.ParseFloat(string(*v), 64)
	case *tree.DInt:
		return float64(*v), nil
	case *tree.DFloat:
		return float64(*v), nil
	case *tree.DDecimal:
		return v.Decimal.Float64()
	}
	err = pgerror.Newf(pgcode.InvalidParameterValue,
		"parameter %q requires a float value", name)
	err = errors.WithDetailf(err,
		"%s is a %s", value, errors.Safe(val.ResolvedType()))
	return 0, err
}

// DatumAsDuration transforms a tree.TypedExpr containing a Datum into a
// time.Duration.
func DatumAsDuration(
	ctx context.Context, evalCtx *eval.Context, name string, value tree.TypedExpr,
) (time.Duration, error) {
	val, err := eval.Expr(ctx, evalCtx, value)
	if err != nil {
		return 0, err
	}
	var d duration.Duration
	switch v := eval.UnwrapDatum(ctx, evalCtx, val).(type) {
	case *tree.DString:
		datum, err := tree.ParseDInterval(evalCtx.SessionData().GetIntervalStyle(), string(*v))
		if err != nil {
			return 0, err
		}
		d = datum.Duration
	case *tree.DInterval:
		d = v.Duration
	default:
		err = pgerror.Newf(pgcode.InvalidParameterValue,
			"parameter %q requires a duration value", name)
		err = errors.WithDetailf(err,
			"%s is a %s", value, errors.Safe(val.ResolvedType()))
		return 0, err
	}

	secs, ok := d.AsInt64()
	if !ok {
		return 0, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"invalid duration",
		)
	}
	return time.Duration(secs) * time.Second, nil
}

// DatumAsInt transforms a tree.TypedExpr containing a Datum into an int.
func DatumAsInt(
	ctx context.Context, evalCtx *eval.Context, name string, value tree.TypedExpr,
) (int64, error) {
	val, err := eval.Expr(ctx, evalCtx, value)
	if err != nil {
		return 0, err
	}
	iv, ok := tree.AsDInt(val)
	if !ok {
		err = pgerror.Newf(pgcode.InvalidParameterValue,
			"parameter %q requires an integer value", name)
		err = errors.WithDetailf(err,
			"%s is a %s", value, errors.Safe(val.ResolvedType()))
		return 0, err
	}
	return int64(iv), nil
}

// DatumAsString transforms a tree.TypedExpr containing a Datum into a string.
func DatumAsString(
	ctx context.Context, evalCtx *eval.Context, name string, value tree.TypedExpr,
) (string, error) {
	val, err := eval.Expr(ctx, evalCtx, value)
	if err != nil {
		return "", err
	}
	s, ok := tree.AsDString(val)
	if !ok {
		err = pgerror.Newf(pgcode.InvalidParameterValue,
			"parameter %q requires a string value", name)
		err = errors.WithDetailf(err,
			"%s is a %s", value, errors.Safe(val.ResolvedType()))
		return "", err
	}
	return string(s), nil
}

// DatumAsBool transforms a tree.TypedExpr containing a Datum into a bool.
func DatumAsBool(
	ctx context.Context, evalCtx *eval.Context, name string, value tree.TypedExpr,
) (bool, error) {
	val, err := eval.Expr(ctx, evalCtx, value)
	if err != nil {
		return false, err
	}
	b, ok := tree.AsDBool(val)
	if !ok {
		err = pgerror.Newf(pgcode.InvalidParameterValue,
			"parameter %q requires a Boolean value", name)
		err = errors.WithDetailf(err,
			"%s is a %s", value, errors.Safe(val.ResolvedType()))
		return false, err
	}
	return bool(b), nil
}

// GetSingleBool returns the boolean if the input Datum is a DBool,
// and returns a detailed error message if not.
func GetSingleBool(name string, val tree.Datum) (*tree.DBool, error) {
	b, ok := val.(*tree.DBool)
	if !ok {
		err := pgerror.Newf(pgcode.InvalidParameterValue,
			"parameter %q requires a Boolean value", name)
		err = errors.WithDetailf(err,
			"%s is a %s", val, errors.Safe(val.ResolvedType()))
		return nil, err
	}
	return b, nil
}

// ParseBoolVar parses a bool, allowing other settings such as "yes"/"no"/"on"/"off".
func ParseBoolVar(varName, val string) (bool, error) {
	val = strings.ToLower(val)
	switch val {
	case "on":
		return true, nil
	case "off":
		return false, nil
	case "yes":
		return true, nil
	case "no":
		return false, nil
	}
	b, err := strconv.ParseBool(val)
	if err != nil {
		return false, pgerror.Newf(pgcode.InvalidParameterValue,
			"parameter \"%s\" requires a Boolean value", varName)
	}
	return b, nil
}
