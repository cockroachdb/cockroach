// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// setVarNode represents a SET SESSION statement.
type setVarNode struct {
	name string
	v    sessionVar
	// typedValues == nil means RESET.
	typedValues []tree.TypedExpr
}

// SetVar sets session variables.
// Privileges: None.
//   Notes: postgres/mysql do not require privileges for session variables (some exceptions).
func (p *planner) SetVar(ctx context.Context, n *tree.SetVar) (planNode, error) {
	if n.Name == "" {
		// A client has sent the reserved internal syntax SET ROW ...,
		// or the user entered `SET "" = foo`. Reject it.
		return nil, pgerror.Newf(pgcode.Syntax,
			"invalid variable name: %q", n.Name)
	}

	name := strings.ToLower(n.Name)
	_, v, err := getSessionVar(name, false /* missingOk */)
	if err != nil {
		return nil, err
	}

	var typedValues []tree.TypedExpr
	if len(n.Values) > 0 {
		isReset := false
		if len(n.Values) == 1 {
			if _, ok := n.Values[0].(tree.DefaultVal); ok {
				// "SET var = DEFAULT" means RESET.
				// In that case, we want typedValues to remain nil, so that
				// the Start() logic recognizes the RESET too.
				isReset = true
			}
		}

		if !isReset {
			typedValues = make([]tree.TypedExpr, len(n.Values))
			for i, expr := range n.Values {
				expr = paramparse.UnresolvedNameToStrVal(expr)

				var dummyHelper tree.IndexedVarHelper
				typedValue, err := p.analyzeExpr(
					ctx, expr, nil, dummyHelper, types.String, false, "SET SESSION "+name)
				if err != nil {
					return nil, wrapSetVarError(name, expr.String(), "%v", err)
				}
				typedValues[i] = typedValue
			}
		}
	}

	if v.Set == nil && v.RuntimeSet == nil {
		return nil, newCannotChangeParameterError(name)
	}

	if typedValues == nil {
		// Statement is RESET. Do we have a default available?
		// We do not use getDefaultString here because we need to delay
		// the computation of the default to the execute phase.
		if _, ok := p.sessionDataMutator.defaults[name]; !ok && v.GlobalDefault == nil {
			return nil, newCannotChangeParameterError(name)
		}
	}

	return &setVarNode{name: name, v: v, typedValues: typedValues}, nil
}

func (n *setVarNode) startExec(params runParams) error {
	var strVal string

	if _, ok := DummyVars[n.name]; ok {
		telemetry.Inc(sqltelemetry.DummySessionVarValueCounter(n.name))
		params.p.BufferClientNotice(
			params.ctx,
			pgnotice.NewWithSeverityf("WARNING", "setting session var %q is a no-op", n.name),
		)
	}
	if n.typedValues != nil {
		for i, v := range n.typedValues {
			d, err := v.Eval(params.EvalContext())
			if err != nil {
				return err
			}
			n.typedValues[i] = d
		}
		var err error
		if n.v.GetStringVal != nil {
			strVal, err = n.v.GetStringVal(params.ctx, params.extendedEvalCtx, n.typedValues)
		} else {
			// No string converter defined, use the default one.
			strVal, err = getStringVal(params.EvalContext(), n.name, n.typedValues)
		}
		if err != nil {
			return err
		}
	} else {
		// Statement is RESET and we already know we have a default. Find it.
		_, strVal = getSessionVarDefaultString(n.name, n.v, params.p.sessionDataMutator)
	}

	if n.v.RuntimeSet != nil {
		return n.v.RuntimeSet(params.ctx, params.extendedEvalCtx, strVal)
	}
	if n.v.SetWithPlanner != nil {
		return n.v.SetWithPlanner(params.ctx, params.p, strVal)
	}
	return n.v.Set(params.ctx, params.p.sessionDataMutator, strVal)
}

// getSessionVarDefaultString retrieves a string suitable to pass to a
// session var's Set() method. First return value is false if there is
// no default.
func getSessionVarDefaultString(
	varName string, v sessionVar, m *sessionDataMutator,
) (bool, string) {
	if defVal, ok := m.defaults[varName]; ok {
		return true, defVal
	}
	if v.GlobalDefault != nil {
		return true, v.GlobalDefault(&m.settings.SV)
	}
	return false, ""
}

func (n *setVarNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *setVarNode) Values() tree.Datums            { return nil }
func (n *setVarNode) Close(_ context.Context)        {}

func getStringVal(evalCtx *tree.EvalContext, name string, values []tree.TypedExpr) (string, error) {
	if len(values) != 1 {
		return "", newSingleArgVarError(name)
	}
	return paramparse.DatumAsString(evalCtx, name, values[0])
}

func getIntVal(evalCtx *tree.EvalContext, name string, values []tree.TypedExpr) (int64, error) {
	if len(values) != 1 {
		return 0, newSingleArgVarError(name)
	}
	return paramparse.DatumAsInt(evalCtx, name, values[0])
}

func timeZoneVarGetStringVal(
	_ context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr,
) (string, error) {
	if len(values) != 1 {
		return "", newSingleArgVarError("timezone")
	}
	d, err := values[0].Eval(&evalCtx.EvalContext)
	if err != nil {
		return "", err
	}

	var loc *time.Location
	var offset int64
	switch v := tree.UnwrapDatum(&evalCtx.EvalContext, d).(type) {
	case *tree.DString:
		location := string(*v)
		loc, err = timeutil.TimeZoneStringToLocation(
			location,
			timeutil.TimeZoneStringToLocationISO8601Standard,
		)
		if err != nil {
			return "", wrapSetVarError("timezone", values[0].String(),
				"cannot find time zone %q: %v", location, err)
		}

	case *tree.DInterval:
		offset, _, _, err = v.Duration.Encode()
		if err != nil {
			return "", wrapSetVarError("timezone", values[0].String(), "%v", err)
		}
		offset /= int64(time.Second)

	case *tree.DInt:
		offset = int64(*v) * 60 * 60

	case *tree.DFloat:
		offset = int64(float64(*v) * 60.0 * 60.0)

	case *tree.DDecimal:
		sixty := apd.New(60, 0)
		ed := apd.MakeErrDecimal(tree.ExactCtx)
		ed.Mul(sixty, sixty, sixty)
		ed.Mul(sixty, sixty, &v.Decimal)
		offset = ed.Int64(sixty)
		if ed.Err() != nil {
			return "", wrapSetVarError("timezone", values[0].String(),
				"time zone value %s would overflow an int64", sixty)
		}

	default:
		return "", newVarValueError("timezone", values[0].String())
	}
	if loc == nil {
		loc = timeutil.FixedOffsetTimeZoneToLocation(int(offset), d.String())
	}

	return loc.String(), nil
}

func timeZoneVarSet(_ context.Context, m *sessionDataMutator, s string) error {
	loc, err := timeutil.TimeZoneStringToLocation(
		s,
		timeutil.TimeZoneStringToLocationISO8601Standard,
	)
	if err != nil {
		return wrapSetVarError("TimeZone", s, "%v", err)
	}

	m.SetLocation(loc)
	return nil
}

func makeTimeoutVarGetter(
	varName string,
) func(
	ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr) (string, error) {
	return func(
		ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr,
	) (string, error) {
		if len(values) != 1 {
			return "", newSingleArgVarError(varName)
		}
		d, err := values[0].Eval(&evalCtx.EvalContext)
		if err != nil {
			return "", err
		}

		var timeout time.Duration
		switch v := tree.UnwrapDatum(&evalCtx.EvalContext, d).(type) {
		case *tree.DString:
			return string(*v), nil
		case *tree.DInterval:
			timeout, err = intervalToDuration(v)
			if err != nil {
				return "", wrapSetVarError(varName, values[0].String(), "%v", err)
			}
		case *tree.DInt:
			timeout = time.Duration(*v) * time.Millisecond
		}
		return timeout.String(), nil
	}
}

func validateTimeoutVar(timeString string, varName string) (time.Duration, error) {
	interval, err := tree.ParseDIntervalWithTypeMetadata(timeString, types.IntervalTypeMetadata{
		DurationField: types.IntervalDurationField{
			DurationType: types.IntervalDurationType_MILLISECOND,
		},
	})
	if err != nil {
		return 0, wrapSetVarError(varName, timeString, "%v", err)
	}
	timeout, err := intervalToDuration(interval)
	if err != nil {
		return 0, wrapSetVarError(varName, timeString, "%v", err)
	}

	if timeout < 0 {
		return 0, wrapSetVarError(varName, timeString,
			"%v cannot have a negative duration", varName)
	}

	return timeout, nil
}

func stmtTimeoutVarSet(ctx context.Context, m *sessionDataMutator, s string) error {
	timeout, err := validateTimeoutVar(s, "statement_timeout")
	if err != nil {
		return err
	}

	m.SetStmtTimeout(timeout)
	return nil
}

func idleInSessionTimeoutVarSet(ctx context.Context, m *sessionDataMutator, s string) error {
	timeout, err := validateTimeoutVar(s, "idle_in_session_timeout")
	if err != nil {
		return err
	}

	m.SetIdleInSessionTimeout(timeout)
	return nil
}

func idleInTransactionSessionTimeoutVarSet(
	ctx context.Context, m *sessionDataMutator, s string,
) error {
	timeout, err := validateTimeoutVar(s,
		"idle_in_transaction_session_timeout")
	if err != nil {
		return err
	}

	m.SetIdleInTransactionSessionTimeout(timeout)
	return nil
}

func intervalToDuration(interval *tree.DInterval) (time.Duration, error) {
	nanos, _, _, err := interval.Encode()
	if err != nil {
		return 0, err
	}
	return time.Duration(nanos), nil
}

func newSingleArgVarError(varName string) error {
	return pgerror.Newf(pgcode.InvalidParameterValue,
		"SET %s takes only one argument", varName)
}

func wrapSetVarError(varName, actualValue string, fmt string, args ...interface{}) error {
	err := pgerror.Newf(pgcode.InvalidParameterValue,
		"invalid value for parameter %q: %q", varName, actualValue)
	return errors.WithDetailf(err, fmt, args...)
}

func newVarValueError(varName, actualVal string, allowedVals ...string) (err error) {
	err = pgerror.Newf(pgcode.InvalidParameterValue,
		"invalid value for parameter %q: %q", varName, actualVal)
	if len(allowedVals) > 0 {
		err = errors.WithHintf(err, "Available values: %s", strings.Join(allowedVals, ","))
	}
	return err
}

func newCannotChangeParameterError(varName string) error {
	return pgerror.Newf(pgcode.CantChangeRuntimeParam,
		"parameter %q cannot be changed", varName)
}
