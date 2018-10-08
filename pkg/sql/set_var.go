// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
		return nil, pgerror.NewErrorf(pgerror.CodeSyntaxError,
			"invalid variable name: %q", n.Name)
	}

	name := strings.ToLower(n.Name)

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
				expr = unresolvedNameToStrVal(expr)

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

	v, ok := varGen[name]
	if !ok {
		return nil, pgerror.NewErrorf(pgerror.CodeUndefinedObjectError,
			"unrecognized configuration parameter %q", name)
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

// Special rule for SET: because SET doesn't apply in the context
// of a table, SET ... = IDENT really means SET ... = 'IDENT'.
func unresolvedNameToStrVal(expr tree.Expr) tree.Expr {
	if s, ok := expr.(*tree.UnresolvedName); ok {
		return tree.NewStrVal(tree.AsStringWithFlags(s, tree.FmtBareIdentifiers))
	}
	return expr
}

func (n *setVarNode) startExec(params runParams) error {
	var strVal string
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

func datumAsString(evalCtx *tree.EvalContext, name string, value tree.TypedExpr) (string, error) {
	val, err := value.Eval(evalCtx)
	if err != nil {
		return "", err
	}
	s, ok := tree.AsDString(val)
	if !ok {
		return "", pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError,
			"parameter %q requires a string value", name).SetDetailf(
			"%s is a %s", value, val.ResolvedType())
	}
	return string(s), nil
}

func getStringVal(evalCtx *tree.EvalContext, name string, values []tree.TypedExpr) (string, error) {
	if len(values) != 1 {
		return "", newSingleArgVarError(name)
	}
	return datumAsString(evalCtx, name, values[0])
}

func datumAsInt(evalCtx *tree.EvalContext, name string, value tree.TypedExpr) (int64, error) {
	val, err := value.Eval(evalCtx)
	if err != nil {
		return 0, err
	}
	iv, ok := tree.AsDInt(val)
	if !ok {
		return 0, pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError,
			"parameter %q requires an integer value", name).SetDetailf(
			"%s is a %s", value, val.ResolvedType())
	}
	return int64(iv), nil
}

func getIntVal(evalCtx *tree.EvalContext, name string, values []tree.TypedExpr) (int64, error) {
	if len(values) != 1 {
		return 0, newSingleArgVarError(name)
	}
	return datumAsInt(evalCtx, name, values[0])
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
		loc, err = timeutil.LoadLocation(location)
		if err != nil {
			var err1 error
			loc, err1 = timeutil.LoadLocation(strings.ToUpper(location))
			if err1 != nil {
				loc, err1 = timeutil.LoadLocation(strings.ToTitle(location))
				if err1 != nil {
					return "", wrapSetVarError("timezone", values[0].String(),
						"cannot find time zone %q: %v", location, err)
				}
			}
		}

	case *tree.DInterval:
		offset, _, _, err = v.Duration.Div(time.Second.Nanoseconds()).Encode()
		if err != nil {
			return "", wrapSetVarError("timezone", values[0].String(), "%v", err)
		}

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
	loc, err := timeutil.TimeZoneStringToLocation(s)
	if err != nil {
		// Maybe the string is coming from pgwire as a simple number.
		intVal, err1 := strconv.ParseInt(s, 10, 64)
		if err1 != nil {
			// Ignore the int conversion, the original error is good enough.
			return wrapSetVarError("TimeZone", s, "%v", err)
		}
		loc = timeutil.FixedOffsetTimeZoneToLocation(int(intVal)*60*60, s)
	}

	m.SetLocation(loc)
	return nil
}

func stmtTimeoutVarGetStringVal(
	ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr,
) (string, error) {
	if len(values) != 1 {
		return "", newSingleArgVarError("statement_timeout")
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
			return "", wrapSetVarError("statement_timeout", values[0].String(), "%v", err)
		}
	case *tree.DInt:
		timeout = time.Duration(*v) * time.Millisecond
	}
	return timeout.String(), nil
}

func stmtTimeoutVarSet(ctx context.Context, m *sessionDataMutator, s string) error {
	interval, err := tree.ParseDIntervalWithField(s, tree.Millisecond)
	if err != nil {
		return wrapSetVarError("statement_timeout", s, "%v", err)
	}
	timeout, err := intervalToDuration(interval)
	if err != nil {
		return wrapSetVarError("statement_timeout", s, "%v", err)
	}

	if timeout < 0 {
		return wrapSetVarError("statement_timeout", s,
			"statement_timeout cannot have a negative duration")
	}
	m.SetStmtTimeout(timeout)
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
	return pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError,
		"SET %s takes only one argument", varName)
}

func wrapSetVarError(varName, actualValue string, fmt string, args ...interface{}) error {
	return pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError,
		"invalid value for parameter %q: %q", varName, actualValue).SetDetailf(fmt, args...)
}

func newVarValueError(varName, actualVal string, allowedVals ...string) *pgerror.Error {
	err := pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError,
		"invalid value for parameter %q: %q", varName, actualVal)
	if len(allowedVals) > 0 {
		err = err.SetHintf("Available values: %s", strings.Join(allowedVals, ","))
	}
	return err
}

func newCannotChangeParameterError(varName string) error {
	return pgerror.NewErrorf(pgerror.CodeCantChangeRuntimeParamError,
		"parameter %q cannot be changed", varName)
}
