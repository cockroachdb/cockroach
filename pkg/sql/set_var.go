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
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// setVarNode represents a SET SESSION statement.
type setVarNode struct {
	v sessionVar
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
		return nil, pgerror.NewErrorf(pgerror.CodeInvalidNameError, "invalid variable name: %q", n.Name)
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
				// Special rule for SET: because SET doesn't apply in the context
				// of a table, SET ... = IDENT really means SET ... = 'IDENT'.
				if s, ok := expr.(*tree.UnresolvedName); ok {
					expr = tree.NewStrVal(tree.AsStringWithFlags(s, tree.FmtBareIdentifiers))
				}

				var dummyHelper tree.IndexedVarHelper
				typedValue, err := p.analyzeExpr(
					ctx, expr, nil, dummyHelper, types.String, false, "SET SESSION "+name)
				if err != nil {
					return nil, err
				}
				typedValues[i] = typedValue
			}
		}
	}

	v, ok := varGen[name]
	if !ok {
		return nil, fmt.Errorf("unknown variable: %q", name)
	}

	if typedValues != nil {
		if v.Set == nil {
			return nil, fmt.Errorf("variable \"%s\" cannot be changed", name)
		}
	} else {
		if v.Reset == nil {
			return nil, fmt.Errorf("variable \"%s\" cannot be reset", name)
		}
	}

	return &setVarNode{v: v, typedValues: typedValues}, nil
}

func (n *setVarNode) startExec(params runParams) error {
	if n.typedValues != nil {
		for i, v := range n.typedValues {
			d, err := v.Eval(params.EvalContext())
			if err != nil {
				return err
			}
			n.typedValues[i] = d
		}
		return n.v.Set(
			params.ctx, params.p.sessionDataMutator,
			params.extendedEvalCtx, n.typedValues)
	}
	return n.v.Reset(params.p.sessionDataMutator)
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
		return "", fmt.Errorf("set %s: requires a string value: %s is a %s",
			name, value, val.ResolvedType())
	}
	return string(s), nil
}

func getStringVal(evalCtx *tree.EvalContext, name string, values []tree.TypedExpr) (string, error) {
	if len(values) != 1 {
		return "", fmt.Errorf("set %s: requires a single string value", name)
	}
	return datumAsString(evalCtx, name, values[0])
}

func setTimeZone(
	_ context.Context, m *sessionDataMutator, evalCtx *extendedEvalContext, values []tree.TypedExpr,
) error {
	if len(values) != 1 {
		return errors.New("set time zone requires a single argument")
	}
	d, err := values[0].Eval(&evalCtx.EvalContext)
	if err != nil {
		return err
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
					return fmt.Errorf("cannot find time zone %q: %v", location, err)
				}
			}
		}

	case *tree.DInterval:
		offset, _, _, err = v.Duration.Div(time.Second.Nanoseconds()).Encode()
		if err != nil {
			return err
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
			return fmt.Errorf("time zone value %s would overflow an int64", sixty)
		}

	default:
		return fmt.Errorf("bad time zone value: %s", d.String())
	}
	if loc == nil {
		loc = timeutil.FixedOffsetTimeZoneToLocation(int(offset), d.String())
	}
	m.SetLocation(loc)
	return nil
}

func setStmtTimeout(
	_ context.Context, m *sessionDataMutator, evalCtx *extendedEvalContext, values []tree.TypedExpr,
) error {
	if len(values) != 1 {
		return errors.New("set statement_timeout requires a single argument")
	}
	d, err := values[0].Eval(&evalCtx.EvalContext)
	if err != nil {
		return err
	}

	var timeout time.Duration
	switch v := tree.UnwrapDatum(&evalCtx.EvalContext, d).(type) {
	case *tree.DString:
		interval, err := tree.ParseDInterval(string(*v))
		if err != nil {
			return err
		}
		timeout, err = intervalToDuration(interval)
		if err != nil {
			return err
		}
	case *tree.DInterval:
		timeout, err = intervalToDuration(v)
		if err != nil {
			return err
		}
	case *tree.DInt:
		timeout = time.Duration(*v) * time.Millisecond
	}

	if timeout < 0 {
		return errors.New("statement_timeout cannot have a negative duration")
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
