// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// setVarNode represents a SET {SESSION | LOCAL} statement.
type setVarNode struct {
	zeroInputPlanNode
	name  string
	local bool
	v     sessionVar
	// typedValues == nil means RESET.
	typedValues []tree.TypedExpr
}

// resetAllNode represents a RESET ALL statement.
type resetAllNode struct {
	zeroInputPlanNode
}

// SetVar sets session variables.
// Privileges: None.
//
//	Notes: postgres/mysql do not require privileges for session variables (some exceptions).
func (p *planner) SetVar(ctx context.Context, n *tree.SetVar) (planNode, error) {
	if n.ResetAll {
		return &resetAllNode{}, nil
	}
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
	if _, ok, _ := settings.LookupForLocalAccess(settings.SettingName(name), p.ExecCfg().Codec.ForSystemTenant()); ok {
		p.BufferClientNotice(
			ctx,
			errors.WithHint(
				pgnotice.Newf("setting custom variable %q", name),
				"did you mean SET CLUSTER SETTING?",
			),
		)
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
					ctx, expr, dummyHelper, types.String, false, "SET SESSION "+name)
				if err != nil {
					return nil, wrapSetVarError(err, name, expr.String())
				}
				typedValues[i] = typedValue
			}
		}
	}

	if v.Set == nil && v.RuntimeSet == nil && v.SetWithPlanner == nil {
		return nil, newCannotChangeParameterError(name)
	}

	if typedValues == nil {
		// Statement is RESET. Do we have a default available?
		// We do not use getDefaultString here because we need to delay
		// the computation of the default to the execute phase.
		if _, ok := p.sessionDataMutatorIterator.defaults[name]; !ok && v.GlobalDefault == nil {
			return nil, newCannotChangeParameterError(name)
		}
	}

	return &setVarNode{name: name, local: n.Local, v: v, typedValues: typedValues}, nil
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
			d, err := eval.Expr(params.ctx, params.EvalContext(), v)
			if err != nil {
				return err
			}
			n.typedValues[i] = d
		}
		var err error
		if n.v.GetStringVal != nil {
			strVal, err = n.v.GetStringVal(params.ctx, params.extendedEvalCtx, n.typedValues, params.p.Txn())
		} else {
			// No string converter defined, use the default one.
			strVal, err = getStringVal(params.ctx, params.EvalContext(), n.name, n.typedValues)
		}
		if err != nil {
			return err
		}
	} else {
		// Statement is RESET and we already know we have a default. Find it.
		_, strVal = getSessionVarDefaultString(
			n.name,
			n.v,
			params.p.sessionDataMutatorIterator.sessionDataMutatorBase,
		)
	}

	return params.p.SetSessionVar(params.ctx, n.name, strVal, n.local)
}

// applyOnSessionDataMutators applies the given function on the relevant
// sessionDataMutators.
func (p *planner) applyOnSessionDataMutators(
	ctx context.Context, local bool, applyFunc func(m sessionDataMutator) error,
) error {
	if local {
		// We don't allocate a new SessionData object on implicit transactions.
		// This no-ops in postgres with a warning, so copy accordingly.
		if p.extendedEvalCtx.TxnImplicit {
			p.BufferClientNotice(
				ctx,
				pgnotice.NewWithSeverityf(
					"WARNING",
					"SET LOCAL can only be used in transaction blocks",
				),
			)
			return nil
		}
		return p.sessionDataMutatorIterator.applyOnTopMutator(applyFunc)
	}
	return p.sessionDataMutatorIterator.applyOnEachMutatorError(applyFunc)
}

// getSessionVarDefaultString retrieves a string suitable to pass to a
// session var's Set() method. First return value is false if there is
// no default.
func getSessionVarDefaultString(
	varName string, v sessionVar, m sessionDataMutatorBase,
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

func (p *planner) resetAllSessionVars(ctx context.Context) error {
	for varName, v := range varGen {
		if v.Set == nil && v.RuntimeSet == nil && v.SetWithPlanner == nil {
			continue
		}
		// For Postgres compatibility, Don't reset `role` here.
		if varName == "role" {
			continue
		}
		hasDefault, defVal := getSessionVarDefaultString(
			varName,
			v,
			p.sessionDataMutatorIterator.sessionDataMutatorBase,
		)
		if !hasDefault {
			continue
		}
		if err := p.SetSessionVar(ctx, varName, defVal, false /* isLocal */); err != nil {
			return err
		}
	}
	for varName := range p.SessionData().CustomOptions {
		_, v, err := getSessionVar(varName, false /* missingOK */)
		if err != nil {
			return err
		}
		_, defVal := getSessionVarDefaultString(
			varName,
			v,
			p.sessionDataMutatorIterator.sessionDataMutatorBase,
		)
		if err := p.SetSessionVar(ctx, varName, defVal, false /* isLocal */); err != nil {
			return err
		}
	}
	return nil
}

func (n *resetAllNode) startExec(params runParams) error {
	return params.p.resetAllSessionVars(params.ctx)
}

func (n *resetAllNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *resetAllNode) Values() tree.Datums            { return nil }
func (n *resetAllNode) Close(_ context.Context)        {}

func getStringVal(
	ctx context.Context, evalCtx *eval.Context, name string, values []tree.TypedExpr,
) (string, error) {
	if len(values) != 1 {
		return "", newSingleArgVarError(name)
	}
	return paramparse.DatumAsString(ctx, evalCtx, name, values[0])
}

func getIntVal(
	ctx context.Context, evalCtx *eval.Context, name string, values []tree.TypedExpr,
) (int64, error) {
	if len(values) != 1 {
		return 0, newSingleArgVarError(name)
	}
	return paramparse.DatumAsInt(ctx, evalCtx, name, values[0])
}

func getFloatVal(
	ctx context.Context, evalCtx *eval.Context, name string, values []tree.TypedExpr,
) (float64, error) {
	if len(values) != 1 {
		return 0, newSingleArgVarError(name)
	}
	return paramparse.DatumAsFloat(ctx, evalCtx, name, values[0])
}

func timeZoneVarGetStringVal(
	ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr, _ *kv.Txn,
) (string, error) {
	if len(values) != 1 {
		return "", newSingleArgVarError("timezone")
	}
	d, err := eval.Expr(ctx, &evalCtx.Context, values[0])
	if err != nil {
		return "", err
	}

	var loc *time.Location
	var offset int64
	switch v := eval.UnwrapDatum(ctx, &evalCtx.Context, d).(type) {
	case *tree.DString:
		location := string(*v)
		loc, err = timeutil.TimeZoneStringToLocation(
			location,
			timeutil.TimeZoneStringToLocationISO8601Standard,
		)
		if err != nil {
			return "", wrapSetVarError(errors.Wrapf(err, "cannot find time zone %q", location), "timezone", values[0].String())
		}

	case *tree.DInterval:
		offset, _, _, err = v.Duration.Encode()
		if err != nil {
			return "", wrapSetVarError(err, "timezone", values[0].String())
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
			return "", wrapSetVarError(errors.Newf("time zone value %s would overflow an int64", sixty), "timezone", values[0].String())
		}

	default:
		return "", newVarValueError("timezone", values[0].String())
	}
	if loc == nil {
		loc = timeutil.TimeZoneOffsetToLocation(int(offset))
	}

	return loc.String(), nil
}

func timeZoneVarSet(_ context.Context, m sessionDataMutator, s string) error {
	loc, err := timeutil.TimeZoneStringToLocation(
		s,
		timeutil.TimeZoneStringToLocationISO8601Standard,
	)
	if err != nil {
		return wrapSetVarError(err, "TimeZone", s)
	}

	m.SetLocation(loc)
	return nil
}

func makeTimeoutVarGetter(
	varName string,
) func(
	ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr, txn *kv.Txn) (string, error) {
	return func(
		ctx context.Context, evalCtx *extendedEvalContext, values []tree.TypedExpr, txn *kv.Txn,
	) (string, error) {
		if len(values) != 1 {
			return "", newSingleArgVarError(varName)
		}
		d, err := eval.Expr(ctx, &evalCtx.Context, values[0])
		if err != nil {
			return "", err
		}

		var timeout time.Duration
		switch v := eval.UnwrapDatum(ctx, &evalCtx.Context, d).(type) {
		case *tree.DString:
			return string(*v), nil
		case *tree.DInterval:
			timeout, err = durationToTotalNanos(v.Duration)
			if err != nil {
				return "", wrapSetVarError(err, varName, values[0].String())
			}
		case *tree.DInt:
			timeout = time.Duration(*v) * time.Millisecond
		}
		return timeout.String(), nil
	}
}

func validateTimeoutVar(
	style duration.IntervalStyle, timeString string, varName string,
) (time.Duration, error) {
	interval, err := tree.ParseIntervalWithTypeMetadata(
		style,
		timeString,
		types.IntervalTypeMetadata{
			DurationField: types.IntervalDurationField{
				DurationType: types.IntervalDurationType_MILLISECOND,
			},
		},
	)
	if err != nil {
		return 0, wrapSetVarError(err, varName, timeString)
	}
	timeout, err := durationToTotalNanos(interval)
	if err != nil {
		return 0, wrapSetVarError(err, varName, timeString)
	}

	if timeout < 0 {
		return 0, wrapSetVarError(errors.Newf("%v cannot have a negative duration", redact.SafeString(varName)), varName, timeString)
	}

	return timeout, nil
}

func stmtTimeoutVarSet(ctx context.Context, m sessionDataMutator, s string) error {
	timeout, err := validateTimeoutVar(
		m.data.GetIntervalStyle(),
		s,
		"statement_timeout",
	)
	if err != nil {
		return err
	}

	m.SetStmtTimeout(timeout)
	return nil
}

func lockTimeoutVarSet(ctx context.Context, m sessionDataMutator, s string) error {
	timeout, err := validateTimeoutVar(
		m.data.GetIntervalStyle(),
		s,
		"lock_timeout",
	)
	if err != nil {
		return err
	}

	m.SetLockTimeout(timeout)
	return nil
}

func deadlockTimeoutVarSet(ctx context.Context, m sessionDataMutator, s string) error {
	timeout, err := validateTimeoutVar(
		m.data.GetIntervalStyle(),
		s,
		"deadlock_timeout",
	)
	if err != nil {
		return err
	}

	m.SetDeadlockTimeout(timeout)
	return nil
}

func idleInSessionTimeoutVarSet(ctx context.Context, m sessionDataMutator, s string) error {
	timeout, err := validateTimeoutVar(
		m.data.GetIntervalStyle(),
		s,
		"idle_in_session_timeout",
	)
	if err != nil {
		return err
	}

	m.SetIdleInSessionTimeout(timeout)
	return nil
}

func transactionTimeoutVarSet(ctx context.Context, m sessionDataMutator, s string) error {
	timeout, err := validateTimeoutVar(
		m.data.GetIntervalStyle(),
		s,
		"transaction_timeout",
	)
	if err != nil {
		return err
	}

	m.SetTransactionTimeout(timeout)
	return nil
}

func idleInTransactionSessionTimeoutVarSet(
	ctx context.Context, m sessionDataMutator, s string,
) error {
	timeout, err := validateTimeoutVar(
		m.data.GetIntervalStyle(),
		s,
		"idle_in_transaction_session_timeout",
	)
	if err != nil {
		return err
	}

	m.SetIdleInTransactionSessionTimeout(timeout)
	return nil
}

func durationToTotalNanos(duration duration.Duration) (time.Duration, error) {
	nanos, _, _, err := duration.Encode()
	if err != nil {
		return 0, err
	}
	return time.Duration(nanos), nil
}

func newSingleArgVarError(varName string) error {
	return pgerror.Newf(pgcode.InvalidParameterValue,
		"SET %s takes only one argument", varName)
}

func wrapSetVarError(cause error, varName, actualValue string) error {
	return pgerror.Wrapf(
		cause,
		pgcode.InvalidParameterValue,
		"invalid value for parameter %q: %q",
		redact.SafeString(varName),
		actualValue,
	)
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
