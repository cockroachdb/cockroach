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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Set sets session variables.
// Privileges: None.
//   Notes: postgres/mysql do not require privileges for session variables (some exceptions).
func (p *planner) Set(n *parser.Set) (planNode, error) {
	if n.Name == nil {
		// A client has sent the reserved internal syntax SET ROW ...
		// Reject it.
		return nil, errors.New("invalid statement: SET ROW")
	}

	// By using VarName.String() here any variables that are keywords will
	// be double quoted.
	name := strings.ToUpper(n.Name.String())
	typedValues := make([]parser.TypedExpr, len(n.Values))
	for i, expr := range n.Values {
		typedValue, err := parser.TypeCheck(expr, nil, parser.TypeString)
		if err != nil {
			return nil, err
		}
		typedValues[i] = typedValue
	}
	switch name {
	case `DATABASE`:
		dbName, err := p.getStringVal(name, typedValues)
		if err != nil {
			return nil, err
		}
		if len(dbName) != 0 {
			// Verify database descriptor exists.
			if _, err := p.mustGetDatabaseDesc(dbName); err != nil {
				return nil, err
			}
		}
		p.session.Database = dbName
		p.evalCtx.Database = dbName

	case `SYNTAX`:
		s, err := p.getStringVal(name, typedValues)
		if err != nil {
			return nil, err
		}
		switch parser.Name(s).Normalize() {
		case parser.ReNormalizeName(parser.Modern.String()):
			p.session.Syntax = int32(parser.Modern)
		case parser.ReNormalizeName(parser.Traditional.String()):
			p.session.Syntax = int32(parser.Traditional)
		default:
			return nil, fmt.Errorf("%s: \"%s\" is not in (%q, %q)", name, s, parser.Modern, parser.Traditional)
		}

	case `DEFAULT_TRANSACTION_ISOLATION`:
		// It's unfortunate that clients want us to support both SET
		// SESSION CHARACTERISTICS AS TRANSACTION ..., which takes the
		// isolation level as keywords/identifiers (e.g. JDBC), and SET
		// DEFAULT_TRANSACTION_ISOLATION TO '...', which takes an
		// expression (e.g. psycopg2). But that's how it is.  Just ensure
		// this code keeps in sync with SetDefaultIsolation() below.
		s, err := p.getStringVal(name, typedValues)
		if err != nil {
			return nil, err
		}
		switch strings.ToUpper(s) {
		case `READ UNCOMMITTED`, `READ COMMITTED`, `SNAPSHOT`:
			p.session.DefaultIsolationLevel = enginepb.SNAPSHOT
		case `REPEATABLE READ`, `SERIALIZABLE`:
			p.session.DefaultIsolationLevel = enginepb.SERIALIZABLE
		default:
			return nil, fmt.Errorf("%s: unknown isolation level: %q", name, s)
		}

	case `DIST_SQL`:
		s, err := p.getStringVal(name, typedValues)
		if err != nil {
			return nil, err
		}
		switch parser.Name(s).Normalize() {
		case parser.ReNormalizeName("off"):
			p.session.DistSQLMode = distSQLOff
		case parser.ReNormalizeName("on"):
			p.session.DistSQLMode = distSQLOn
		case parser.ReNormalizeName("always"):
			p.session.DistSQLMode = distSQLAlways
		default:
			return nil, fmt.Errorf("%s: \"%s\" not supported", name, s)
		}
	case `TRACE`:
		s, err := p.getStringVal(name, typedValues)
		if err != nil {
			return nil, err
		}
		switch parser.Name(s).Normalize() {
		case parser.ReNormalizeName("on"):
			if p.session.TxnState.State != Open || !p.session.TxnState.implicitTxn {
				// It'd be nice in principle to be able to start a trace while in a
				// transaction, but things get hairy: would we automatically stop the
				// trace when the transaction is done? Given the current implementation,
				// we'd need to decide if we hijack the transaction's context or the
				// session's context. If we'd hijack the transaction's ctx, then we'd
				// need to finish the trace automatically, otherwise we'd lose the
				// opportunity to ever stop it. If we'd hijack the session's ctx, but
				// use a ctx derived from the txn's, then we'd potentially have
				// statements outside of the txn executing in the wrong context.
				return nil, fmt.Errorf("cannot start tracing while inside a transaction")
			}
			if err := p.session.Tracing.StartTracing(); err != nil {
				return nil, err
			}
		case parser.ReNormalizeName("off"):
			if p.session.TxnState.State != Open || !p.session.TxnState.implicitTxn {
				return nil, fmt.Errorf("cannot stop tracing while inside a transaction")
			}
			if err := p.session.Tracing.StopTracing(); err != nil {
				return nil, fmt.Errorf("Error stopping tracing: %s", err)
			}
		default:
			return nil, fmt.Errorf("%s: \"%s\" not supported", name, s)
		}

	// These settings are sent by various client drivers. We don't support
	// changing them, so we either silently ignore them or throw an error given
	// a setting that we do not respect.
	case `EXTRA_FLOAT_DIGITS`:
	// See https://www.postgresql.org/docs/9.6/static/runtime-config-client.html
	case `APPLICATION_NAME`:
	// Set by clients to improve query logging.
	// See https://www.postgresql.org/docs/9.6/static/runtime-config-logging.html#GUC-APPLICATION-NAME
	case `CLIENT_ENCODING`:
		// See https://www.postgresql.org/docs/9.6/static/multibyte.html
		s, err := p.getStringVal(name, typedValues)
		if err != nil {
			return nil, err
		}
		if strings.ToUpper(s) != "UTF8" {
			return nil, fmt.Errorf("non-UTF8 encoding %s not supported", s)
		}
	case `SEARCH_PATH`:
	// Controls the schema search order. We don't really support this as we
	// don't have first-class support for schemas.
	// TODO(jordan) can we hook this up to EvalContext.SearchPath without
	// breaking things?
	// See https://www.postgresql.org/docs/9.6/static/runtime-config-client.html
	case `STANDARD_CONFORMING_STRINGS`:
		// If true, escape backslash literals in strings. We do this by default,
		// and we do not support the opposite behavior.
		// See https://www.postgresql.org/docs/9.1/static/runtime-config-compatible.html#GUC-STANDARD-CONFORMING-STRINGS
		s, err := p.getStringVal(name, typedValues)
		if err != nil {
			return nil, err
		}
		if parser.Name(s).Normalize() != parser.ReNormalizeName("on") {
			return nil, fmt.Errorf("%s: \"%s\" not supported", name, s)
		}
	case `CLIENT_MIN_MESSAGES`:
	// Controls returned message verbosity. We don't support this.
	// See https://www.postgresql.org/docs/9.6/static/runtime-config-compatible.html

	default:
		return nil, fmt.Errorf("unknown variable: %q", name)
	}
	return &emptyNode{}, nil
}

func (p *planner) getStringVal(name string, values []parser.TypedExpr) (string, error) {
	if len(values) != 1 {
		return "", fmt.Errorf("%s: requires a single string value", name)
	}
	val, err := values[0].Eval(&p.evalCtx)
	if err != nil {
		return "", err
	}
	s, ok := parser.AsDString(val)
	if !ok {
		return "", fmt.Errorf("%s: requires a single string value: %s is a %s",
			name, values[0], val.ResolvedType())
	}
	return string(s), nil
}

func (p *planner) SetDefaultIsolation(n *parser.SetDefaultIsolation) (planNode, error) {
	// Note: We also support SET DEFAULT_TRANSACTION_ISOLATION TO ' .... ' above.
	// Ensure both versions stay in sync.
	switch n.Isolation {
	case parser.SerializableIsolation:
		p.session.DefaultIsolationLevel = enginepb.SERIALIZABLE
	case parser.SnapshotIsolation:
		p.session.DefaultIsolationLevel = enginepb.SNAPSHOT
	default:
		return nil, fmt.Errorf("unsupported default isolation level: %s", n.Isolation)
	}
	return &emptyNode{}, nil
}

func (p *planner) SetTimeZone(n *parser.SetTimeZone) (planNode, error) {
	typedValue, err := parser.TypeCheck(n.Value, nil, parser.TypeInt)
	if err != nil {
		return nil, err
	}
	d, err := typedValue.Eval(&p.evalCtx)
	if err != nil {
		return nil, err
	}

	var loc *time.Location
	var offset int64
	switch v := parser.UnwrapDatum(d).(type) {
	case *parser.DString:
		location := string(*v)
		loc, err = timeutil.LoadLocation(location)
		if err != nil {
			return nil, fmt.Errorf("cannot find time zone %q: %v", location, err)
		}

	case *parser.DInterval:
		offset, _, _, err = v.Duration.Div(time.Second.Nanoseconds()).Encode()
		if err != nil {
			return nil, err
		}

	case *parser.DInt:
		offset = int64(*v) * 60 * 60

	case *parser.DFloat:
		offset = int64(float64(*v) * 60.0 * 60.0)

	case *parser.DDecimal:
		sixty := inf.NewDec(60, 0)
		sixty.Mul(sixty, sixty).Mul(sixty, &v.Dec)
		sixty.Round(sixty, 0, inf.RoundDown)
		var ok bool
		if offset, ok = sixty.Unscaled(); !ok {
			return nil, fmt.Errorf("time zone value %s would overflow an int64", sixty)
		}

	default:
		return nil, fmt.Errorf("bad time zone value: %v", n.Value)
	}
	if loc == nil {
		loc = time.FixedZone(d.String(), int(offset))
	}
	p.session.Location = loc
	return &emptyNode{}, nil
}
