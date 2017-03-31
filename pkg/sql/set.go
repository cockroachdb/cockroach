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

	"golang.org/x/net/context"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Set sets session variables.
// Privileges: None.
//   Notes: postgres/mysql do not require privileges for session variables (some exceptions).
func (p *planner) Set(ctx context.Context, n *parser.Set) (planNode, error) {
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
			if _, err := MustGetDatabaseDesc(ctx, p.txn, p.getVirtualTabler(), dbName); err != nil {
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
			p.session.Syntax = parser.Modern
		case parser.ReNormalizeName(parser.Traditional.String()):
			p.session.Syntax = parser.Traditional
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

	case `DISTSQL`:
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

	// These settings are sent by various client drivers. We don't support
	// changing them, so we either silently ignore them or throw an error given
	// a setting that we do not respect.
	case `EXTRA_FLOAT_DIGITS`:
	// See https://www.postgresql.org/docs/9.6/static/runtime-config-client.html
	case `APPLICATION_NAME`:
		// Set by clients to improve query logging.
		// See https://www.postgresql.org/docs/9.6/static/runtime-config-logging.html#GUC-APPLICATION-NAME
		s, err := p.getStringVal(name, typedValues)
		if err != nil {
			return nil, err
		}
		p.session.resetApplicationName(s)

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
		// https://www.postgresql.org/docs/9.6/static/runtime-config-client.html
		newSearchPath := make(parser.SearchPath, len(typedValues))
		foundPgCatalog := false
		for i, v := range typedValues {
			val, err := v.Eval(&p.evalCtx)
			if err != nil {
				return nil, err
			}
			s, ok := parser.AsDString(val)
			if !ok {
				return nil, fmt.Errorf("%s: requires string values: %s is %s not string",
					name, v, val.ResolvedType())
			}
			if s == pgCatalogName {
				foundPgCatalog = true
			}
			newSearchPath[i] = parser.Name(s).Normalize()
		}
		if !foundPgCatalog {
			// "The system catalog schema, pg_catalog, is always searched,
			// whether it is mentioned in the path or not. If it is
			// mentioned in the path then it will be searched in the
			// specified order. If pg_catalog is not in the path then it
			// will be searched before searching any of the path items."
			newSearchPath = append([]string{"pg_catalog"}, newSearchPath...)
		}
		p.session.SearchPath = newSearchPath

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
		sixty := apd.New(60, 0)
		ed := apd.MakeErrDecimal(parser.ExactCtx)
		ed.Mul(sixty, sixty, sixty)
		ed.Mul(sixty, sixty, &v.Decimal)
		offset = ed.Int64(sixty)
		if ed.Err() != nil {
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
