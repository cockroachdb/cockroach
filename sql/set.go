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

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/storage/engine/enginepb"
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

	// By using QualifiedName.String() here any variables that are keywords will
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

	case `SYNTAX`:
		s, err := p.getStringVal(name, typedValues)
		if err != nil {
			return nil, err
		}
		switch sqlbase.NormalizeName(s) {
		case sqlbase.NormalizeName(parser.Modern.String()):
			p.session.Syntax = int32(parser.Modern)
		case sqlbase.NormalizeName(parser.Traditional.String()):
			p.session.Syntax = int32(parser.Traditional)
		default:
			return nil, fmt.Errorf("%s: \"%s\" is not in (%q, %q)", name, s, parser.Modern, parser.Traditional)
		}

	case `EXTRA_FLOAT_DIGITS`:
		// These settings are sent by the JDBC driver but we silently ignore them.

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
	s, ok := val.(*parser.DString)
	if !ok {
		return "", fmt.Errorf("%s: requires a single string value: %s is a %s",
			name, values[0], val.Type())
	}
	return string(*s), nil
}

func (p *planner) SetDefaultIsolation(n *parser.SetDefaultIsolation) (planNode, error) {
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
	var offset int64
	switch v := d.(type) {
	case *parser.DString:
		location := string(*v)
		if location == "DEFAULT" || location == "LOCAL" {
			location = "UTC"
		}
		loc, err := time.LoadLocation(location)
		if err != nil {
			return nil, fmt.Errorf("cannot find time zone %q: %v", location, err)
		}
		p.session.Location = loc

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
	if offset != 0 {
		p.session.Location = time.FixedZone("", int(offset))
	}
	return &emptyNode{}, nil
}
