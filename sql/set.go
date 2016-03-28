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
	"fmt"
	"strings"
	"time"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
)

// Set sets session variables.
// Privileges: None.
//   Notes: postgres/mysql do not require privileges for session variables (some exceptions).
func (p *planner) Set(n *parser.Set) (planNode, *roachpb.Error) {
	// By using QualifiedName.String() here any variables that are keywords will
	// be double quoted.
	name := strings.ToUpper(n.Name.String())
	switch name {
	case `DATABASE`:
		dbName, err := p.getStringVal(name, n.Values)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		if len(dbName) != 0 {
			// Verify database descriptor exists.
			if _, pErr := p.getDatabaseDesc(dbName); pErr != nil {
				return nil, pErr
			}
		}
		p.session.Database = dbName

	case `SYNTAX`:
		s, err := p.getStringVal(name, n.Values)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		switch NormalizeName(s) {
		case NormalizeName(parser.Modern.String()):
			p.session.Syntax = int32(parser.Modern)
		case NormalizeName(parser.Traditional.String()):
			p.session.Syntax = int32(parser.Traditional)
		default:
			return nil, roachpb.NewUErrorf("%s: \"%s\" is not in (%q, %q)", name, s, parser.Modern, parser.Traditional)
		}

	case `EXTRA_FLOAT_DIGITS`:
		// These settings are sent by the JDBC driver but we silently ignore them.

	default:
		return nil, roachpb.NewUErrorf("unknown variable: %q", name)
	}
	return &emptyNode{}, nil
}

func (p *planner) getStringVal(name string, values parser.Exprs) (string, error) {
	if len(values) != 1 {
		return "", fmt.Errorf("%s: requires a single string value", name)
	}
	val, err := values[0].Eval(p.evalCtx)
	if err != nil {
		return "", err
	}
	s, ok := val.(parser.DString)
	if !ok {
		return "", fmt.Errorf("%s: requires a single string value: %s is a %s",
			name, values[0], val.Type())
	}
	return string(s), nil
}

func (p *planner) SetDefaultIsolation(n *parser.SetDefaultIsolation) (planNode, error) {
	switch n.Isolation {
	case parser.SerializableIsolation:
		p.session.DefaultIsolationLevel = roachpb.SERIALIZABLE
	case parser.SnapshotIsolation:
		p.session.DefaultIsolationLevel = roachpb.SNAPSHOT
	default:
		return nil, fmt.Errorf("unsupported default isolation level: %s", n.Isolation)
	}
	return &emptyNode{}, nil
}

func (p *planner) SetTimeZone(n *parser.SetTimeZone) (planNode, error) {
	d, err := n.Value.Eval(p.evalCtx)
	if err != nil {
		return nil, err
	}
	var offset int64
	switch v := d.(type) {
	case parser.DString:
		location := string(v)
		if location == "DEFAULT" || location == "LOCAL" {
			location = "UTC"
		}
		if _, err := time.LoadLocation(location); err != nil {
			return nil, fmt.Errorf("cannot find time zone %q: %v", location, err)
		}
		p.session.Timezone = &SessionLocation{Location: location}

	case parser.DInterval:
		offset, _, _, err = v.Duration.Div(time.Second.Nanoseconds()).Encode()
		if err != nil {
			return nil, err
		}

	case parser.DInt:
		offset = int64(v) * 60 * 60

	case parser.DFloat:
		offset = int64(float64(v) * 60.0 * 60.0)

	case *parser.DDecimal:
		sixty := inf.NewDec(60, 0)
		sixty.Mul(sixty, sixty).Mul(sixty, &v.Dec)
		var ok bool
		if offset, ok = sixty.Unscaled(); !ok {
			return nil, fmt.Errorf("time zone value %s would overflow an int64", sixty)
		}

	default:
		return nil, fmt.Errorf("bad time zone value: %v", n.Value)
	}
	if offset != 0 {
		p.session.Timezone = &SessionOffset{Offset: offset}
	}
	p.evalCtx.GetLocation = p.session.getLocation
	return &emptyNode{}, nil
}
