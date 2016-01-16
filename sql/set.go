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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/shopspring/decimal"
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
		dbName, pErr := p.getStringVal(name, n.Values)
		if pErr != nil {
			return nil, pErr
		}
		if len(dbName) != 0 {
			// Verify database descriptor exists.
			if _, pErr := p.getDatabaseDesc(dbName); pErr != nil {
				return nil, pErr
			}
		}
		p.session.Database = dbName

	case `SYNTAX`:
		s, pErr := p.getStringVal(name, n.Values)
		if pErr != nil {
			return nil, pErr
		}
		switch normalizeName(string(s)) {
		case normalizeName(parser.Modern.String()):
			p.session.Syntax = int32(parser.Modern)
		case normalizeName(parser.Traditional.String()):
			p.session.Syntax = int32(parser.Traditional)
		default:
			return nil, roachpb.NewUErrorf("%s: \"%s\" is not in (%q, %q)", name, s, parser.Modern, parser.Traditional)
		}

	default:
		return nil, roachpb.NewUErrorf("unknown variable: %q", name)
	}
	return &valuesNode{}, nil
}

func (p *planner) getStringVal(name string, values parser.Exprs) (string, *roachpb.Error) {
	if len(values) != 1 {
		return "", roachpb.NewUErrorf("%s: requires a single string value", name)
	}
	val, err := values[0].Eval(p.evalCtx)
	if err != nil {
		return "", roachpb.NewError(err)
	}
	s, ok := val.(parser.DString)
	if !ok {
		return "", roachpb.NewUErrorf("%s: requires a single string value: %s is a %s",
			name, values[0], val.Type())
	}
	return string(s), nil
}

func (p *planner) SetTimeZone(n *parser.SetTimeZone) (planNode, *roachpb.Error) {
	d, err := n.Value.Eval(p.evalCtx)
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	var offset int64
	switch v := d.(type) {
	case parser.DString:
		location := string(v)
		if location == "DEFAULT" || location == "LOCAL" {
			location = "UTC"
		}
		if _, err := time.LoadLocation(location); err != nil {
			return nil, roachpb.NewError(err)
		}
		p.session.Timezone = &Session_Location{Location: location}

	case parser.DInterval:
		offset = int64(v.Duration / time.Second)

	case parser.DInt:
		offset = int64(v) * 60 * 60

	case parser.DFloat:
		offset = int64(float64(v) * 60.0 * 60.0)

	case parser.DDecimal:
		sixty := decimal.New(60, 0)
		offset = v.Mul(sixty).Mul(sixty).IntPart()

	default:
		return nil, roachpb.NewUErrorf("bad time zone value: %v", n.Value)
	}
	if offset != 0 {
		p.session.Timezone = &Session_Offset{Offset: offset}
	}
	p.evalCtx.GetLocation = p.session.getLocation
	return &valuesNode{}, nil
}
