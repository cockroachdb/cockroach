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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
)

// Set sets session variables.
// Privileges: None.
//   Notes: postgres/mysql do not require privileges for session variables (some exceptions).
func (p *planner) Set(n *parser.Set) (planNode, error) {
	// By using QualifiedName.String() here any variables that are keywords will
	// be double quoted.
	name := strings.ToUpper(n.Name.String())
	switch name {
	case `DATABASE`:
		dbName, err := getStringVal(name, n.Values)
		if err != nil {
			return nil, err
		}
		if len(dbName) != 0 {
			// Verify database descriptor exists.
			if _, err := p.getDatabaseDesc(dbName); err != nil {
				return nil, err
			}
		}
		p.session.Database = dbName

	case `SYNTAX`:
		s, err := getStringVal(name, n.Values)
		if err != nil {
			return nil, err
		}
		switch strings.ToLower(string(s)) {
		case "modern":
			p.session.Syntax = int32(parser.Modern)
		case "traditional":
			p.session.Syntax = int32(parser.Traditional)
		default:
			return nil, fmt.Errorf("%s: \"%s\" is not in (\"modern\", \"traditional\")", name, s)
		}

	default:
		return nil, util.Errorf("unknown variable: %s", name)
	}
	return &valuesNode{}, nil
}

func getStringVal(name string, values parser.Exprs) (string, error) {
	if len(values) != 1 {
		return "", fmt.Errorf("%s: requires a single string value", name)
	}
	val, err := parser.EvalExpr(values[0])
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
