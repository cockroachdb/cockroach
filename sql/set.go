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
func (p *planner) Set(n *parser.Set) (planNode, error) {
	// By using QualifiedName.String() here any variables that are keywords will
	// be double quoted.
	name := strings.ToLower(n.Name.String())
	switch name {
	case `"database"`: // Quoted: database is a reserved word
		if len(n.Values) != 1 {
			return nil, fmt.Errorf("database: requires a single string value")
		}
		val, err := parser.EvalExpr(n.Values[0], nil)
		if err != nil {
			return nil, err
		}
		p.session.Database = val.String()
	default:
		return nil, util.Errorf("unknown variable: %s", name)
	}
	return &valuesNode{}, nil
}
