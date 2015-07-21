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

	"github.com/cockroachdb/cockroach/sql/parser"
)

// Values constructs a valuesNode from a VALUES expression.
func (p *planner) Values(n parser.Values) (planNode, error) {
	v := &valuesNode{
		rows: make([]parser.DTuple, 0, len(n)),
	}
	for _, tuple := range n {
		data, err := parser.EvalExpr(tuple, nil)
		if err != nil {
			return nil, err
		}
		vals, ok := data.(parser.DTuple)
		if !ok {
			return nil, fmt.Errorf("expected a tuple, but found %T", data)
		}
		v.rows = append(v.rows, vals)
	}
	return v, nil
}

type valuesNode struct {
	columns []string
	rows    []parser.DTuple
	nextRow int // The index of the next row.
}

func (n *valuesNode) Columns() []string {
	return n.columns
}

func (n *valuesNode) Values() parser.DTuple {
	return n.rows[n.nextRow-1]
}

func (n *valuesNode) Next() bool {
	if n.nextRow >= len(n.rows) {
		return false
	}
	n.nextRow++
	return true
}

func (*valuesNode) Err() error {
	return nil
}
