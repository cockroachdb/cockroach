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
	"errors"
	"fmt"

	"github.com/cockroachdb/cockroach/sql/parser"
)

// Values constructs a valuesNode from a VALUES expression.
func (p *planner) Values(n parser.Values) (planNode, error) {
	v := &valuesNode{
		rows: make([]parser.DTuple, 0, len(n)),
	}

	nCols := 0
	for _, tuple := range n {
		for i := range tuple {
			var err error
			tuple[i], err = p.parser.TypeCheckAndNormalizeExpr(p.evalCtx, tuple[i])
			if err != nil {
				return nil, err
			}
			tuple[i], err = p.expandSubqueries(tuple[i], 1)
			if err != nil {
				return nil, err
			}
		}
		data, err := tuple.Eval(p.evalCtx)
		if err != nil {
			return nil, err
		}
		vals, ok := data.(parser.DTuple)
		if !ok {
			return nil, fmt.Errorf("expected a tuple, but found %T", data)
		}
		if len(v.rows) == 0 {
			nCols = len(vals)
		} else if nCols != len(vals) {
			return nil, errors.New("VALUES lists must all be the same length")
		}
		v.rows = append(v.rows, vals)
	}

	v.columns = make([]string, nCols)
	for i := 0; i < nCols; i++ {
		v.columns[i] = fmt.Sprintf("column%d", i+1)
	}
	return v, nil
}

type valuesNode struct {
	columns  []string
	ordering []int
	rows     []parser.DTuple
	nextRow  int // The index of the next row.
}

func (n *valuesNode) Columns() []string {
	return n.columns
}

func (n *valuesNode) Ordering() ([]int, int) {
	return nil, 0
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

func (n *valuesNode) Len() int {
	return len(n.rows)
}

func (n *valuesNode) Less(i, j int) bool {
	// TODO(pmattis): An alternative to this type of field-based comparison would
	// be to construct a sort-key per row using encodeTableKey(). Using a
	// sort-key approach would likely fit better with a disk-based sort.
	ra, rb := n.rows[i], n.rows[j]
	for _, k := range n.ordering {
		var da, db parser.Datum
		if k < 0 {
			da = rb[-(k + 1)]
			db = ra[-(k + 1)]
		} else {
			da = ra[k-1]
			db = rb[k-1]
		}
		// TODO(pmattis): This is assuming that the datum types are compatible. I'm
		// not sure this always holds as `CASE` expressions can return different
		// types for a column for different rows. Investigate how other RDBMs
		// handle this.
		if c := da.Compare(db); c < 0 {
			return true
		} else if c > 0 {
			return false
		}
	}
	return true
}

func (n *valuesNode) Swap(i, j int) {
	n.rows[i], n.rows[j] = n.rows[j], n.rows[i]
}

func (n *valuesNode) ExplainPlan() (name, description string, children []planNode) {
	name = "values"
	pluralize := func(n int) string {
		if n == 1 {
			return ""
		}
		return "s"
	}
	description = fmt.Sprintf("%d column%s, %d row%s",
		len(n.columns), pluralize(len(n.columns)),
		len(n.rows), pluralize(len(n.rows)))
	return name, description, nil
}
