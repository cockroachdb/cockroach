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
	"strconv"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/encoding"
)

// Values constructs a valuesNode from a VALUES expression.
func (p *planner) Values(n parser.Values) (planNode, *roachpb.Error) {
	v := &valuesNode{
		rows: make([]parser.DTuple, 0, len(n)),
	}

	for num, tuple := range n {
		if num == 0 {
			v.columns = make([]ResultColumn, 0, len(tuple))
		} else if a, e := len(tuple), len(v.columns); a != e {
			return nil, roachpb.NewUErrorf("VALUES lists must all be the same length, %d for %d", a, e)
		}

		for i := range tuple {
			var pErr *roachpb.Error
			tuple[i], pErr = p.expandSubqueries(tuple[i], 1)
			if pErr != nil {
				return nil, pErr
			}
			var err error
			typ, err := tuple[i].TypeCheck(p.evalCtx.Args)
			if err != nil {
				return nil, roachpb.NewError(err)
			}
			tuple[i], err = p.parser.NormalizeExpr(p.evalCtx, tuple[i])
			if err != nil {
				return nil, roachpb.NewError(err)
			}
			if num == 0 {
				v.columns = append(v.columns, ResultColumn{Name: "column" + strconv.Itoa(i+1), Typ: typ})
			} else if v.columns[i].Typ == parser.DNull {
				v.columns[i].Typ = typ
			} else if typ != parser.DNull && !typ.TypeEqual(v.columns[i].Typ) {
				return nil, roachpb.NewUErrorf("VALUES list type mismatch, %s for %s", typ.Type(), v.columns[i].Typ.Type())
			}
		}
		data, err := tuple.Eval(p.evalCtx)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		vals, ok := data.(parser.DTuple)
		if !ok {
			return nil, roachpb.NewUErrorf("expected a tuple, but found %T", data)
		}
		v.rows = append(v.rows, vals)
	}

	return v, nil
}

type valuesNode struct {
	columns  []ResultColumn
	ordering columnOrdering
	rows     []parser.DTuple
	nextRow  int // The index of the next row.
}

func (n *valuesNode) Columns() []ResultColumn {
	return n.columns
}

func (n *valuesNode) Ordering() orderingInfo {
	return orderingInfo{}
}

func (n *valuesNode) Values() parser.DTuple {
	return n.rows[n.nextRow-1]
}

func (n *valuesNode) DebugValues() debugValues {
	return debugValues{
		rowIdx: n.nextRow - 1,
		key:    fmt.Sprintf("%d", n.nextRow-1),
		value:  n.rows[n.nextRow-1],
		output: debugValueRow,
	}
}

func (n *valuesNode) Next() bool {
	if n.nextRow >= len(n.rows) {
		return false
	}
	n.nextRow++
	return true
}

func (*valuesNode) PErr() *roachpb.Error {
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
	for _, c := range n.ordering {
		var da, db parser.Datum
		if c.direction == encoding.Ascending {
			da = ra[c.colIdx]
			db = rb[c.colIdx]
		} else {
			da = rb[c.colIdx]
			db = ra[c.colIdx]
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
