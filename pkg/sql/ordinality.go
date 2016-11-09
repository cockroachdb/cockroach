// Copyright 2016 The Cockroach Authors.
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
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// ordinalityNode represents a node that adds an "ordinality" column
// to its child node which numbers the rows it produces. Used support
// WITH ORDINALITY.
//
// Note that the ordinalityNode produces results that number the *full
// set of original values*, as defined by the upstream data source
// specification. In particular, applying a filter before or after
// an intermediate ordinalityNode will produce different results.
//
// In other words, *ordinalityNode establishes a barrier to many
// common SQL optimizations*. Its use should be limited in clients to
// situations where the corresponding performance cost is affordable.
type ordinalityNode struct {
	source   planNode
	ordering orderingInfo
	columns  ResultColumns
	row      parser.DTuple
	curCnt   int64
}

func (p *planner) wrapOrdinality(src planNode) (planNode, error) {
	srcColumns := src.Columns()

	res := &ordinalityNode{
		source: src,
		row:    make(parser.DTuple, len(srcColumns)+1),
		curCnt: 1,
	}

	// Allocate an extra column for the ordinality values
	res.columns = make(ResultColumns, len(srcColumns)+1)
	copy(res.columns, srcColumns)
	res.columns[len(res.columns)-1] = ResultColumn{
		Name: "ordinality",
		Typ:  parser.TypeInt,
	}

	// Copy the ordering from the source node, making room for the new
	// column.
	origOrdering := src.Ordering()
	res.ordering.exactMatchCols = make(map[int]struct{})
	for k, v := range origOrdering.exactMatchCols {
		res.ordering.exactMatchCols[k] = v
	}
	res.ordering.ordering = make(sqlbase.ColumnOrdering, len(res.ordering.ordering)+1)
	copy(res.ordering.ordering[1:], origOrdering.ordering)
	// Inform upper nodes that the ordinality column establishes
	// ordering.
	extraColIdx := len(res.columns) - 1
	res.ordering.exactMatchCols[extraColIdx] = struct{}{}
	res.ordering.ordering[0] = sqlbase.ColumnOrderInfo{
		ColIdx:    extraColIdx,
		Direction: encoding.Ascending,
	}
	res.ordering.unique = true

	return res, nil
}

func (o *ordinalityNode) Next() (bool, error) {
	hasNext, err := o.source.Next()
	if !hasNext || err != nil {
		return hasNext, err
	}
	copy(o.row, o.source.Values())
	o.row[len(o.row)-1] = parser.NewDInt(parser.DInt(o.curCnt))
	o.curCnt++
	return true, nil
}

func (o *ordinalityNode) ExplainPlan(_ bool) (string, string, []planNode) {
	return "ordinality", "", []planNode{o.source}
}

func (o *ordinalityNode) Ordering() orderingInfo                { return o.ordering }
func (o *ordinalityNode) Values() parser.DTuple                 { return o.row }
func (o *ordinalityNode) DebugValues() debugValues              { return o.source.DebugValues() }
func (o *ordinalityNode) MarkDebug(mode explainMode)            { o.source.MarkDebug(mode) }
func (o *ordinalityNode) Columns() ResultColumns                { return o.columns }
func (o *ordinalityNode) expandPlan() error                     { return o.source.expandPlan() }
func (o *ordinalityNode) Start() error                          { return o.source.Start() }
func (o *ordinalityNode) Close()                                { o.source.Close() }
func (o *ordinalityNode) ExplainTypes(_ func(string, string))   {}
func (o *ordinalityNode) SetLimitHint(numRows int64, soft bool) { o.source.SetLimitHint(numRows, soft) }
