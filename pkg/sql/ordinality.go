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
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// ordinalityNode represents a node that adds an "ordinality" column
// to its child node which numbers the rows it produces. Used to
// support WITH ORDINALITY.
//
// Note that the ordinalityNode produces results that number the *full
// set of original values*, as defined by the upstream data source
// specification. In particular, applying a filter before or after
// an intermediate ordinalityNode will produce different results.
//
// It is inserted in the logical plan between the renderNode and its
// source node, thus earlier than the WHERE filters.
//
// In other words, *ordinalityNode establishes a barrier to many
// common SQL optimizations*. Its use should be limited in clients to
// situations where the corresponding performance cost is affordable.
type ordinalityNode struct {
	source   planNode
	ordering orderingInfo
	columns  sqlbase.ResultColumns
	row      parser.Datums
	curCnt   int64
}

func (p *planner) wrapOrdinality(ds planDataSource) planDataSource {
	src := ds.plan
	srcColumns := src.Columns()

	res := &ordinalityNode{
		source:   src,
		ordering: src.Ordering(),
		row:      make(parser.Datums, len(srcColumns)+1),
		curCnt:   1,
	}

	// Allocate an extra column for the ordinality values.
	res.columns = make(sqlbase.ResultColumns, len(srcColumns)+1)
	copy(res.columns, srcColumns)
	newColIdx := len(res.columns) - 1
	res.columns[newColIdx] = sqlbase.ResultColumn{
		Name: "ordinality",
		Typ:  parser.TypeInt,
	}

	// Extend the dataSourceInfo with information about the
	// new column.
	ds.info.sourceColumns = res.columns
	if srcIdx, ok := ds.info.sourceAliases.srcIdx(anonymousTable); !ok {
		ds.info.sourceAliases = append(ds.info.sourceAliases, sourceAlias{
			name:        anonymousTable,
			columnRange: []int{newColIdx},
		})
	} else {
		srcAlias := &ds.info.sourceAliases[srcIdx]
		srcAlias.columnRange = append(srcAlias.columnRange, newColIdx)
	}

	ds.plan = res

	return ds
}

func (o *ordinalityNode) Next(ctx context.Context) (bool, error) {
	hasNext, err := o.source.Next(ctx)
	if !hasNext || err != nil {
		return hasNext, err
	}
	copy(o.row, o.source.Values())
	// o.row was allocated one spot larger than o.source.Values().
	// Store the ordinality value there.
	o.row[len(o.row)-1] = parser.NewDInt(parser.DInt(o.curCnt))
	o.curCnt++
	return true, nil
}

func (o *ordinalityNode) Ordering() orderingInfo          { return o.ordering }
func (o *ordinalityNode) Values() parser.Datums           { return o.row }
func (o *ordinalityNode) DebugValues() debugValues        { return o.source.DebugValues() }
func (o *ordinalityNode) MarkDebug(mode explainMode)      { o.source.MarkDebug(mode) }
func (o *ordinalityNode) Columns() sqlbase.ResultColumns  { return o.columns }
func (o *ordinalityNode) Start(ctx context.Context) error { return o.source.Start(ctx) }
func (o *ordinalityNode) Close(ctx context.Context)       { o.source.Close(ctx) }

func (o *ordinalityNode) Spans(ctx context.Context) (_, _ roachpb.Spans, _ error) {
	return o.source.Spans(ctx)
}

// restrictOrdering transforms an ordering requirement on the output
// of an ordinalityNode into an ordering requirement on its input.
func (o *ordinalityNode) restrictOrdering(
	desiredOrdering sqlbase.ColumnOrdering,
) sqlbase.ColumnOrdering {
	// If there's a desired ordering on the ordinality column, drop it.
	if len(desiredOrdering) > 0 {
		newDesired := make(sqlbase.ColumnOrdering, 0, len(desiredOrdering))
		for _, ordInfo := range desiredOrdering {
			if ordInfo.ColIdx == len(o.columns)-1 {
				break
			}
			newDesired = append(newDesired, ordInfo)
		}
		desiredOrdering = newDesired
	}
	return desiredOrdering
}

// optimizeOrdering updates the ordinalityNode's ordering based on a
// potentially new ordering information from its source.
func (o *ordinalityNode) optimizeOrdering() {
	// We are going to "optimize" the ordering. We had an ordering
	// initially from the source, but expand() may have caused it to
	// change. So here retrieve the ordering of the source again.
	origOrdering := o.source.Ordering()

	if len(origOrdering.ordering) > 0 {
		// TODO(knz/radu): we basically have two simultaneous orderings.
		// What we really want is something that orderingInfo cannot
		// currently express: that the rows are ordered by a set of
		// columns AND at the same time they are also ordered by a
		// different set of columns. However since ordinalityNode is
		// currently the only case where this happens we consider it's not
		// worth the hassle and just use the source ordering.
		o.ordering = origOrdering
	} else {
		// No ordering defined in the source, so create a new one.
		o.ordering.exactMatchCols = origOrdering.exactMatchCols
		o.ordering.ordering = sqlbase.ColumnOrdering{
			sqlbase.ColumnOrderInfo{
				ColIdx:    len(o.columns) - 1,
				Direction: encoding.Ascending,
			},
		}
		o.ordering.unique = true
	}
}
