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
// Author: Vivek Menezes (vivek.menezes@gmail.com)

package sql

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
)

// distinctNode de-duplicates row returned by a wrapped planNode.
// This assume the rows are already sorted somehow.
// FIXME: explain how if there is no ORDER BY and no index!
type distinctNode struct {
	plan planNode
	// All the columns that are part of the Sort. Set to nil if no-sort, or
	// sort used an expression that was not part of the requested column set.
	columnsInOrder []bool
	// encoding of the columnsInOrder columns for the previous row.
	prefixSeen []byte
	// encoding of the non-columnInOrder columns for rows sharing the same
	// prefixSeen value.
	suffixSeen map[string]struct{}
	err        error
	explain    explainMode
	debugVals  debugValues
}

// distinct constructs a distinctNode.
func (*planner) Distinct(n *parser.SelectClause) *distinctNode {
	if !n.Distinct {
		return nil
	}
	return &distinctNode{}
}

// wrap connects the distinctNode to its source planNode.
// invoked by selectTopNode.expandPlan() prior
// to invoking distinctNode.expandPlan() below.
func wrapDistinct(n **distinctNode, plan planNode) planNode {
	if *n == nil {
		return plan
	}
	(*n).plan = plan
	return *n
}

func (n *distinctNode) expandPlan() error {
	// At this point the selectTopNode has already expanded the plans
	// before distinctNode.
	ordering := n.plan.Ordering()
	if !ordering.isEmpty() {
		n.columnsInOrder = make([]bool, len(n.plan.Columns()))
		for colIdx := range ordering.exactMatchCols {
			if colIdx >= len(n.columnsInOrder) {
				// If the exact-match column is not part of the output, we can safely ignore it.
				continue
			}
			n.columnsInOrder[colIdx] = true
		}
		for _, c := range ordering.ordering {
			if c.colIdx >= len(n.columnsInOrder) {
				// Cannot use sort order. This happens when the
				// columns used for sorting are not part of the output.
				// e.g. SELECT a FROM t ORDER BY c.
				n.columnsInOrder = nil
				break
			}
			n.columnsInOrder[c.colIdx] = true
		}
	}
	return nil
}

func (n *distinctNode) Start() error {
	n.suffixSeen = make(map[string]struct{})
	return nil
}

func (n *distinctNode) Columns() []ResultColumn { return n.plan.Columns() }
func (n *distinctNode) Values() parser.DTuple   { return n.plan.Values() }
func (n *distinctNode) Ordering() orderingInfo  { return n.plan.Ordering() }

func (n *distinctNode) MarkDebug(mode explainMode) {
	if mode != explainDebug {
		panic(fmt.Sprintf("unknown debug mode %d", mode))
	}
	n.explain = mode
	n.plan.MarkDebug(mode)
}

func (n *distinctNode) DebugValues() debugValues {
	if n.explain != explainDebug {
		panic(fmt.Sprintf("node not in debug mode (mode %d)", n.explain))
	}
	return n.debugVals
}

func (n *distinctNode) Next() bool {
	if n.err != nil {
		return false
	}
	for n.plan.Next() {
		if n.explain == explainDebug {
			n.debugVals = n.plan.DebugValues()
			if n.debugVals.output != debugValueRow {
				// Let the non-row debug values pass through.
				return true
			}
		}
		// Detect duplicates
		prefix, suffix := n.encodeValues(n.Values())
		if n.err != nil {
			return false
		}

		if !bytes.Equal(prefix, n.prefixSeen) {
			// The prefix of the row which is ordered differs from the last row;
			// reset our seen set.
			if len(n.suffixSeen) > 0 {
				n.suffixSeen = make(map[string]struct{})
			}
			n.prefixSeen = prefix
			if suffix != nil {
				n.suffixSeen[string(suffix)] = struct{}{}
			}
			return true
		}

		// The prefix of the row is the same as the last row; check
		// to see if the suffix which is not ordered has been seen.
		if suffix != nil {
			sKey := string(suffix)
			if _, ok := n.suffixSeen[sKey]; !ok {
				n.suffixSeen[sKey] = struct{}{}
				return true
			}
		}

		// The row is a duplicate
		if n.explain == explainDebug {
			// Return as a filtered row.
			n.debugVals.output = debugValueFiltered
			return true
		}
	}
	n.err = n.plan.Err()
	return false
}

func (n *distinctNode) Err() error {
	return n.err
}

func (n *distinctNode) encodeValues(values parser.DTuple) ([]byte, []byte) {
	var prefix, suffix []byte
	for i, val := range values {
		if n.columnsInOrder != nil && n.columnsInOrder[i] {
			if prefix == nil {
				prefix = make([]byte, 0, 100)
			}
			prefix, n.err = sqlbase.EncodeDatum(prefix, val)
		} else {
			if suffix == nil {
				suffix = make([]byte, 0, 100)
			}
			suffix, n.err = sqlbase.EncodeDatum(suffix, val)
		}
		if n.err != nil {
			break
		}
	}
	return prefix, suffix
}

func (n *distinctNode) ExplainPlan(_ bool) (string, string, []planNode) {
	var description string
	if n.columnsInOrder != nil {
		columns := n.Columns()
		strs := make([]string, 0, len(columns))
		for i, column := range columns {
			if n.columnsInOrder[i] {
				strs = append(strs, column.Name)
			}
		}
		description = strings.Join(strs, ",")
	}
	return "distinct", description, []planNode{n.plan}
}

func (n *distinctNode) ExplainTypes(_ func(string, string)) {}

func (n *distinctNode) SetLimitHint(numRows int64, soft bool) {
	// Any limit becomes a "soft" limit underneath.
	n.plan.SetLimitHint(numRows, true)
}
