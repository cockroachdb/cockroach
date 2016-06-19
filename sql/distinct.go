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

// distinctNode de-duplicates rows returned by a wrapped planNode.
type distinctNode struct {
	plan planNode
	top  *selectTopNode
	// All the columns that are part of the Sort. Set to nil if no-sort, or
	// sort used an expression that was not part of the requested column set.
	columnsInOrder []bool
	// Encoding of the columnsInOrder columns for the previous row.
	prefixSeen []byte
	// Encoding of the non-columnInOrder columns for rows sharing the same
	// prefixSeen value.
	suffixSeen map[string]struct{}
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
func (n *distinctNode) wrap(plan planNode) planNode {
	if n == nil {
		return plan
	}
	n.plan = plan
	return n
}

func (n *distinctNode) expandPlan() error {
	// At this point the selectTopNode has already expanded the plans
	// upstream of distinctNode.
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
			if c.ColIdx >= len(n.columnsInOrder) {
				// Cannot use sort order. This happens when the
				// columns used for sorting are not part of the output.
				// e.g. SELECT a FROM t ORDER BY c.
				n.columnsInOrder = nil
				break
			}
			n.columnsInOrder[c.ColIdx] = true
		}
	}
	return nil
}

func (n *distinctNode) Start() error {
	n.suffixSeen = make(map[string]struct{})
	return n.plan.Start()
}

// setTop connects the distinctNode back to the selectTopNode that
// caused its existence. This is needed because the distinctNode needs
// to refer to other nodes in the selectTopNode before its
// expandPlan() method has ran and its child plan is known and
// connected.
func (n *distinctNode) setTop(top *selectTopNode) {
	if n != nil {
		n.top = top
	}
}

func (n *distinctNode) Columns() []ResultColumn {
	if n.plan != nil {
		return n.plan.Columns()
	}
	// Pre-prepare: not connected yet. Ask the top select node.
	return n.top.Columns()
}

func (n *distinctNode) Values() parser.DTuple  { return n.plan.Values() }
func (n *distinctNode) Ordering() orderingInfo { return n.plan.Ordering() }

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

func (n *distinctNode) Next() (bool, error) {
	for {
		next, err := n.plan.Next()
		if !next {
			return false, err
		}
		if n.explain == explainDebug {
			n.debugVals = n.plan.DebugValues()
			if n.debugVals.output != debugValueRow {
				// Let the non-row debug values pass through.
				return true, nil
			}
		}
		// Detect duplicates
		prefix, suffix, err := n.encodeValues(n.Values())
		if err != nil {
			return false, err
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
			return true, nil
		}

		// The prefix of the row is the same as the last row; check
		// to see if the suffix which is not ordered has been seen.
		if suffix != nil {
			sKey := string(suffix)
			if _, ok := n.suffixSeen[sKey]; !ok {
				n.suffixSeen[sKey] = struct{}{}
				return true, nil
			}
		}

		// The row is a duplicate
		if n.explain == explainDebug {
			// Return as a filtered row.
			n.debugVals.output = debugValueFiltered
			return true, nil
		}
	}
}

func (n *distinctNode) encodeValues(values parser.DTuple) ([]byte, []byte, error) {
	var prefix, suffix []byte
	var err error
	for i, val := range values {
		if n.columnsInOrder != nil && n.columnsInOrder[i] {
			if prefix == nil {
				prefix = make([]byte, 0, 100)
			}
			prefix, err = sqlbase.EncodeDatum(prefix, val)
		} else {
			if suffix == nil {
				suffix = make([]byte, 0, 100)
			}
			suffix, err = sqlbase.EncodeDatum(suffix, val)
		}
		if err != nil {
			break
		}
	}
	return prefix, suffix, err
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
