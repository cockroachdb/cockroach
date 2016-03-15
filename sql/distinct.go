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

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
)

// distinct constructs a distinctNode.
func (*planner) distinct(n *parser.SelectClause, p planNode) planNode {
	if !n.Distinct {
		return p
	}
	d := &distinctNode{
		planNode:   p,
		suffixSeen: make(map[string]struct{}),
	}
	ordering := p.Ordering()
	if !ordering.isEmpty() {
		d.columnsInOrder = make([]bool, len(p.Columns()))
		for colIdx := range ordering.exactMatchCols {
			if colIdx >= len(d.columnsInOrder) {
				// If the exact-match column is not part of the output, we can safely ignore it.
				continue
			}
			d.columnsInOrder[colIdx] = true
		}
		for _, c := range ordering.ordering {
			if c.colIdx >= len(d.columnsInOrder) {
				// Cannot use sort order. This happens when the
				// columns used for sorting are not part of the output.
				// e.g. SELECT a FROM t ORDER BY c.
				d.columnsInOrder = nil
				break
			}
			d.columnsInOrder[c.colIdx] = true
		}
	}
	return d
}

type distinctNode struct {
	planNode
	// All the columns that are part of the Sort. Set to nil if no-sort, or
	// sort used an expression that was not part of the requested column set.
	columnsInOrder []bool
	// encoding of the columnsInOrder columns for the previous row.
	prefixSeen []byte
	// encoding of the non-columnInOrder columns for rows sharing the same
	// prefixSeen value.
	suffixSeen map[string]struct{}
	pErr       *roachpb.Error
	explain    explainMode
	debugVals  debugValues
}

func (n *distinctNode) MarkDebug(mode explainMode) {
	if mode != explainDebug {
		panic(fmt.Sprintf("unknown debug mode %d", mode))
	}
	n.explain = mode
	n.planNode.MarkDebug(mode)
}

func (n *distinctNode) DebugValues() debugValues {
	if n.explain != explainDebug {
		panic(fmt.Sprintf("node not in debug mode (mode %d)", n.explain))
	}
	return n.debugVals
}

func (n *distinctNode) Next() bool {
	if n.pErr != nil {
		return false
	}
	for n.planNode.Next() {
		if n.explain == explainDebug {
			n.debugVals = n.planNode.DebugValues()
			if n.debugVals.output != debugValueRow {
				// Let the non-row debug values pass through.
				return true
			}
		}
		// Detect duplicates
		prefix, suffix := n.encodeValues(n.Values())
		if n.pErr != nil {
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
	n.pErr = n.planNode.PErr()
	return false
}

func (n *distinctNode) PErr() *roachpb.Error {
	return n.pErr
}

func (n *distinctNode) encodeValues(values parser.DTuple) ([]byte, []byte) {
	var prefix, suffix []byte
	for i, val := range values {
		if n.columnsInOrder != nil && n.columnsInOrder[i] {
			if prefix == nil {
				prefix = make([]byte, 0, 100)
			}
			var err error
			prefix, err = encodeDatum(prefix, val)
			n.pErr = roachpb.NewError(err)
		} else {
			if suffix == nil {
				suffix = make([]byte, 0, 100)
			}
			var err error
			suffix, err = encodeDatum(suffix, val)
			n.pErr = roachpb.NewError(err)
		}
		if n.pErr != nil {
			break
		}
	}
	return prefix, suffix
}

func (n *distinctNode) ExplainPlan() (string, string, []planNode) {
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
	return "distinct", description, []planNode{n.planNode}
}

func (n *distinctNode) SetLimitHint(numRows int64, soft bool) {
	// Any limit becomes a "soft" limit underneath.
	n.planNode.SetLimitHint(numRows, true)
}
