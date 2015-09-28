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
// Author: Vivek Menezes (vivek.menezes@gmail.com)

package sql

import (
	"bytes"
	"strings"

	"github.com/cockroachdb/cockroach/sql/parser"
)

// distinct constructs a distinctNode.
func (*planner) distinct(n *parser.Select, p planNode) planNode {
	if !n.Distinct {
		return p
	}
	d := &distinctNode{
		planNode:   p,
		suffixSeen: make(map[string]struct{}),
	}
	ordering, prefix := p.Ordering()
	if len(ordering) != 0 {
		d.columnsInOrder = make([]bool, len(p.Columns()))
	}
	for _, p := range ordering {
		if p == 0 {
			if prefix > 0 {
				prefix--
				continue
			}
			break
		}
		if p < 0 {
			p = -p
		}
		if p <= len(d.columnsInOrder) {
			d.columnsInOrder[p-1] = true
		} else {
			// Cannot use sort order. This happens when the
			// columns used for sorting are not part of the output.
			// e.g. SELECT a FROM t ORDER BY c.
			d.columnsInOrder = nil
			break
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
	err        error
}

func (n *distinctNode) Next() bool {
	if n.err != nil {
		return false
	}
	for n.planNode.Next() {
		// Detect duplicates
		prefix, suffix := n.encodeValues(n.Values())
		if n.err != nil {
			return false
		}
		if bytes.Equal(prefix, n.prefixSeen) {
			// The prefix of the row is the same as the last row; check
			// to see if the suffix which is not ordered has been seen.
			if suffix == nil {
				// duplicate
				continue
			}
			sKey := string(suffix)
			if _, ok := n.suffixSeen[sKey]; ok {
				// duplicate
				continue
			}
			n.suffixSeen[sKey] = struct{}{}
		} else {
			// The prefix of the row which is ordered differs from the last row;
			// reset our seen set.
			if len(n.suffixSeen) > 0 {
				n.suffixSeen = make(map[string]struct{})
			}
			n.prefixSeen = prefix
			if suffix != nil {
				n.suffixSeen[string(suffix)] = struct{}{}
			}
		}
		return true
	}
	n.err = n.planNode.Err()
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
			prefix, n.err = encodeDatum(prefix, val)
		} else {
			if suffix == nil {
				suffix = make([]byte, 0, 100)
			}
			suffix, n.err = encodeDatum(suffix, val)
		}
		if n.err != nil {
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
				strs = append(strs, column)
			}
		}
		description = strings.Join(strs, ",")
	}
	return "distinct", description, []planNode{n.planNode}
}
