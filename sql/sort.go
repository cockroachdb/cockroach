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
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/log"
)

// orderBy constructs a sortNode based on the ORDER BY clause. Construction of
// the sortNode might adjust the number of render targets in the scanNode if
// any ordering expressions are specified.
func (p *planner) orderBy(n *parser.Select, s *scanNode) (*sortNode, error) {
	if n.OrderBy == nil {
		return nil, nil
	}

	// We grab a copy of columns here because we might add new render targets
	// below. This is the set of columns requested by the query.
	columns := s.Columns()
	var ordering []int

	for _, o := range n.OrderBy {
		index := 0

		// Normalize the expression which has the side-effect of evaluating
		// constant expressions and unwrapping expressions like "((a))" to "a".
		expr, err := parser.NormalizeExpr(o.Expr)
		if err != nil {
			return nil, err
		}

		if qname, ok := expr.(*parser.QualifiedName); ok {
			if len(qname.Indirect) == 0 {
				// Look for an output column that matches the qualified name. This
				// handles cases like:
				//
				//   SELECT a AS b FROM t ORDER BY b
				target := string(qname.Base)
				for j, col := range columns {
					if equalName(target, col) {
						index = j + 1
						break
					}
				}
			}

			if index == 0 {
				// No output column matched the qualified name, so look for an existing
				// render target that matches the column name. This handles cases like:
				//
				//   SELECT a AS b FROM t ORDER BY a
				if err := qname.NormalizeColumnName(); err != nil {
					return nil, err
				}
				if qname.Table() == "" || equalName(s.desc.Alias, qname.Table()) {
					for j, r := range s.render {
						if qval, ok := r.(*qvalue); ok {
							if equalName(qval.col.Name, qname.Column()) {
								index = j + 1
								break
							}
						}
					}
				}
			}
		}

		if index == 0 {
			// The order by expression matched neither an output column nor an
			// existing render target.
			if datum, ok := expr.(parser.Datum); ok {
				// If we evaluated to an int, use that as an index to order by. This
				// handles cases like:
				//
				//   SELECT * FROM t ORDER BY 1
				i, ok := datum.(parser.DInt)
				if !ok {
					return nil, fmt.Errorf("invalid ORDER BY: %s", expr)
				}
				index = int(i)
				if index < 1 || index > len(columns) {
					return nil, fmt.Errorf("invalid ORDER BY index: %d not in range [1, %d]",
						index, len(columns))
				}
			} else {
				// Add a new render expression to use for ordering. This handles cases
				// were the expression is either not a qualified name or is a qualified
				// name that is otherwise not referenced by the query:
				//
				//   SELECT a FROM t ORDER by b
				//   SELECT a, b FROM t ORDER by a+b
				if err := s.addRender(parser.SelectExpr{Expr: expr}); err != nil {
					return nil, err
				}
				index = len(s.columns)
			}
		}

		if o.Direction == parser.Descending {
			index = -index
		}
		ordering = append(ordering, index)
	}

	return &sortNode{columns: columns, ordering: ordering}, nil
}

type sortNode struct {
	plan     planNode
	columns  []string
	ordering []int
	needSort bool
	err      error
}

func (n *sortNode) Columns() []string {
	return n.columns
}

func (n *sortNode) Ordering() []int {
	if n == nil {
		return nil
	}
	return n.ordering
}

func (n *sortNode) Values() parser.DTuple {
	// If an ordering expression was used the number of columns in each row might
	// differ from the number of columns requested, so trim the result.
	v := n.plan.Values()
	return v[:len(n.columns)]
}

func (n *sortNode) Next() bool {
	if n.needSort {
		n.needSort = false
		if !n.initValues() {
			return false
		}
	}
	return n.plan.Next()
}

func (n *sortNode) Err() error {
	return n.err
}

func (n *sortNode) ExplainPlan() (name, description string, children []planNode) {
	if n.needSort {
		name = "sort"
	} else {
		name = "nosort"
	}

	columns := n.plan.Columns()
	strs := make([]string, len(n.ordering))
	for i, o := range n.ordering {
		prefix := '+'
		if o < 0 {
			o = -o
			prefix = '-'
		}
		strs[i] = fmt.Sprintf("%c%s", prefix, columns[o-1])
	}
	description = strings.Join(strs, ",")

	return name, description, []planNode{n.plan}
}

// wrap the supplied planNode with the sortNode if sorting is required.
func (n *sortNode) wrap(plan planNode) planNode {
	if n != nil {
		// Check to see if the requested ordering is compatible with the existing
		// ordering.
		existingOrdering := plan.Ordering()
		for i := range n.ordering {
			if i >= len(existingOrdering) || n.ordering[i] != existingOrdering[i] {
				if log.V(2) {
					log.Infof("Sort: %d != %d", existingOrdering, n.ordering)
				}
				n.plan = plan
				n.needSort = true
				return n
			}
		}

		if len(n.columns) < len(plan.Columns()) {
			// No sorting required, but we have to strip off the extra render
			// expressions we added.
			n.plan = plan
			return n
		}
	}

	if log.V(2) {
		log.Infof("Sort: no sorting required")
	}
	return plan
}

func (n *sortNode) initValues() bool {
	// TODO(pmattis): If the result set is large, we might need to perform the
	// sort on disk.
	v := &valuesNode{ordering: n.ordering}
	for n.plan.Next() {
		values := n.plan.Values()
		valuesCopy := make(parser.DTuple, len(values))
		copy(valuesCopy, values)
		v.rows = append(v.rows, valuesCopy)
	}
	n.err = n.plan.Err()
	if n.err != nil {
		return false
	}
	sort.Sort(v)
	n.plan = v
	return true
}
