// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package explain

import (
	"bytes"
	"fmt"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// OutputBuilder is used to build the output of an explain tree.
//
// See ExampleOutputBuilder for sample usage.
type OutputBuilder struct {
	flags   Flags
	entries []entry

	// Current depth level (# of EnterNode() calls - # of LeaveNode() calls).
	level int
}

// NewOutputBuilder creates a new OutputBuilder.
//
// EnterNode / EnterMetaNode and AddField should be used to populate data, after
// which a Build* method should be used.
func NewOutputBuilder(flags Flags) *OutputBuilder {
	return &OutputBuilder{flags: flags}
}

type entry struct {
	// level is non-zero for node entries, zero for field entries.
	level    int
	node     string
	columns  string
	ordering string

	field    string
	fieldVal string
}

func (e *entry) isNode() bool {
	return e.level > 0
}

// EnterNode creates a new node as a child of the current node.
func (ob *OutputBuilder) EnterNode(
	name string, columns sqlbase.ResultColumns, ordering sqlbase.ColumnOrdering,
) {
	var colStr, ordStr string
	if ob.flags.Verbose {
		colStr = columns.String(ob.flags.ShowTypes, false /* showHidden */)
		ordStr = ordering.String(columns)
	}
	ob.enterNode(name, colStr, ordStr)
}

// EnterMetaNode is like EnterNode, but the output will always have empty
// strings for the columns and ordering. This is used for "meta nodes" like
// "fk-cascade".
func (ob *OutputBuilder) EnterMetaNode(name string) {
	ob.enterNode(name, "", "")
}

func (ob *OutputBuilder) enterNode(name, columns, ordering string) {
	ob.level++
	ob.entries = append(ob.entries, entry{
		level:    ob.level,
		node:     name,
		columns:  columns,
		ordering: ordering,
	})
}

// LeaveNode moves the current node back up the tree by one level.
func (ob *OutputBuilder) LeaveNode() {
	ob.level--
}

// AddField adds an information field under the current node.
func (ob *OutputBuilder) AddField(key, value string) {
	ob.entries = append(ob.entries, entry{field: key, fieldVal: value})
}

// buildTreeRows creates the treeprinter structure; returns one string for each
// entry in ob.entries.
func (ob *OutputBuilder) buildTreeRows() []string {
	// We reconstruct the hierarchy using the levels.
	// stack keeps track of the current node on each level.
	tp := treeprinter.New()
	stack := []treeprinter.Node{tp}

	for _, entry := range ob.entries {
		if entry.isNode() {
			stack = append(stack[:entry.level], stack[entry.level-1].Child(entry.node))
		} else {
			tp.AddEmptyLine()
		}
	}

	treeRows := tp.FormattedRows()
	for len(treeRows) < len(ob.entries) {
		// This shouldn't happen - the formatter should emit one row per entry.
		// But just in case, add empty strings if necessary to avoid a panic later.
		treeRows = append(treeRows, "")
	}

	return treeRows
}

// BuildExplainRows builds the output rows for an EXPLAIN (PLAN) statement.
//
// The columns are:
//   verbose=false:  Tree Field Description
//   verbose=true:   Tree Level Type Field Description
func (ob *OutputBuilder) BuildExplainRows() []tree.Datums {
	treeRows := ob.buildTreeRows()
	rows := make([]tree.Datums, len(ob.entries))
	level := 1
	for i, e := range ob.entries {
		if e.isNode() {
			level = e.level
		}
		if !ob.flags.Verbose {
			rows[i] = tree.Datums{
				tree.NewDString(treeRows[i]), // Tree
				tree.NewDString(e.field),     // Field
				tree.NewDString(e.fieldVal),  // Description
			}
		} else {
			rows[i] = tree.Datums{
				tree.NewDString(treeRows[i]),       // Tree
				tree.NewDInt(tree.DInt(level - 1)), // Level
				tree.NewDString(e.node),            // Type
				tree.NewDString(e.field),           // Field
				tree.NewDString(e.fieldVal),        // Description
				tree.NewDString(e.columns),         // Columns
				tree.NewDString(e.ordering),        // Ordering
			}
		}
	}
	return rows
}

// BuildString creates a string representation of the plan information.
// The output string always ends in a newline.
func (ob *OutputBuilder) BuildString() string {
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)

	treeRows := ob.buildTreeRows()
	for i, e := range ob.entries {
		fmt.Fprintf(tw, "%s\t%s\t%s", treeRows[i], e.field, e.fieldVal)
		if ob.flags.Verbose {
			fmt.Fprintf(tw, "\t%s\t%s", e.columns, e.ordering)
		}
		fmt.Fprintf(tw, "\n")
	}
	_ = tw.Flush()
	return util.RemoveTrailingSpaces(buf.String())
}

// BuildProtoTree creates a representation of the plan as a tree of
// roachpb.ExplainTreePlanNodes.
func (ob *OutputBuilder) BuildProtoTree() *roachpb.ExplainTreePlanNode {
	// We reconstruct the hierarchy using the levels.
	// stack keeps track of the current node on each level. We use a sentinel node
	// for level 0.
	sentinel := &roachpb.ExplainTreePlanNode{}
	stack := []*roachpb.ExplainTreePlanNode{sentinel}

	for _, entry := range ob.entries {
		if entry.isNode() {
			parent := stack[entry.level-1]
			child := &roachpb.ExplainTreePlanNode{Name: entry.node}
			parent.Children = append(parent.Children, child)
			stack = append(stack[:entry.level], child)
		} else {
			node := stack[len(stack)-1]
			node.Attrs = append(node.Attrs, &roachpb.ExplainTreePlanNode_Attr{
				Key:   entry.field,
				Value: entry.fieldVal,
			})
		}
	}

	return sentinel.Children[0]
}
