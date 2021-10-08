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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
	"github.com/cockroachdb/errors"
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

// fieldStr returns a "field" or "field: val" string; only used when this entry
// is a field.
func (e *entry) fieldStr() string {
	if e.fieldVal == "" {
		return e.field
	}
	return fmt.Sprintf("%s: %s", e.field, e.fieldVal)
}

// EnterNode creates a new node as a child of the current node.
func (ob *OutputBuilder) EnterNode(
	name string, columns colinfo.ResultColumns, ordering colinfo.ColumnOrdering,
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

// AddRedactableField adds an information field under the current node, hiding
// the value depending depending on the given redact flag.
func (ob *OutputBuilder) AddRedactableField(flag RedactFlags, key, value string) {
	if ob.flags.Redact.Has(flag) {
		value = "<hidden>"
	}
	ob.AddField(key, value)
}

// Attr adds an information field under the current node.
func (ob *OutputBuilder) Attr(key string, value interface{}) {
	ob.AddField(key, fmt.Sprint(value))
}

// VAttr adds an information field under the current node, if the Verbose flag
// is set.
func (ob *OutputBuilder) VAttr(key string, value interface{}) {
	if ob.flags.Verbose {
		ob.AddField(key, fmt.Sprint(value))
	}
}

// Attrf is a formatter version of Attr.
func (ob *OutputBuilder) Attrf(key, format string, args ...interface{}) {
	ob.AddField(key, fmt.Sprintf(format, args...))
}

// Expr adds an information field with an expression. The expression's
// IndexedVars refer to the given columns. If the expression is nil, nothing is
// emitted.
func (ob *OutputBuilder) Expr(key string, expr tree.TypedExpr, varColumns colinfo.ResultColumns) {
	if expr == nil {
		return
	}
	flags := tree.FmtSymbolicSubqueries
	if ob.flags.ShowTypes {
		flags |= tree.FmtShowTypes
	}
	if ob.flags.HideValues {
		flags |= tree.FmtHideConstants
	}
	f := tree.NewFmtCtx(
		flags,
		tree.FmtIndexedVarFormat(func(ctx *tree.FmtCtx, idx int) {
			// Ensure proper quoting.
			n := tree.Name(varColumns[idx].Name)
			ctx.WriteString(n.String())
		}),
	)
	f.FormatNode(expr)
	ob.AddField(key, f.CloseAndGetString())
}

// VExpr is a verbose-only variant of Expr.
func (ob *OutputBuilder) VExpr(key string, expr tree.TypedExpr, varColumns colinfo.ResultColumns) {
	if ob.flags.Verbose {
		ob.Expr(key, expr, varColumns)
	}
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

// BuildStringRows creates a string representation of the plan information and
// returns it as a list of strings (one for each row). The strings do not end in
// newline.
func (ob *OutputBuilder) BuildStringRows() []string {
	var result []string
	tp := treeprinter.NewWithStyle(treeprinter.BulletStyle)
	stack := []treeprinter.Node{tp}
	entries := ob.entries

	pop := func() *entry {
		e := &entries[0]
		entries = entries[1:]
		return e
	}

	popField := func() *entry {
		if len(entries) > 0 && !entries[0].isNode() {
			return pop()
		}
		return nil
	}

	// There may be some top-level non-node entries (like "distributed"). Print
	// them separately, as they can't be part of the tree.
	for e := popField(); e != nil; e = popField() {
		result = append(result, e.fieldStr())
	}
	if len(result) > 0 {
		result = append(result, "")
	}

	for len(entries) > 0 {
		entry := pop()
		child := stack[entry.level-1].Child(entry.node)
		stack = append(stack[:entry.level], child)
		if entry.columns != "" {
			child.AddLine(fmt.Sprintf("columns: %s", entry.columns))
		}
		if entry.ordering != "" {
			child.AddLine(fmt.Sprintf("ordering: %s", entry.ordering))
		}
		// Add any fields for the node.
		for entry = popField(); entry != nil; entry = popField() {
			child.AddLine(entry.fieldStr())
		}
	}
	result = append(result, tp.FormattedRows()...)
	return result
}

// BuildString creates a string representation of the plan information.
// The output string always ends in a newline.
func (ob *OutputBuilder) BuildString() string {
	rows := ob.BuildStringRows()
	var buf bytes.Buffer
	for _, row := range rows {
		buf.WriteString(row)
		buf.WriteString("\n")
	}
	return buf.String()
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

// AddTopLevelField adds a top-level field. Cannot be called while inside a
// node.
func (ob *OutputBuilder) AddTopLevelField(key, value string) {
	if ob.level != 0 {
		panic(errors.AssertionFailedf("inside node"))
	}
	ob.AddField(key, value)
}

// AddRedactableTopLevelField adds a top-level field, hiding the value depending
// depending on the given redact flag.
func (ob *OutputBuilder) AddRedactableTopLevelField(redactFlag RedactFlags, key, value string) {
	if ob.flags.Redact.Has(redactFlag) {
		value = "<hidden>"
	}
	ob.AddTopLevelField(key, value)
}

// AddDistribution adds a top-level distribution field. Cannot be called
// while inside a node.
func (ob *OutputBuilder) AddDistribution(value string) {
	ob.AddRedactableTopLevelField(RedactDistribution, "distribution", value)
}

// AddVectorized adds a top-level vectorized field. Cannot be called
// while inside a node.
func (ob *OutputBuilder) AddVectorized(value bool) {
	ob.AddRedactableTopLevelField(RedactVectorized, "vectorized", fmt.Sprintf("%t", value))
}

// AddPlanningTime adds a top-level planning time field. Cannot be called
// while inside a node.
func (ob *OutputBuilder) AddPlanningTime(delta time.Duration) {
	if ob.flags.Redact.Has(RedactVolatile) {
		delta = 10 * time.Microsecond
	}
	ob.AddTopLevelField("planning time", humanizeutil.Duration(delta))
}

// AddExecutionTime adds a top-level execution time field. Cannot be called
// while inside a node.
func (ob *OutputBuilder) AddExecutionTime(delta time.Duration) {
	if ob.flags.Redact.Has(RedactVolatile) {
		delta = 100 * time.Microsecond
	}
	ob.AddTopLevelField("execution time", humanizeutil.Duration(delta))
}

// AddKVReadStats adds a top-level field for the bytes/rows read from KV.
func (ob *OutputBuilder) AddKVReadStats(rows, bytes int64) {
	ob.AddTopLevelField("rows read from KV", fmt.Sprintf(
		"%s (%s)", humanizeutil.Count(uint64(rows)), humanizeutil.IBytes(bytes),
	))
}

// AddKVTime adds a top-level field for the cumulative time spent in KV.
func (ob *OutputBuilder) AddKVTime(kvTime time.Duration) {
	ob.AddRedactableTopLevelField(RedactVolatile, "cumulative time spent in KV", humanizeutil.Duration(kvTime))
}

// AddContentionTime adds a top-level field for the cumulative contention time.
func (ob *OutputBuilder) AddContentionTime(contentionTime time.Duration) {
	ob.AddRedactableTopLevelField(
		RedactVolatile,
		"cumulative time spent due to contention",
		humanizeutil.Duration(contentionTime),
	)
}

// AddMaxMemUsage adds a top-level field for the memory used by the query.
func (ob *OutputBuilder) AddMaxMemUsage(bytes int64) {
	ob.AddRedactableTopLevelField(
		RedactVolatile, "maximum memory usage", humanizeutil.IBytes(bytes),
	)
}

// AddNetworkStats adds a top-level field for network statistics.
func (ob *OutputBuilder) AddNetworkStats(messages, bytes int64) {
	ob.AddRedactableTopLevelField(
		RedactVolatile,
		"network usage",
		fmt.Sprintf("%s (%s messages)", humanizeutil.IBytes(bytes), humanizeutil.Count(uint64(messages))),
	)
}

// AddMaxDiskUsage adds a top-level field for the sql temporary disk space used
// by the query. If we're redacting leave this out to keep logic test output
// independent of disk spilling. Disk spilling is controlled by a metamorphic
// constant so it may or may not occur randomly so it makes sense to omit this
// information entirely if we're redacting. Since disk spilling is rare we only
// include this field is bytes is greater than zero.
func (ob *OutputBuilder) AddMaxDiskUsage(bytes int64) {
	if !ob.flags.Redact.Has(RedactVolatile) && bytes > 0 {
		ob.AddTopLevelField("max sql temp disk usage",
			humanizeutil.IBytes(bytes))
	}
}

// AddRegionsStats adds a top-level field for regions executed on statistics.
func (ob *OutputBuilder) AddRegionsStats(regions []string) {
	ob.AddRedactableTopLevelField(
		RedactNodes,
		"regions",
		strings.Join(regions, ", "),
	)
}
