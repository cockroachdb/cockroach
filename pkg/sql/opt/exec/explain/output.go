// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package explain

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// OutputBuilder is used to build the output of an explain tree.
//
// See ExampleOutputBuilder for sample usage.
type OutputBuilder struct {
	flags    Flags
	entries  []entry
	warnings []string

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

// AddFlakyField adds an information field under the current node, hiding the
// value depending on the given deflake flags.
func (ob *OutputBuilder) AddFlakyField(flags DeflakeFlags, key, value string) {
	if ob.flags.Deflake.HasAny(flags) {
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
	flags := tree.FmtSymbolicSubqueries | tree.FmtShortenConstants
	if ob.flags.ShowTypes {
		flags |= tree.FmtShowTypes
	}
	if ob.flags.HideValues {
		flags |= tree.FmtHideConstants
	}
	if ob.flags.RedactValues {
		flags |= tree.FmtMarkRedactionNode | tree.FmtOmitNameRedaction
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
			// Do not print "ordering" redundantly  with "order" attribute of
			// sort operators. Note that we choose to keep the latter so that
			// "already ordered" and "K" attributes are printed right after the
			// order.
			if entry.node != nodeNames[sortOp] && entry.node != nodeNames[topKOp] {
				child.AddLine(fmt.Sprintf("ordering: %s", entry.ordering))
			}
		}
		// Add any fields for the node.
		for entry = popField(); entry != nil; entry = popField() {
			field := entry.fieldStr()
			if ob.flags.RedactValues {
				field = string(redact.RedactableString(field).Redact())
			}
			child.AddLine(field)
		}
	}
	result = append(result, tp.FormattedRows()...)
	if len(ob.GetWarnings()) > 0 {
		result = append(result, "")
		result = append(result, ob.GetWarnings()...)
	}
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
// appstatspb.ExplainTreePlanNodes.
func (ob *OutputBuilder) BuildProtoTree() *appstatspb.ExplainTreePlanNode {
	// We reconstruct the hierarchy using the levels.
	// stack keeps track of the current node on each level. We use a sentinel node
	// for level 0.
	sentinel := &appstatspb.ExplainTreePlanNode{}
	stack := []*appstatspb.ExplainTreePlanNode{sentinel}

	for _, entry := range ob.entries {
		if entry.isNode() {
			parent := stack[entry.level-1]
			child := &appstatspb.ExplainTreePlanNode{Name: entry.node}
			parent.Children = append(parent.Children, child)
			stack = append(stack[:entry.level], child)
		} else {
			node := stack[len(stack)-1]
			node.Attrs = append(node.Attrs, &appstatspb.ExplainTreePlanNode_Attr{
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

// AddFlakyTopLevelField adds a top-level field, hiding the value depending on
// the given deflake flags.
func (ob *OutputBuilder) AddFlakyTopLevelField(flags DeflakeFlags, key, value string) {
	if ob.flags.Deflake.HasAny(flags) {
		value = "<hidden>"
	}
	ob.AddTopLevelField(key, value)
}

// AddDistribution adds a top-level distribution field. Cannot be called
// while inside a node.
func (ob *OutputBuilder) AddDistribution(value string) {
	ob.AddFlakyTopLevelField(DeflakeDistribution, "distribution", value)
}

// AddVectorized adds a top-level vectorized field. Cannot be called
// while inside a node.
func (ob *OutputBuilder) AddVectorized(value bool) {
	ob.AddFlakyTopLevelField(DeflakeVectorized, "vectorized", fmt.Sprintf("%t", value))
}

// AddGeneric adds a top-level generic field, if value is true. Cannot be called
// while inside a node.
func (ob *OutputBuilder) AddPlanType(generic, optimized bool) {
	switch {
	case generic && optimized:
		ob.AddTopLevelField("plan type", "generic, re-optimized")
	case generic && !optimized:
		ob.AddTopLevelField("plan type", "generic, reused")
	default:
		ob.AddTopLevelField("plan type", "custom")
	}
}

// AddPlanningTime adds a top-level planning time field. Cannot be called
// while inside a node.
func (ob *OutputBuilder) AddPlanningTime(delta time.Duration) {
	if ob.flags.Deflake.HasAny(DeflakeVolatile) {
		delta = 10 * time.Microsecond
	}
	ob.AddTopLevelField("planning time", string(humanizeutil.Duration(delta)))
}

// AddExecutionTime adds a top-level execution time field. Cannot be called
// while inside a node.
func (ob *OutputBuilder) AddExecutionTime(delta time.Duration) {
	if ob.flags.Deflake.HasAny(DeflakeVolatile) {
		delta = 100 * time.Microsecond
	}
	ob.AddTopLevelField("execution time", string(humanizeutil.Duration(delta)))
}

// AddClientTime adds a top-level client-level protocol time field. Cannot be
// called while inside a node.
func (ob *OutputBuilder) AddClientTime(delta time.Duration) {
	if ob.flags.Deflake.HasAny(DeflakeVolatile) {
		delta = time.Microsecond
	}
	ob.AddTopLevelField("client time", string(humanizeutil.Duration(delta)))
}

// AddKVReadStats adds a top-level field for the bytes/rows/KV pairs read from
// KV as well as for the number of BatchRequests issued.
func (ob *OutputBuilder) AddKVReadStats(rows, bytes, kvPairs, batchRequests int64) {
	var kvs string
	if kvPairs != rows || ob.flags.Verbose {
		// Only show the number of KVs when it's different from the number of
		// rows or if verbose output is requested.
		kvs = fmt.Sprintf("%s KVs, ", humanizeutil.Count(uint64(kvPairs)))
	}
	ob.AddTopLevelField("rows decoded from KV", fmt.Sprintf(
		"%s (%s, %s%s gRPC calls)", humanizeutil.Count(uint64(rows)),
		humanizeutil.IBytes(bytes), kvs, humanizeutil.Count(uint64(batchRequests)),
	))
}

// AddKVTime adds a top-level field for the cumulative time spent in KV.
func (ob *OutputBuilder) AddKVTime(kvTime time.Duration) {
	ob.AddFlakyTopLevelField(
		DeflakeVolatile, "cumulative time spent in KV", string(humanizeutil.Duration(kvTime)))
}

// AddContentionTime adds a top-level field for the cumulative contention time.
func (ob *OutputBuilder) AddContentionTime(contentionTime time.Duration) {
	ob.AddFlakyTopLevelField(
		DeflakeVolatile,
		"cumulative time spent due to contention",
		string(humanizeutil.Duration(contentionTime)),
	)
}

// AddMaxMemUsage adds a top-level field for the memory used by the query.
func (ob *OutputBuilder) AddMaxMemUsage(bytes int64) {
	ob.AddFlakyTopLevelField(
		DeflakeVolatile, "maximum memory usage", string(humanizeutil.IBytes(bytes)),
	)
}

// AddNetworkStats adds a top-level field for network statistics.
func (ob *OutputBuilder) AddNetworkStats(messages, bytes int64) {
	ob.AddFlakyTopLevelField(
		DeflakeVolatile,
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
	if !ob.flags.Deflake.HasAny(DeflakeVolatile) && bytes > 0 {
		ob.AddTopLevelField("max sql temp disk usage",
			string(humanizeutil.IBytes(bytes)))
	}
}

// AddCPUTime adds a top-level field for the cumulative cpu time spent by SQL
// execution. If we're redacting, we leave this out to keep test outputs
// independent of platform because the grunning library isn't currently
// supported on all platforms.
func (ob *OutputBuilder) AddCPUTime(cpuTime time.Duration) {
	if !ob.flags.Deflake.HasAny(DeflakeVolatile) {
		ob.AddTopLevelField("sql cpu time", string(humanizeutil.Duration(cpuTime)))
	}
}

// AddRUEstimate adds a top-level field for the estimated number of RUs consumed
// by the query.
func (ob *OutputBuilder) AddRUEstimate(ru float64) {
	ob.AddFlakyTopLevelField(
		DeflakeVolatile,
		"estimated RUs consumed",
		string(humanizeutil.Countf(ru)),
	)
}

// AddRegionsStats adds a top-level field for regions executed on statistics.
func (ob *OutputBuilder) AddRegionsStats(regions []string) {
	ob.AddFlakyTopLevelField(
		DeflakeNodes,
		"regions",
		strings.Join(regions, ", "),
	)
}

// AddTxnInfo adds top-level fields for information about the query's
// transaction.
func (ob *OutputBuilder) AddTxnInfo(
	txnIsoLevel isolation.Level,
	txnPriority roachpb.UserPriority,
	txnQoSLevel sessiondatapb.QoSLevel,
	asOfSystemTime *eval.AsOfSystemTime,
) {
	ob.AddTopLevelField("isolation level", txnIsoLevel.StringLower())
	ob.AddTopLevelField("priority", txnPriority.String())
	ob.AddTopLevelField("quality of service", txnQoSLevel.String())
	if asOfSystemTime != nil {
		var boundedStaleness string
		if asOfSystemTime.BoundedStaleness {
			boundedStaleness = " (bounded staleness)"
		}
		ts := tree.PGWireFormatTimestamp(asOfSystemTime.Timestamp.GoTime(), nil /* offset */, nil /* tmp */)
		msg := fmt.Sprintf("AS OF SYSTEM TIME %s%s", ts, boundedStaleness)
		ob.AddTopLevelField("historical", msg)
	}
}

// AddWarning adds the provided string to the list of warnings. Warnings will be
// appended to the end of the output produced by BuildStringRows / BuildString.
func (ob *OutputBuilder) AddWarning(warning string) {
	// Do not add duplicate warnings.
	for _, oldWarning := range ob.warnings {
		if oldWarning == warning {
			return
		}
	}
	ob.warnings = append(ob.warnings, warning)
}

// GetWarnings returns the list of unique warnings accumulated so far.
func (ob *OutputBuilder) GetWarnings() []string {
	return ob.warnings
}
