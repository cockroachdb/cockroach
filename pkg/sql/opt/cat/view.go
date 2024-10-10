// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cat

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// View is an interface to a database view, exposing only the information needed
// by the query optimizer.
type View interface {
	DataSource

	// Query returns the SQL text that specifies the SELECT query that constitutes
	// this view.
	Query() string

	// ColumnNameCount returns the number of column names specified in the view.
	// If zero, then the columns are not aliased. Otherwise, it will match the
	// number of columns in the view.
	ColumnNameCount() int

	// ColumnName returns the name of the column at the ith ordinal position
	// within the view, where i < ColumnNameCount.
	ColumnName(i int) tree.Name

	// IsSystemView returns true if this view is a system view (like
	// crdb_internal.ranges).
	IsSystemView() bool

	// TriggerCount returns the number of triggers present on the view.
	TriggerCount() int

	// Trigger returns the ith trigger, where i < TriggerCount.
	Trigger(i int) Trigger
}

// FormatView nicely formats a catalog view using a treeprinter for debugging
// and testing.
func FormatView(view View, tp treeprinter.Node) {
	var buf bytes.Buffer
	if view.ColumnNameCount() > 0 {
		buf.WriteString(" (")
		for i := 0; i < view.ColumnNameCount(); i++ {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(string(view.ColumnName(i)))
		}
		buf.WriteString(")")
	}

	child := tp.Childf("VIEW %s%s", view.Name(), buf.String())

	child.Child(view.Query())
}
