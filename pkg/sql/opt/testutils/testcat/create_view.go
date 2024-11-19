// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testcat

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// CreateView creates a test view from a parsed DDL statement and adds it to the
// catalog.
func (tc *Catalog) CreateView(stmt *tree.CreateView) *View {
	// Update the view name to include catalog and schema if not provided.
	tc.qualifyTableName(&stmt.Name)

	fmtCtx := tree.NewFmtCtx(tree.FmtParsable)
	stmt.AsSource.Format(fmtCtx)

	view := &View{
		ViewID:      tc.nextStableID(),
		ViewName:    stmt.Name,
		QueryText:   fmtCtx.CloseAndGetString(),
		ColumnNames: stmt.ColumnNames,
	}

	// Add the new view to the catalog.
	tc.AddView(view)

	return view
}

func (*View) addIndex(stmt *tree.IndexTableDef) {
	// TODO(cucaroach): implement
	panic("view indexes are not supported by the test catalog")
}
