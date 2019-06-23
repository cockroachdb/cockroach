// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
