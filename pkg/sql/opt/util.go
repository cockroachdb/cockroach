// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opt

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
)

// VisitFKReferenceTables visits all provided tables as well as recursive
// references from foreign keys (both referenced and referencing). The tables
// are visited in sorted order so that later tables reference earlier tables.
//
// The visiting is controlled by two callbacks:
// - visitPreFn should tell the walker whether it needs to recurse into the
// given table.
// - visitFn allows the caller to do any work on the table being visited.
//   - nil fk argument means that the table is among the provided tables.
//   - non-nil fk argument specifies which FK constraint led to this table.
//
// Note that the same table can be visited multiple times; however, once a table
// is recursed into, it won't be recursed into or visited again in the future
// (i.e. it is marked as "fully processed").
//
// TODO(rytaft): if there is a cycle in the foreign key references,
// statement-bundle recreate will still hit errors. To handle this case, we
// would need to first create the tables without the foreign keys, then add the
// foreign keys later.
func VisitFKReferenceTables(
	ctx context.Context,
	catalog cat.Catalog,
	tables []TableMeta,
	visitPreFn func(_ cat.Table, fk cat.ForeignKeyConstraint) (recurse bool),
	visitFn func(_ cat.Table, fk cat.ForeignKeyConstraint),
) {
	// tableRecursed tracks which tables we've already recursed into. Once a
	// table is recursed into, it is considered "fully processed" and we
	// effectively ignore it from now on. If a table has already been visited
	// but is not recursed into, we still might want to recurse into it later
	// (because we might get to it via a different FK that requires recursing).
	var tableRecursed intsets.Fast
	var visitForeignKeyReferencedTables func(tab cat.Table)
	var visitForeignKeyReferencingTables func(tab cat.Table)
	visitTable := func(table cat.Table, fk cat.ForeignKeyConstraint, recurse bool) {
		tabID := table.ID()
		if recurse {
			tableRecursed.Add(int(tabID))
		}
		// The order of visiting here is important: namely, we want to visit
		// all tables that we reference first, then ourselves, and only then
		// tables that reference us.
		if recurse {
			visitForeignKeyReferencedTables(table)
		}
		visitFn(table, fk)
		if recurse {
			visitForeignKeyReferencingTables(table)
		}
	}
	// handleRelatedTables is a helper function that processes the given table
	// if it hasn't been recursed into yet by visiting all referenced and
	// referencing table of the given one, including via transient (recursive)
	// FK relationships.
	handleRelatedTables := func(tabID cat.StableID, fk cat.ForeignKeyConstraint) {
		if !tableRecursed.Contains(int(tabID)) {
			ds, _, err := catalog.ResolveDataSourceByID(ctx, cat.Flags{}, tabID)
			if err != nil {
				// This is a best-effort attempt to get all the tables, so don't
				// error.
				return
			}
			refTab, ok := ds.(cat.Table)
			if !ok {
				// This is a best-effort attempt to get all the tables, so don't
				// error.
				return
			}
			recurse := visitPreFn(refTab, fk)
			visitTable(refTab, fk, recurse)
		}
	}
	visitForeignKeyReferencedTables = func(tab cat.Table) {
		for i := 0; i < tab.OutboundForeignKeyCount(); i++ {
			fk := tab.OutboundForeignKey(i)
			handleRelatedTables(fk.ReferencedTableID(), fk)
		}
	}
	visitForeignKeyReferencingTables = func(tab cat.Table) {
		for i := 0; i < tab.InboundForeignKeyCount(); i++ {
			fk := tab.InboundForeignKey(i)
			handleRelatedTables(fk.OriginTableID(), fk)
		}
	}
	for _, tabMeta := range tables {
		tabID := tabMeta.Table.ID()
		if !tableRecursed.Contains(int(tabID)) {
			recurse := visitPreFn(tabMeta.Table, nil /* fk */)
			visitTable(tabMeta.Table, nil /* fk */, recurse)
		}
	}
}
