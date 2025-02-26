// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opt

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
)

// VisitFKReferenceTables visits all provided tables as well as recursive
// references from foreign keys (both referenced and referencing). The tables
// are visited in sorted order so that later tables reference earlier tables.
//
// The visiting is controlled by two callbacks:
// - visitPreFn should tell the visitor whether the FKs of the given tables
// should be explored (i.e. whether the visitor should "recurse" into FK
// reference tables of the given one).
// - visitFn allows the caller to do any work on the table being visited.
//
// In both functions:
// - nil fk argument means that the table is among the provided "original"
// tables.
// - non-nil fk argument specifies which FK constraint led to this table.
//
// Note that the same table can be visited multiple times; however, once a table
// is recursed into, it won't be recursed into or visited again in the future
// (i.e. it is marked as "fully processed").
func VisitFKReferenceTables(
	ctx context.Context,
	catalog cat.Catalog,
	tables []TableMeta,
	visitPreFn func(_ cat.Table, fk cat.ForeignKeyConstraint) (exploreFKs bool),
	visitFn func(_ cat.Table, fk cat.ForeignKeyConstraint),
) {
	// tableExplored tracks which tables we've already explored FKs of. Once a
	// table is explored, it is considered "fully processed" and we effectively
	// ignore it from now on. If a table has already been visited but is not
	// explored, we still might want to explore it later (because we might get
	// to it via a different FK that requires exploration).
	var tableExplored intsets.Fast
	var visitForeignKeyReferencedTables func(tab cat.Table)
	var visitForeignKeyReferencingTables func(tab cat.Table)
	visitTable := func(table cat.Table, fk cat.ForeignKeyConstraint, exploreFKs bool) {
		tabID := table.ID()
		if exploreFKs {
			tableExplored.Add(int(tabID))
		}
		// The order of visiting here is important: namely, we want to visit
		// all tables that we reference first, then ourselves, and only then
		// tables that reference us.
		if exploreFKs {
			visitForeignKeyReferencedTables(table)
		}
		visitFn(table, fk)
		if exploreFKs {
			visitForeignKeyReferencingTables(table)
		}
	}
	// handleRelatedTables is a helper function that processes the given table
	// if it hasn't been explored yet by visiting all referenced and referencing
	// table of the given one, including via transient (recursive) FK
	// relationships.
	handleRelatedTables := func(tabID cat.StableID, fk cat.ForeignKeyConstraint) {
		if !tableExplored.Contains(int(tabID)) {
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
			exploreFKs := visitPreFn(refTab, fk)
			visitTable(refTab, fk, exploreFKs)
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
		if !tableExplored.Contains(int(tabID)) {
			exploreFKs := visitPreFn(tabMeta.Table, nil /* fk */)
			visitTable(tabMeta.Table, nil /* fk */, exploreFKs)
		}
	}
}

// GetAllFKsAmongTables returns a list of ALTER statements that corresponds to
// all FOREIGN KEY constraints where both the origin and the referenced tables
// are present in the given set of tables. List of the given tables is assumed
// to be unique.
func GetAllFKsAmongTables(
	tables []cat.Table, fullyQualifiedName func(cat.Table) (tree.TableName, error),
) []*tree.AlterTable {
	idToTable := make(map[cat.StableID]cat.Table)
	for _, table := range tables {
		idToTable[table.ID()] = table
	}
	var addFKs []*tree.AlterTable
	for _, origTable := range tables {
		for i := 0; i < origTable.OutboundForeignKeyCount(); i++ {
			fk := origTable.OutboundForeignKey(i)
			refTable, ok := idToTable[fk.ReferencedTableID()]
			if !ok {
				// The referenced table is not in the given list, so we skip
				// this FK constraint.
				continue
			}
			fromCols, toCols := make(tree.NameList, fk.ColumnCount()), make(tree.NameList, fk.ColumnCount())
			for j := range fromCols {
				fromCols[j] = origTable.Column(fk.OriginColumnOrdinal(origTable, j)).ColName()
				toCols[j] = refTable.Column(fk.ReferencedColumnOrdinal(refTable, j)).ColName()
			}
			origTableName, err := fullyQualifiedName(origTable)
			if err != nil {
				continue
			}
			refTableName, err := fullyQualifiedName(refTable)
			if err != nil {
				continue
			}
			addFKs = append(addFKs, &tree.AlterTable{
				Table: origTableName.ToUnresolvedObjectName(),
				Cmds: []tree.AlterTableCmd{
					&tree.AlterTableAddConstraint{
						ConstraintDef: &tree.ForeignKeyConstraintTableDef{
							Name:     tree.Name(fk.Name()),
							Table:    refTableName,
							FromCols: fromCols,
							ToCols:   toCols,
							Actions: tree.ReferenceActions{
								Delete: fk.DeleteReferenceAction(),
								Update: fk.UpdateReferenceAction(),
							},
							Match: fk.MatchMethod(),
						},
					},
				},
			})
		}
	}
	return addFKs
}
