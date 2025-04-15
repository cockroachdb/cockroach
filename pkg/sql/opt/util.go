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
// - recurseFn should tell the visitor whether it should recurse into FK
// reference tables of the given one.
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
	recurseFn func(_ cat.Table, fk cat.ForeignKeyConstraint) bool,
	visitFn func(_ cat.Table, fk cat.ForeignKeyConstraint),
) {
	// tableRecursed tracks which tables we've already recursed into. Once a
	// table is recursed into, it is considered "fully processed" and we
	// effectively ignore it from now on. If a table has already been visited
	// but is not recursed into, we still might want to recurse into it later
	// (because we might get to it via a different FK that requires recursion).
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
			recurse := recurseFn(refTab, fk)
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
			recurse := recurseFn(tabMeta.Table, nil /* fk */)
			visitTable(tabMeta.Table, nil /* fk */, recurse)
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

// FamiliesForCols returns the set of column families for the set of cols.
func FamiliesForCols(tab cat.Table, tabID TableID, cols ColSet) (families intsets.Fast) {
	for i, n := 0, tab.FamilyCount(); i < n; i++ {
		family := tab.Family(i)
		for j, m := 0, family.ColumnCount(); j < m; j++ {
			if cols.Contains(tabID.ColumnID(family.Column(j).Ordinal)) {
				families.Add(i)
				break
			}
		}
	}
	return families
}
