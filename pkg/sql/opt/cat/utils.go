// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package cat

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
	"github.com/pkg/errors"
)

// ExpandDataSourceGlob is a utility function that expands a tree.TablePattern
// into a list of object names.
func ExpandDataSourceGlob(
	ctx context.Context, catalog Catalog, flags Flags, pattern tree.TablePattern,
) ([]DataSourceName, error) {

	switch p := pattern.(type) {
	case *tree.TableName:
		_, name, err := catalog.ResolveDataSource(ctx, flags, p)
		if err != nil {
			return nil, err
		}
		return []DataSourceName{name}, nil

	case *tree.AllTablesSelector:
		schema, _, err := catalog.ResolveSchema(ctx, flags, &p.TableNamePrefix)
		if err != nil {
			return nil, err
		}

		return schema.GetDataSourceNames(ctx)

	default:
		return nil, errors.Errorf("invalid TablePattern type %T", p)
	}
}

// ResolveTableIndex resolves a TableIndexName.
func ResolveTableIndex(
	ctx context.Context, catalog Catalog, flags Flags, name *tree.TableIndexName,
) (Index, error) {
	if name.Table.TableName != "" {
		ds, _, err := catalog.ResolveDataSource(ctx, flags, &name.Table)
		if err != nil {
			return nil, err
		}
		table, ok := ds.(Table)
		if !ok {
			return nil, pgerror.Newf(
				pgcode.WrongObjectType, "%q is not a table", name.Table.TableName,
			)
		}
		if name.Index == "" {
			// Return primary index.
			return table.Index(0), nil
		}
		for i := 0; i < table.IndexCount(); i++ {
			if idx := table.Index(i); idx.Name() == tree.Name(name.Index) {
				return idx, nil
			}
		}
		return nil, pgerror.Newf(
			pgcode.UndefinedObject, "index %q does not exist", name.Index,
		)
	}

	// We have to search for a table that has an index with the given name.
	schema, _, err := catalog.ResolveSchema(ctx, flags, &name.Table.TableNamePrefix)
	if err != nil {
		return nil, err
	}
	dsNames, err := schema.GetDataSourceNames(ctx)
	if err != nil {
		return nil, err
	}
	var found Index
	for i := range dsNames {
		ds, _, err := catalog.ResolveDataSource(ctx, flags, &dsNames[i])
		if err != nil {
			return nil, err
		}
		table, ok := ds.(Table)
		if !ok {
			// Not a table, ignore.
			continue
		}
		for i := 0; i < table.IndexCount(); i++ {
			if idx := table.Index(i); idx.Name() == tree.Name(name.Index) {
				if found != nil {
					return nil, pgerror.Newf(pgcode.AmbiguousParameter,
						"index name %q is ambiguous (found in %s and %s)",
						name.Index, table.Name().String(), found.Table().Name().String())
				}
				found = idx
				break
			}
		}
	}
	if found == nil {
		return nil, pgerror.Newf(
			pgcode.UndefinedObject, "index %q does not exist", name.Index,
		)
	}
	return found, nil
}

// FindTableColumnByName returns the ordinal of the non-mutation column having
// the given name, if one exists in the given table. Otherwise, it returns -1.
func FindTableColumnByName(tab Table, name tree.Name) int {
	for ord, n := 0, tab.ColumnCount(); ord < n; ord++ {
		if tab.Column(ord).ColName() == name {
			return ord
		}
	}
	return -1
}

// FormatTable nicely formats a catalog table using a treeprinter for debugging
// and testing.
func FormatTable(cat Catalog, tab Table, tp treeprinter.Node) {
	child := tp.Childf("TABLE %s", tab.Name().TableName)

	var buf bytes.Buffer
	for i := 0; i < tab.DeletableColumnCount(); i++ {
		buf.Reset()
		formatColumn(tab.Column(i), IsMutationColumn(tab, i), &buf)
		child.Child(buf.String())
	}

	// If we only have one primary family (the default), don't print it.
	if tab.FamilyCount() > 1 || tab.Family(0).Name() != "primary" {
		for i := 0; i < tab.FamilyCount(); i++ {
			buf.Reset()
			formatFamily(tab.Family(i), &buf)
			child.Child(buf.String())
		}
	}

	for i := 0; i < tab.CheckCount(); i++ {
		child.Childf("CHECK (%s)", tab.Check(i).Constraint)
	}

	for i := 0; i < tab.DeletableIndexCount(); i++ {
		formatCatalogIndex(tab, i, child)
	}

	for i := 0; i < tab.OutboundForeignKeyCount(); i++ {
		formatCatalogFKRef(cat, false /* inbound */, tab.OutboundForeignKey(i), child)
	}

	for i := 0; i < tab.InboundForeignKeyCount(); i++ {
		formatCatalogFKRef(cat, true /* inbound */, tab.InboundForeignKey(i), child)
	}

	// TODO(radu): show stats.
}

// formatCatalogIndex nicely formats a catalog index using a treeprinter for
// debugging and testing.
func formatCatalogIndex(tab Table, ord int, tp treeprinter.Node) {
	idx := tab.Index(ord)
	inverted := ""
	if idx.IsInverted() {
		inverted = "INVERTED "
	}
	mutation := ""
	if IsMutationIndex(tab, ord) {
		mutation = " (mutation)"
	}
	child := tp.Childf("%sINDEX %s%s", inverted, idx.Name(), mutation)

	var buf bytes.Buffer
	colCount := idx.ColumnCount()
	if ord == PrimaryIndex {
		// Omit the "stored" columns from the primary index.
		colCount = idx.KeyColumnCount()
	}

	for i := 0; i < colCount; i++ {
		buf.Reset()

		idxCol := idx.Column(i)
		formatColumn(idxCol.Column, false /* isMutationCol */, &buf)
		if idxCol.Descending {
			fmt.Fprintf(&buf, " desc")
		}

		if i >= idx.LaxKeyColumnCount() {
			fmt.Fprintf(&buf, " (storing)")
		}

		child.Child(buf.String())
	}

	FormatZone(idx.Zone(), child)
}

// formatColPrefix returns a string representation of a list of columns. The
// columns are provided through a function.
func formatCols(tab Table, numCols int, colOrdinal func(tab Table, i int) int) string {
	var buf bytes.Buffer
	buf.WriteByte('(')
	for i := 0; i < numCols; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		colName := tab.Column(colOrdinal(tab, i)).ColName()
		buf.WriteString(colName.String())
	}
	buf.WriteByte(')')

	return buf.String()
}

// formatCatalogFKRef nicely formats a catalog foreign key reference using a
// treeprinter for debugging and testing.
func formatCatalogFKRef(
	cat Catalog, inbound bool, fkRef ForeignKeyConstraint, tp treeprinter.Node,
) {
	originDS, err := cat.ResolveDataSourceByID(context.TODO(), fkRef.OriginTableID())
	if err != nil {
		panic(err)
	}
	refDS, err := cat.ResolveDataSourceByID(context.TODO(), fkRef.ReferencedTableID())
	if err != nil {
		panic(err)
	}
	title := "CONSTRAINT"
	if inbound {
		title = "REFERENCED BY " + title
	}

	tp.Childf(
		"%s %s FOREIGN KEY %v %s REFERENCES %v %s",
		title,
		fkRef.Name(),
		originDS.Name(),
		formatCols(originDS.(Table), fkRef.ColumnCount(), fkRef.OriginColumnOrdinal),
		refDS.Name(),
		formatCols(refDS.(Table), fkRef.ColumnCount(), fkRef.ReferencedColumnOrdinal),
	)
}

func formatColumn(col Column, isMutationCol bool, buf *bytes.Buffer) {
	fmt.Fprintf(buf, "%s %s", col.ColName(), col.DatumType())
	if !col.IsNullable() {
		fmt.Fprintf(buf, " not null")
	}
	if col.IsComputed() {
		fmt.Fprintf(buf, " as (%s) stored", col.ComputedExprStr())
	}
	if col.HasDefault() {
		fmt.Fprintf(buf, " default (%s)", col.DefaultExprStr())
	}
	if col.IsHidden() {
		fmt.Fprintf(buf, " [hidden]")
	}
	if isMutationCol {
		fmt.Fprintf(buf, " [mutation]")
	}
}

func formatFamily(family Family, buf *bytes.Buffer) {
	fmt.Fprintf(buf, "FAMILY %s (", family.Name())
	for i, n := 0, family.ColumnCount(); i < n; i++ {
		if i != 0 {
			buf.WriteString(", ")
		}
		col := family.Column(i)
		buf.WriteString(string(col.ColName()))
	}
	buf.WriteString(")")
}
