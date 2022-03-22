// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cat

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
	"github.com/cockroachdb/errors"
)

// ExpandDataSourceGlob is a utility function that expands a tree.TablePattern
// into a list of object names.
func ExpandDataSourceGlob(
	ctx context.Context, catalog Catalog, flags Flags, pattern tree.TablePattern,
) ([]DataSourceName, descpb.IDs, error) {
	switch p := pattern.(type) {
	case *tree.TableName:
		ds, name, err := catalog.ResolveDataSource(ctx, flags, p)
		if err != nil {
			return nil, nil, err
		}
		return []DataSourceName{name}, descpb.IDs{descpb.ID(ds.ID())}, nil

	case *tree.AllTablesSelector:
		schema, _, err := catalog.ResolveSchema(ctx, flags, &p.ObjectNamePrefix)
		if err != nil {
			return nil, nil, err
		}

		return schema.GetDataSourceNames(ctx)

	default:
		return nil, nil, errors.Errorf("invalid TablePattern type %T", p)
	}
}

// ResolveTableIndex resolves a TableIndexName.
func ResolveTableIndex(
	ctx context.Context, catalog Catalog, flags Flags, name *tree.TableIndexName,
) (Index, DataSourceName, error) {
	if name.Table.ObjectName != "" {
		ds, tn, err := catalog.ResolveDataSource(ctx, flags, &name.Table)
		if err != nil {
			return nil, DataSourceName{}, err
		}
		table, ok := ds.(Table)
		if !ok {
			return nil, DataSourceName{}, pgerror.Newf(
				pgcode.WrongObjectType, "%q is not a table", name.Table.ObjectName,
			)
		}
		if name.Index == "" {
			// Return primary index.
			return table.Index(0), tn, nil
		}
		for i := 0; i < table.IndexCount(); i++ {
			if idx := table.Index(i); idx.Name() == tree.Name(name.Index) {
				return idx, tn, nil
			}
		}
		// Fallback to referencing @primary as the PRIMARY KEY.
		// Note that indexes with "primary" as their name takes precedence above.
		if name.Index == tabledesc.LegacyPrimaryKeyIndexName {
			return table.Index(0), tn, nil
		}
		return nil, DataSourceName{}, pgerror.Newf(
			pgcode.UndefinedObject, "index %q does not exist", name.Index,
		)
	}

	// We have to search for a table that has an index with the given name.
	schema, _, err := catalog.ResolveSchema(ctx, flags, &name.Table.ObjectNamePrefix)
	if err != nil {
		return nil, DataSourceName{}, err
	}
	dsNames, _, err := schema.GetDataSourceNames(ctx)
	if err != nil {
		return nil, DataSourceName{}, err
	}
	var found Index
	var foundTabName DataSourceName
	for i := range dsNames {
		ds, tn, err := catalog.ResolveDataSource(ctx, flags, &dsNames[i])
		if err != nil {
			return nil, DataSourceName{}, err
		}
		table, ok := ds.(Table)
		if !ok {
			// Not a table, ignore.
			continue
		}
		for i := 0; i < table.IndexCount(); i++ {
			if idx := table.Index(i); idx.Name() == tree.Name(name.Index) {
				if found != nil {
					return nil, DataSourceName{}, pgerror.Newf(pgcode.AmbiguousParameter,
						"index name %q is ambiguous (found in %s and %s)",
						name.Index, tn.String(), foundTabName.String())
				}
				found = idx
				foundTabName = tn
				break
			}
		}
	}
	if found == nil {
		return nil, DataSourceName{}, pgerror.Newf(
			pgcode.UndefinedObject, "index %q does not exist", name.Index,
		)
	}
	return found, foundTabName, nil
}

// FormatTable nicely formats a catalog table using a treeprinter for debugging
// and testing.
func FormatTable(cat Catalog, tab Table, tp treeprinter.Node) {
	child := tp.Childf("TABLE %s", tab.Name())
	if tab.IsVirtualTable() {
		child.Child("virtual table")
	}

	var buf bytes.Buffer
	for i := 0; i < tab.ColumnCount(); i++ {
		buf.Reset()
		formatColumn(tab.Column(i), &buf)
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

	for i := 0; i < tab.UniqueCount(); i++ {
		var withoutIndexStr string
		uniq := tab.Unique(i)
		if uniq.WithoutIndex() {
			withoutIndexStr = "WITHOUT INDEX "
		}
		c := child.Childf(
			"UNIQUE %s%s",
			withoutIndexStr,
			formatCols(tab, tab.Unique(i).ColumnCount(), tab.Unique(i).ColumnOrdinal),
		)
		if pred, isPartial := uniq.Predicate(); isPartial {
			c.Childf("WHERE %s", pred)
		}
	}

	// TODO(radu): show stats.
}

// formatCatalogIndex nicely formats a catalog index using a treeprinter for
// debugging and testing.
func formatCatalogIndex(tab Table, ord int, tp treeprinter.Node) {
	idx := tab.Index(ord)
	idxType := ""
	if idx.Ordinal() == PrimaryIndex {
		idxType = "PRIMARY "
	} else if idx.IsUnique() {
		idxType = "UNIQUE "
	} else if idx.IsInverted() {
		idxType = "INVERTED "
	}
	mutation := ""
	if IsMutationIndex(tab, ord) {
		mutation = " (mutation)"
	}
	child := tp.Childf("%sINDEX %s%s", idxType, idx.Name(), mutation)

	var buf bytes.Buffer
	colCount := idx.ColumnCount()
	if ord == PrimaryIndex {
		// Omit the "stored" columns from the primary index.
		colCount = idx.KeyColumnCount()
	}

	for i := 0; i < colCount; i++ {
		buf.Reset()

		idxCol := idx.Column(i)
		formatColumn(idxCol.Column, &buf)
		if idxCol.Descending {
			fmt.Fprintf(&buf, " desc")
		}

		if i >= idx.LaxKeyColumnCount() {
			fmt.Fprintf(&buf, " (storing)")
		}

		if i < idx.ImplicitColumnCount() {
			fmt.Fprintf(&buf, " (implicit)")
		}

		child.Child(buf.String())
	}

	FormatZone(idx.Zone(), child)

	if n := idx.PartitionCount(); n > 0 {
		c := child.Child("partitions")
		for i := 0; i < n; i++ {
			p := idx.Partition(i)
			part := c.Child(p.Name())
			prefixes := part.Child("partition by list prefixes")
			for _, datums := range p.PartitionByListPrefixes() {
				prefixes.Child(datums.String())
			}
			FormatZone(p.Zone(), part)
		}
	}
	if pred, isPartial := idx.Predicate(); isPartial {
		child.Childf("WHERE %s", pred)
	}
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
	catalog Catalog, inbound bool, fkRef ForeignKeyConstraint, tp treeprinter.Node,
) {
	originDS, _, err := catalog.ResolveDataSourceByID(context.TODO(), Flags{}, fkRef.OriginTableID())
	if err != nil {
		panic(err)
	}
	refDS, _, err := catalog.ResolveDataSourceByID(context.TODO(), Flags{}, fkRef.ReferencedTableID())
	if err != nil {
		panic(err)
	}
	title := "CONSTRAINT"
	if inbound {
		title = "REFERENCED BY " + title
	}
	var extra bytes.Buffer
	if fkRef.MatchMethod() != tree.MatchSimple {
		fmt.Fprintf(&extra, " %s", fkRef.MatchMethod())
	}

	if action := fkRef.DeleteReferenceAction(); action != tree.NoAction {
		fmt.Fprintf(&extra, " ON DELETE %s", action.String())
	}

	tp.Childf(
		"%s %s FOREIGN KEY %v %s REFERENCES %v %s%s",
		title,
		fkRef.Name(),
		originDS.Name(),
		formatCols(originDS.(Table), fkRef.ColumnCount(), fkRef.OriginColumnOrdinal),
		refDS.Name(),
		formatCols(refDS.(Table), fkRef.ColumnCount(), fkRef.ReferencedColumnOrdinal),
		extra.String(),
	)
}

func formatColumn(col *Column, buf *bytes.Buffer) {
	fmt.Fprintf(buf, "%s %s", col.ColName(), col.DatumType())
	if !col.IsNullable() {
		fmt.Fprintf(buf, " not null")
	}
	if col.IsComputed() {
		if col.IsVirtualComputed() {
			fmt.Fprintf(buf, " as (%s) virtual", col.ComputedExprStr())
		} else {
			fmt.Fprintf(buf, " as (%s) stored", col.ComputedExprStr())
		}
	}
	if col.HasDefault() {
		generatedAsIdentityType := col.GeneratedAsIdentityType()
		if generatedAsIdentityType == NotGeneratedAsIdentity {
			fmt.Fprintf(buf, " default (%s)", col.DefaultExprStr())
		} else {
			switch generatedAsIdentityType {
			case GeneratedAlwaysAsIdentity:
				fmt.Fprintf(buf, " generated always as identity")
			case GeneratedByDefaultAsIdentity:
				fmt.Fprintf(buf, " generated by default as identity")
			}
			if col.HasGeneratedAsIdentitySequenceOption() {
				fmt.Fprintf(buf, " (%s)", col.GeneratedAsIdentitySequenceOption())
			}
		}
	}
	if col.HasOnUpdate() {
		fmt.Fprintf(buf, " on update (%s)", col.OnUpdateExprStr())
	}

	kind := col.Kind()
	// Omit the visibility for mutation and inverted columns, which are always
	// inaccessible.
	if kind != WriteOnly && kind != DeleteOnly && kind != Inverted {
		switch col.Visibility() {
		case Hidden:
			fmt.Fprintf(buf, " [hidden]")
		case Inaccessible:
			fmt.Fprintf(buf, " [inaccessible]")
		}
	}

	switch kind {
	case WriteOnly:
		fmt.Fprintf(buf, " [write-only]")

	case DeleteOnly:
		fmt.Fprintf(buf, " [delete-only]")

	case System:
		fmt.Fprintf(buf, " [system]")

	case Inverted:
		fmt.Fprintf(buf, " [inverted]")
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
