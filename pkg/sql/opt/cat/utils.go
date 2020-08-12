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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
	"github.com/cockroachdb/errors"
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
		schema, _, err := catalog.ResolveSchema(ctx, flags, &p.ObjectNamePrefix)
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
		return nil, DataSourceName{}, pgerror.Newf(
			pgcode.UndefinedObject, "index %q does not exist", name.Index,
		)
	}

	// We have to search for a table that has an index with the given name.
	schema, _, err := catalog.ResolveSchema(ctx, flags, &name.Table.ObjectNamePrefix)
	if err != nil {
		return nil, DataSourceName{}, err
	}
	dsNames, err := schema.GetDataSourceNames(ctx)
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
		formatColumn(tab.Column(i), tab.ColumnKind(i), &buf)
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
		formatColumn(idxCol.Column, tab.ColumnKind(i), &buf)
		if idxCol.Descending {
			fmt.Fprintf(&buf, " desc")
		}

		if i >= idx.LaxKeyColumnCount() {
			fmt.Fprintf(&buf, " (storing)")
		}

		child.Child(buf.String())
	}

	FormatZone(idx.Zone(), child)

	partPrefixes := idx.PartitionByListPrefixes()
	if len(partPrefixes) != 0 {
		c := child.Child("partition by list prefixes")
		for i := range partPrefixes {
			c.Child(partPrefixes[i].String())
		}
	}
	if n := idx.InterleaveAncestorCount(); n > 0 {
		c := child.Child("interleave ancestors")
		for i := 0; i < n; i++ {
			table, index, numKeyCols := idx.InterleaveAncestor(i)
			c.Childf(
				"table=%d index=%d (%d key column%s)",
				table, index, numKeyCols, util.Pluralize(int64(numKeyCols)),
			)
		}
	}
	if n := idx.InterleavedByCount(); n > 0 {
		c := child.Child("interleaved by")
		for i := 0; i < n; i++ {
			table, index := idx.InterleavedBy(i)
			c.Childf("table=%d index=%d", table, index)
		}
	}
	if pred, isPartial := idx.Predicate(); isPartial {
		c := child.Child("WHERE")
		c.Childf(pred)
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

func formatColumn(col Column, kind ColumnKind, buf *bytes.Buffer) {
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
	switch kind {
	case WriteOnly, DeleteOnly:
		fmt.Fprintf(buf, " [mutation]")
	case System:
		fmt.Fprintf(buf, " [system]")
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

// InterleaveAncestorDescendant returns ok=true if a and b are interleaved
// indexes and one of them is the ancestor of the other.
// If a is an ancestor of b, aIsAncestor is true.
// If b is an ancestor of a, aIsAncestor is false.
func InterleaveAncestorDescendant(a, b Index) (ok bool, aIsAncestor bool) {
	aCount := a.InterleaveAncestorCount()
	bCount := b.InterleaveAncestorCount()
	// If a is the ancestor of b; then:
	//  - a has a smaller ancestor count;
	//  - a's ancestors are a prefix of b's ancestors;
	//  - a shows up in b's ancestor list on the position that would allow them to
	//    share a's ancestors.
	//
	// For example:
	//    x
	//    |
	//    y
	//    |
	//    a  (ancestors: x, y)
	//    |
	//    z
	//    |
	//    b  (ancestors: x, y, a, z)
	if aCount < bCount {
		tabID, idxID, _ := b.InterleaveAncestor(aCount)
		if tabID == a.Table().ID() && idxID == a.ID() {
			return true, true // a is the ancestor
		}
	} else if bCount < aCount {
		tabID, idxID, _ := a.InterleaveAncestor(bCount)
		if tabID == b.Table().ID() && idxID == b.ID() {
			return true, false // a is not the ancestor
		}
	}

	return false, false
}
