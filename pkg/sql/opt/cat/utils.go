// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cat

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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
	return catalog.ResolveIndex(ctx, flags, name)
}

// FormatTable nicely formats a catalog table using a treeprinter for debugging
// and testing. With redactableValues set to true, all user-supplied constants
// and literals (e.g. DEFAULT values, constants in generated column expressions,
// etc.) are surrounded by redaction markers.
func FormatTable(
	ctx context.Context, cat Catalog, tab Table, tp treeprinter.Node, redactableValues bool,
) {
	child := tp.Childf("TABLE %s", tab.Name())
	if tab.IsVirtualTable() {
		child.Child("virtual table")
	}

	var buf bytes.Buffer
	for i := 0; i < tab.ColumnCount(); i++ {
		buf.Reset()
		formatColumn(tab.Column(i), &buf, redactableValues)
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
		child.Childf("CHECK (%s)", MaybeMarkRedactable(tab.Check(i).Constraint(), redactableValues))
	}

	for i := 0; i < tab.DeletableIndexCount(); i++ {
		formatCatalogIndex(tab, i, child, redactableValues)
	}

	for i := 0; i < tab.OutboundForeignKeyCount(); i++ {
		formatCatalogFKRef(ctx, cat, false /* inbound */, tab.OutboundForeignKey(i), child)
	}

	for i := 0; i < tab.InboundForeignKeyCount(); i++ {
		formatCatalogFKRef(ctx, cat, true /* inbound */, tab.InboundForeignKey(i), child)
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
			c.Childf("WHERE %s", MaybeMarkRedactable(pred, redactableValues))
		}
	}

	// TODO(radu): show stats.
}

// formatCatalogIndex nicely formats a catalog index using a treeprinter for
// debugging and testing.
func formatCatalogIndex(tab Table, ord int, tp treeprinter.Node, redactableValues bool) {
	index := tab.Index(ord)
	indexType := ""
	if index.Ordinal() == PrimaryIndex {
		indexType = "PRIMARY "
	} else if index.IsUnique() {
		indexType = "UNIQUE "
	} else {
		switch index.Type() {
		case idxtype.INVERTED:
			indexType = "INVERTED "
		case idxtype.VECTOR:
			indexType = "VECTOR "
		}
	}
	mutation := ""
	if IsMutationIndex(tab, ord) {
		mutation = " (mutation)"
	}

	idxVisibililty := ""
	if invisibility := index.GetInvisibility(); invisibility != 0.0 {
		if invisibility == 1.0 {
			idxVisibililty = " NOT VISIBLE"
		} else {
			idxVisibililty = " VISIBILITY " + fmt.Sprintf("%.2f", 1-invisibility)
		}
	}

	child := tp.Childf("%sINDEX %s%s%s", indexType, index.Name(), mutation, idxVisibililty)

	var buf bytes.Buffer
	colCount := index.ColumnCount()
	if ord == PrimaryIndex {
		// Omit the "stored" columns from the primary index.
		colCount = index.KeyColumnCount()
	}

	for i := 0; i < colCount; i++ {
		buf.Reset()

		idxCol := index.Column(i)
		formatColumn(idxCol.Column, &buf, redactableValues)
		if idxCol.Descending {
			fmt.Fprintf(&buf, " desc")
		}

		if i >= index.LaxKeyColumnCount() {
			fmt.Fprintf(&buf, " (storing)")
		}

		if i < index.ImplicitColumnCount() {
			fmt.Fprintf(&buf, " (implicit)")
		}

		child.Child(buf.String())
	}

	FormatZone(index.Zone(), child)

	if n := index.PartitionCount(); n > 0 {
		c := child.Child("partitions")
		for i := 0; i < n; i++ {
			p := index.Partition(i)
			part := c.Child(p.Name())
			prefixes := part.Child("partition by list prefixes")
			for _, datums := range p.PartitionByListPrefixes() {
				prefixes.Child(MaybeMarkRedactable(datums.String(), redactableValues))
			}
			FormatZone(p.Zone(), part)
		}
	}
	if pred, isPartial := index.Predicate(); isPartial {
		child.Childf("WHERE %s", MaybeMarkRedactable(pred, redactableValues))
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
	ctx context.Context,
	catalog Catalog,
	inbound bool,
	fkRef ForeignKeyConstraint,
	tp treeprinter.Node,
) {
	originDS, _, err := catalog.ResolveDataSourceByID(ctx, Flags{}, fkRef.OriginTableID())
	if err != nil {
		panic(err)
	}
	refDS, _, err := catalog.ResolveDataSourceByID(ctx, Flags{}, fkRef.ReferencedTableID())
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

func formatColumn(col *Column, buf *bytes.Buffer, redactableValues bool) {
	fmt.Fprintf(buf, "%s %s", col.ColName(), col.DatumType())
	if !col.IsNullable() {
		fmt.Fprintf(buf, " not null")
	}
	if col.IsComputed() {
		exprStr := MaybeMarkRedactable(col.ComputedExprStr(), redactableValues)
		if col.IsVirtualComputed() {
			fmt.Fprintf(buf, " as (%s) virtual", exprStr)
		} else {
			fmt.Fprintf(buf, " as (%s) stored", exprStr)
		}
	}
	if col.HasDefault() {
		generatedAsIdentityType := col.GeneratedAsIdentityType()
		if generatedAsIdentityType == NotGeneratedAsIdentity {
			fmt.Fprintf(buf, " default (%s)", MaybeMarkRedactable(col.DefaultExprStr(), redactableValues))
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
		fmt.Fprintf(
			buf, " on update (%s)", MaybeMarkRedactable(col.OnUpdateExprStr(), redactableValues),
		)
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

// MaybeMarkRedactable surrounds an unsafe string with redaction markers if
// markRedactable is true.
func MaybeMarkRedactable(unsafe string, markRedactable bool) string {
	if markRedactable {
		return string(redact.Sprintf("%s", encoding.Unsafe(unsafe)))
	}
	return unsafe
}
