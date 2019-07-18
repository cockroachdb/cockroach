// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

// ShowCreateView returns a valid SQL representation of the CREATE VIEW
// statement used to create the given view. It is used in the implementation of
// the crdb_internal.create_statements virtual table.
func ShowCreateView(
	ctx context.Context, tn *tree.Name, desc *sqlbase.TableDescriptor,
) (string, error) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteString("CREATE VIEW ")
	f.FormatNode(tn)
	f.WriteString(" (")
	for i := range desc.Columns {
		if i > 0 {
			f.WriteString(", ")
		}
		f.FormatNameP(&desc.Columns[i].Name)
	}
	f.WriteString(") AS ")
	f.WriteString(desc.ViewQuery)
	return f.CloseAndGetString(), nil
}

func printForeignKeyConstraint(
	ctx context.Context,
	buf *bytes.Buffer,
	dbPrefix string,
	originTable *sqlbase.TableDescriptor,
	fk *sqlbase.ForeignKeyConstraint,
	lCtx *internalLookupCtx,
) error {
	var refNames []string
	var originNames []string
	var fkTableName tree.TableName
	if lCtx != nil {
		fkTable, err := lCtx.getTableByID(fk.ReferencedTableID)
		if err != nil {
			return err
		}
		fkDb, err := lCtx.getDatabaseByID(fkTable.ParentID)
		if err != nil {
			return err
		}
		refNames, err = fkTable.NamesForColumnIDs(fk.ReferencedColumnIDs)
		if err != nil {
			return err
		}
		fkTableName = tree.MakeTableName(tree.Name(fkDb.Name), tree.Name(fkTable.Name))
		fkTableName.ExplicitSchema = fkDb.Name != dbPrefix
		originNames, err = originTable.NamesForColumnIDs(fk.OriginColumnIDs)
		if err != nil {
			return err
		}
	} else {
		refNames = []string{"???"}
		originNames = []string{"???"}
		fkTableName = tree.MakeTableName(tree.Name(""), tree.Name(fmt.Sprintf("[%d as ref]", fk.ReferencedTableID)))
		fkTableName.ExplicitSchema = false
	}
	buf.WriteString("FOREIGN KEY (")
	formatQuoteNames(buf, originNames...)
	buf.WriteString(") REFERENCES ")
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	fmtCtx.FormatNode(&fkTableName)
	buf.WriteString(fmtCtx.CloseAndGetString())
	buf.WriteString("(")
	formatQuoteNames(buf, refNames...)
	buf.WriteByte(')')
	// We omit MATCH SIMPLE because it is the default.
	if fk.Match != sqlbase.ForeignKeyReference_SIMPLE {
		buf.WriteByte(' ')
		buf.WriteString(fk.Match.String())
	}
	if fk.OnDelete != sqlbase.ForeignKeyReference_NO_ACTION {
		buf.WriteString(" ON DELETE ")
		buf.WriteString(fk.OnDelete.String())
	}
	if fk.OnUpdate != sqlbase.ForeignKeyReference_NO_ACTION {
		buf.WriteString(" ON UPDATE ")
		buf.WriteString(fk.OnUpdate.String())
	}
	return nil
}

// ShowCreateSequence returns a valid SQL representation of the
// CREATE SEQUENCE statement used to create the given sequence.
func ShowCreateSequence(
	ctx context.Context, tn *tree.Name, desc *sqlbase.TableDescriptor,
) (string, error) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteString("CREATE SEQUENCE ")
	f.FormatNode(tn)
	opts := desc.SequenceOpts
	f.Printf(" MINVALUE %d", opts.MinValue)
	f.Printf(" MAXVALUE %d", opts.MaxValue)
	f.Printf(" INCREMENT %d", opts.Increment)
	f.Printf(" START %d", opts.Start)
	if opts.Virtual {
		f.Printf(" VIRTUAL")
	}
	return f.CloseAndGetString(), nil
}

// ShowCreateTable returns a valid SQL representation of the CREATE
// TABLE statement used to create the given table.
//
// The names of the tables references by foreign keys, and the
// interleaved parent if any, are prefixed by their own database name
// unless it is equal to the given dbPrefix. This allows us to elide
// the prefix when the given table references other tables in the
// current database.
func ShowCreateTable(
	ctx context.Context,
	tn *tree.Name,
	dbPrefix string,
	desc *sqlbase.TableDescriptor,
	lCtx *internalLookupCtx,
	ignoreFKs bool,
) (string, error) {
	a := &sqlbase.DatumAlloc{}

	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteString("CREATE TABLE ")
	f.FormatNode(tn)
	f.WriteString(" (")
	primaryKeyIsOnVisibleColumn := false
	visibleCols := desc.VisibleColumns()
	for i := range visibleCols {
		col := &visibleCols[i]
		if i != 0 {
			f.WriteString(",")
		}
		f.WriteString("\n\t")
		f.WriteString(col.SQLString())
		if desc.IsPhysicalTable() && desc.PrimaryIndex.ColumnIDs[0] == col.ID {
			// Only set primaryKeyIsOnVisibleColumn to true if the primary key
			// is on a visible column (not rowid).
			primaryKeyIsOnVisibleColumn = true
		}
	}
	if primaryKeyIsOnVisibleColumn {
		f.WriteString(",\n\tCONSTRAINT ")
		formatQuoteNames(&f.Buffer, desc.PrimaryIndex.Name)
		f.WriteString(" ")
		f.WriteString(desc.PrimaryKeyString())
	}
	if !ignoreFKs {
		for _, fk := range desc.OutboundFKs {
			f.WriteString(",\n\tCONSTRAINT ")
			f.FormatNameP(&fk.Name)
			f.WriteString(" ")
			if err := printForeignKeyConstraint(ctx, &f.Buffer, dbPrefix, desc, fk, lCtx); err != nil {
				return "", err
			}
		}
	}
	allIdx := append(desc.Indexes, desc.PrimaryIndex)
	for i := range allIdx {
		idx := &allIdx[i]
		if idx.ID != desc.PrimaryIndex.ID {
			// Showing the primary index is handled above.
			f.WriteString(",\n\t")
			f.WriteString(idx.SQLString(&sqlbase.AnonymousTable))
			// Showing the INTERLEAVE and PARTITION BY for the primary index are
			// handled last.
			if err := showCreateInterleave(ctx, idx, &f.Buffer, dbPrefix, lCtx); err != nil {
				return "", err
			}
			if err := ShowCreatePartitioning(
				a, desc, idx, &idx.Partitioning, &f.Buffer, 1 /* indent */, 0, /* colOffset */
			); err != nil {
				return "", err
			}
		}
	}

	for _, fam := range desc.Families {
		activeColumnNames := make([]string, 0, len(fam.ColumnNames))
		for i, colID := range fam.ColumnIDs {
			if _, err := desc.FindActiveColumnByID(colID); err == nil {
				activeColumnNames = append(activeColumnNames, fam.ColumnNames[i])
			}
		}
		f.WriteString(",\n\tFAMILY ")
		formatQuoteNames(&f.Buffer, fam.Name)
		f.WriteString(" (")
		formatQuoteNames(&f.Buffer, activeColumnNames...)
		f.WriteString(")")
	}

	// TODO (lucy): probably go back to using desc.Checks for consistency
	for _, e := range desc.AllActiveAndInactiveChecks() {
		f.WriteString(",\n\t")
		if len(e.Name) > 0 {
			f.WriteString("CONSTRAINT ")
			formatQuoteNames(&f.Buffer, e.Name)
			f.WriteString(" ")
		}
		f.WriteString("CHECK (")
		f.WriteString(e.Expr)
		f.WriteString(")")
	}

	f.WriteString("\n)")

	if err := showCreateInterleave(ctx, &desc.PrimaryIndex, &f.Buffer, dbPrefix, lCtx); err != nil {
		return "", err
	}
	if err := ShowCreatePartitioning(
		a, desc, &desc.PrimaryIndex, &desc.PrimaryIndex.Partitioning, &f.Buffer, 0 /* indent */, 0, /* colOffset */
	); err != nil {
		return "", err
	}

	return f.CloseAndGetString(), nil
}

// formatQuoteNames quotes and adds commas between names.
func formatQuoteNames(buf *bytes.Buffer, names ...string) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	for i := range names {
		if i > 0 {
			f.WriteString(", ")
		}
		f.FormatNameP(&names[i])
	}
	buf.WriteString(f.CloseAndGetString())
}

// showCreateInterleave returns an INTERLEAVE IN PARENT clause for the specified
// index, if applicable.
//
// The name of the parent table is prefixed by its database name unless
// it is equal to the given dbPrefix. This allows us to elide the prefix
// when the given index is interleaved in a table of the current database.
func showCreateInterleave(
	ctx context.Context,
	idx *sqlbase.IndexDescriptor,
	buf *bytes.Buffer,
	dbPrefix string,
	lCtx *internalLookupCtx,
) error {
	if len(idx.Interleave.Ancestors) == 0 {
		return nil
	}
	intl := idx.Interleave
	parentTableID := intl.Ancestors[len(intl.Ancestors)-1].TableID

	var parentName tree.TableName
	if lCtx != nil {
		parentTable, err := lCtx.getTableByID(parentTableID)
		if err != nil {
			return err
		}
		parentDbDesc, err := lCtx.getDatabaseByID(parentTable.ParentID)
		if err != nil {
			return err
		}
		parentName = tree.MakeTableName(tree.Name(parentDbDesc.Name), tree.Name(parentTable.Name))
		parentName.ExplicitSchema = parentDbDesc.Name != dbPrefix
	} else {
		parentName = tree.MakeTableName(tree.Name(""), tree.Name(fmt.Sprintf("[%d as parent]", parentTableID)))
		parentName.ExplicitCatalog = false
		parentName.ExplicitSchema = false
	}
	var sharedPrefixLen int
	for _, ancestor := range intl.Ancestors {
		sharedPrefixLen += int(ancestor.SharedPrefixLen)
	}
	buf.WriteString(" INTERLEAVE IN PARENT ")
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	fmtCtx.FormatNode(&parentName)
	buf.WriteString(fmtCtx.CloseAndGetString())
	buf.WriteString(" (")
	formatQuoteNames(buf, idx.ColumnNames[:sharedPrefixLen]...)
	buf.WriteString(")")
	return nil
}

// ShowCreatePartitioning returns a PARTITION BY clause for the specified
// index, if applicable.
func ShowCreatePartitioning(
	a *sqlbase.DatumAlloc,
	tableDesc *sqlbase.TableDescriptor,
	idxDesc *sqlbase.IndexDescriptor,
	partDesc *sqlbase.PartitioningDescriptor,
	buf *bytes.Buffer,
	indent int,
	colOffset int,
) error {
	if partDesc.NumColumns == 0 {
		return nil
	}

	// We don't need real prefixes in the DecodePartitionTuple calls because we
	// only use the tree.Datums part of the output.
	fakePrefixDatums := make([]tree.Datum, colOffset)
	for i := range fakePrefixDatums {
		fakePrefixDatums[i] = tree.DNull
	}

	indentStr := strings.Repeat("\t", indent)
	buf.WriteString(` PARTITION BY `)
	if len(partDesc.List) > 0 {
		buf.WriteString(`LIST`)
	} else if len(partDesc.Range) > 0 {
		buf.WriteString(`RANGE`)
	} else {
		return errors.Errorf(`invalid partition descriptor: %v`, partDesc)
	}
	buf.WriteString(` (`)
	for i := 0; i < int(partDesc.NumColumns); i++ {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(idxDesc.ColumnNames[colOffset+i])
	}
	buf.WriteString(`) (`)
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	for i := range partDesc.List {
		part := &partDesc.List[i]
		if i != 0 {
			buf.WriteString(`, `)
		}
		buf.WriteString("\n")
		buf.WriteString(indentStr)
		buf.WriteString("\tPARTITION ")
		fmtCtx.FormatNameP(&part.Name)
		_, _ = fmtCtx.Buffer.WriteTo(buf)
		buf.WriteString(` VALUES IN (`)
		for j, values := range part.Values {
			if j != 0 {
				buf.WriteString(`, `)
			}
			tuple, _, err := sqlbase.DecodePartitionTuple(
				a, tableDesc, idxDesc, partDesc, values, fakePrefixDatums)
			if err != nil {
				return err
			}
			buf.WriteString(tuple.String())
		}
		buf.WriteString(`)`)
		if err := ShowCreatePartitioning(
			a, tableDesc, idxDesc, &part.Subpartitioning, buf, indent+1,
			colOffset+int(partDesc.NumColumns),
		); err != nil {
			return err
		}
	}
	for i, part := range partDesc.Range {
		if i != 0 {
			buf.WriteString(`, `)
		}
		buf.WriteString("\n")
		buf.WriteString(indentStr)
		buf.WriteString("\tPARTITION ")
		buf.WriteString(part.Name)
		buf.WriteString(" VALUES FROM ")
		fromTuple, _, err := sqlbase.DecodePartitionTuple(
			a, tableDesc, idxDesc, partDesc, part.FromInclusive, fakePrefixDatums)
		if err != nil {
			return err
		}
		buf.WriteString(fromTuple.String())
		buf.WriteString(" TO ")
		toTuple, _, err := sqlbase.DecodePartitionTuple(
			a, tableDesc, idxDesc, partDesc, part.ToExclusive, fakePrefixDatums)
		if err != nil {
			return err
		}
		buf.WriteString(toTuple.String())
	}
	buf.WriteString("\n")
	buf.WriteString(indentStr)
	buf.WriteString(")")
	return nil
}
