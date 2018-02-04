// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"bytes"
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

// ShowCreateTable returns a SHOW CREATE TABLE statement for the specified table.
// Privileges: Any privilege on table.
func (p *planner) ShowCreateTable(ctx context.Context, n *tree.ShowCreateTable) (planNode, error) {
	// We make the check whether the name points to a table or not in
	// SQL, so as to avoid a double lookup (a first one to check if the
	// descriptor is of the right type, another to populate the
	// create_statements vtable).
	// The condition "database_name IS NULL" ensures that virtual tables are included.
	const showCreateTableQuery = `
     SELECT %[3]s AS "Table",
            IFNULL(create_statement,
                   crdb_internal.force_error('` + pgerror.CodeUndefinedTableError + `',
                                             %[3]s || ' is not a table')::string
            ) AS "CreateTable"
       FROM (SELECT create_statement FROM %[4]s.crdb_internal.create_statements
              WHERE (database_name IS NULL OR database_name = %[1]s)
                AND schema_name = %[5]s
                AND descriptor_name = %[2]s
                AND descriptor_type = 'table'
              UNION ALL VALUES (NULL) ORDER BY 1 DESC) LIMIT 1
  `
	return p.showTableDetails(ctx, "SHOW CREATE TABLE", n.Table, showCreateTableQuery)
}

// ShowCreateView returns a CREATE VIEW statement for the specified view.
// Privileges: Any privilege on view.
func (p *planner) ShowCreateView(ctx context.Context, n *tree.ShowCreateView) (planNode, error) {
	// We make the check whether the name points to a view or not in
	// SQL, so as to avoid a double lookup (a first one to check if the
	// descriptor is of the right type, another to populate the
	// create_statements vtable).
	const showCreateViewQuery = `
     SELECT %[3]s AS "View",
            IFNULL(create_statement,
                   crdb_internal.force_error('` + pgerror.CodeUndefinedTableError + `',
                                             %[3]s || ' is not a view')::string
            ) AS "CreateView"
       FROM (SELECT create_statement FROM %[4]s.crdb_internal.create_statements
              WHERE database_name = %[1]s AND schema_name = %[5]s AND descriptor_name = %[2]s AND descriptor_type = 'view'
              UNION ALL VALUES (NULL) ORDER BY 1 DESC) LIMIT 1
  `
	return p.showTableDetails(ctx, "SHOW CREATE VIEW", n.View, showCreateViewQuery)
}

func (p *planner) ShowCreateSequence(
	ctx context.Context, n *tree.ShowCreateSequence,
) (planNode, error) {
	// We make the check whether the name points to a sequence or not in
	// SQL, so as to avoid a double lookup (a first one to check if the
	// descriptor is of the right type, another to populate the
	// create_statements vtable).
	const showCreateSequenceQuery = `
			SELECT %[3]s AS "Sequence",
							IFNULL(create_statement,
										 crdb_internal.force_error('` + pgerror.CodeUndefinedTableError + `',
                                              %[3]s || ' is not a sequence')::string
							) AS "CreateSequence"
				 FROM (SELECT create_statement FROM %[4]s.crdb_internal.create_statements
								WHERE database_name = %[1]s AND schema_name = %[5]s AND descriptor_name = %[2]s
								AND descriptor_type = 'sequence'
								UNION ALL VALUES (NULL) ORDER BY 1 DESC) LIMIT 1
	`
	return p.showTableDetails(ctx, "SHOW CREATE SEQUENCE", n.Sequence, showCreateSequenceQuery)
}

// showCreateView returns a valid SQL representation of the CREATE
// VIEW statement used to create the given view.
func (p *planner) showCreateView(
	ctx context.Context, tn *tree.Name, desc *sqlbase.TableDescriptor,
) (string, error) {
	f := tree.NewFmtCtxWithBuf(tree.FmtSimple)
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

func (p *planner) printForeignKeyConstraint(
	ctx context.Context,
	buf *bytes.Buffer,
	dbPrefix string,
	idx *sqlbase.IndexDescriptor,
	lCtx *internalLookupCtx,
) error {
	fk := &idx.ForeignKey
	if !fk.IsSet() {
		return nil
	}
	fkTable, err := lCtx.getTableByID(fk.Table)
	if err != nil {
		return err
	}
	fkDb, err := lCtx.getDatabaseByID(fkTable.ParentID)
	if err != nil {
		return err
	}
	fkIdx, err := fkTable.FindIndexByID(fk.Index)
	if err != nil {
		return err
	}
	fkTableName := tree.MakeTableName(tree.Name(fkDb.Name), tree.Name(fkTable.Name))
	fkTableName.ExplicitSchema = fkDb.Name != dbPrefix
	fmtCtx := tree.MakeFmtCtx(buf, tree.FmtSimple)
	buf.WriteString("FOREIGN KEY (")
	formatQuoteNames(buf, idx.ColumnNames[0:idx.ForeignKey.SharedPrefixLen]...)
	buf.WriteString(") REFERENCES ")
	fmtCtx.FormatNode(&fkTableName)
	buf.WriteString(" (")
	formatQuoteNames(buf, fkIdx.ColumnNames...)
	buf.WriteByte(')')
	idx.ColNamesString()
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

// showCreateSequence returns a valid SQL representation of the
// CREATE SEQUENCE statement used to create the given sequence.
func (p *planner) showCreateSequence(
	ctx context.Context, tn *tree.Name, desc *sqlbase.TableDescriptor,
) (string, error) {
	f := tree.NewFmtCtxWithBuf(tree.FmtSimple)
	f.WriteString("CREATE SEQUENCE ")
	f.FormatNode(tn)
	opts := desc.SequenceOpts
	f.Printf(" MINVALUE %d", opts.MinValue)
	f.Printf(" MAXVALUE %d", opts.MaxValue)
	f.Printf(" INCREMENT %d", opts.Increment)
	f.Printf(" START %d", opts.Start)
	return f.CloseAndGetString(), nil
}

// showCreateTable returns a valid SQL representation of the CREATE
// TABLE statement used to create the given table.
//
// The names of the tables references by foreign keys, and the
// interleaved parent if any, are prefixed by their own database name
// unless it is equal to the given dbPrefix. This allows us to elide
// the prefix when the given table references other tables in the
// current database.
func (p *planner) showCreateTable(
	ctx context.Context,
	tn *tree.Name,
	dbPrefix string,
	desc *sqlbase.TableDescriptor,
	lCtx *internalLookupCtx,
) (string, error) {
	a := &sqlbase.DatumAlloc{}

	f := tree.NewFmtCtxWithBuf(tree.FmtSimple)
	f.WriteString("CREATE TABLE ")
	f.FormatNode(tn)
	f.WriteString(" (")
	primaryKeyIsOnVisibleColumn := false
	for i, col := range desc.VisibleColumns() {
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
		formatQuoteNames(f.Buffer, desc.PrimaryIndex.Name)
		f.WriteString(" ")
		f.WriteString(desc.PrimaryKeyString())
	}
	allIdx := append(desc.Indexes, desc.PrimaryIndex)
	for i := range allIdx {
		idx := &allIdx[i]
		if fk := &idx.ForeignKey; fk.IsSet() {
			f.WriteString(",\n\tCONSTRAINT ")
			f.FormatNameP(&fk.Name)
			f.WriteString(" ")
			if err := p.printForeignKeyConstraint(ctx, f.Buffer, dbPrefix, idx, lCtx); err != nil {
				return "", err
			}
		}
		if idx.ID != desc.PrimaryIndex.ID {
			// Showing the primary index is handled above.
			f.WriteString(",\n\t")
			f.WriteString(idx.SQLString(""))
			// Showing the INTERLEAVE and PARTITION BY for the primary index are
			// handled last.
			if err := p.showCreateInterleave(ctx, idx, f.Buffer, dbPrefix, lCtx); err != nil {
				return "", err
			}
			if err := ShowCreatePartitioning(
				a, desc, idx, &idx.Partitioning, f.Buffer, 1 /* indent */, 0, /* colOffset */
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
		formatQuoteNames(f.Buffer, fam.Name)
		f.WriteString(" (")
		formatQuoteNames(f.Buffer, activeColumnNames...)
		f.WriteString(")")
	}

	for _, e := range desc.Checks {
		f.WriteString(",\n\t")
		if len(e.Name) > 0 {
			f.WriteString("CONSTRAINT ")
			formatQuoteNames(f.Buffer, e.Name)
			f.WriteString(" ")
		}
		f.WriteString("CHECK (")
		f.WriteString(e.Expr)
		f.WriteString(")")
	}

	f.WriteString("\n)")

	if err := p.showCreateInterleave(ctx, &desc.PrimaryIndex, f.Buffer, dbPrefix, lCtx); err != nil {
		return "", err
	}
	if err := ShowCreatePartitioning(
		a, desc, &desc.PrimaryIndex, &desc.PrimaryIndex.Partitioning, f.Buffer, 0 /* indent */, 0, /* colOffset */
	); err != nil {
		return "", err
	}

	return f.CloseAndGetString(), nil
}

// formatQuoteNames quotes and adds commas between names.
func formatQuoteNames(buf *bytes.Buffer, names ...string) {
	f := tree.MakeFmtCtx(buf, tree.FmtSimple)
	for i := range names {
		if i > 0 {
			f.WriteString(", ")
		}
		f.FormatNameP(&names[i])
	}
}

// showCreateInterleave returns an INTERLEAVE IN PARENT clause for the specified
// index, if applicable.
//
// The name of the parent table is prefixed by its database name unless
// it is equal to the given dbPrefix. This allows us to elide the prefix
// when the given index is interleaved in a table of the current database.
func (p *planner) showCreateInterleave(
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
	parentTable, err := lCtx.getTableByID(intl.Ancestors[len(intl.Ancestors)-1].TableID)
	if err != nil {
		return err
	}
	parentDbDesc, err := lCtx.getDatabaseByID(parentTable.ParentID)
	if err != nil {
		return err
	}
	parentName := tree.MakeTableName(tree.Name(parentDbDesc.Name), tree.Name(parentTable.Name))
	parentName.ExplicitSchema = parentDbDesc.Name != dbPrefix
	var sharedPrefixLen int
	for _, ancestor := range intl.Ancestors {
		sharedPrefixLen += int(ancestor.SharedPrefixLen)
	}
	fmtCtx := tree.MakeFmtCtx(buf, tree.FmtSimple)
	buf.WriteString(" INTERLEAVE IN PARENT ")
	fmtCtx.FormatNode(&parentName)
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
	fmtCtx := tree.MakeFmtCtx(buf, tree.FmtSimple)
	for i := range partDesc.List {
		part := &partDesc.List[i]
		if i != 0 {
			buf.WriteString(`, `)
		}
		buf.WriteString("\n")
		buf.WriteString(indentStr)
		buf.WriteString("\tPARTITION ")
		fmtCtx.FormatNameP(&part.Name)
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
