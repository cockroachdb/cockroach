// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// tableComments stores the comment data for a table.
type tableComments struct {
	comment *string
	columns []comment
	indexes []comment
}

type comment struct {
	subID   int
	comment string
}

// selectComment retrieves all the comments pertaining to a table (comments on the table
// itself but also column and index comments.)
func selectComment(ctx context.Context, p PlanHookState, tableID sqlbase.ID) (tc *tableComments) {
	query := fmt.Sprintf("SELECT type, object_id, sub_id, comment FROM system.comments WHERE object_id = %d", tableID)

	commentRows, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Query(
		ctx, "show-tables-with-comment", p.Txn(), query)
	if err != nil {
		log.VEventf(ctx, 1, "%q", err)
	} else {
		for _, row := range commentRows {
			commentType := int(tree.MustBeDInt(row[0]))
			switch commentType {
			case keys.TableCommentType, keys.ColumnCommentType, keys.IndexCommentType:
				subID := int(tree.MustBeDInt(row[2]))
				cmt := string(tree.MustBeDString(row[3]))

				if tc == nil {
					tc = &tableComments{}
				}

				switch commentType {
				case keys.TableCommentType:
					tc.comment = &cmt
				case keys.ColumnCommentType:
					tc.columns = append(tc.columns, comment{subID, cmt})
				case keys.IndexCommentType:
					tc.indexes = append(tc.indexes, comment{subID, cmt})
				}
			}
		}
	}

	return tc
}

// ShowCreateView returns a valid SQL representation of the CREATE VIEW
// statement used to create the given view. It is used in the implementation of
// the crdb_internal.create_statements virtual table.
func ShowCreateView(
	ctx context.Context, tn *tree.Name, desc *sqlbase.ImmutableTableDescriptor,
) (string, error) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteString("CREATE ")
	if desc.Temporary {
		f.WriteString("TEMP ")
	}
	f.WriteString("VIEW ")
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

// showComments prints out the COMMENT statements sufficient to populate a
// table's comments, including its index and column comments.
func showComments(
	table *sqlbase.ImmutableTableDescriptor, tc *tableComments, buf *bytes.Buffer,
) error {
	if tc == nil {
		return nil
	}

	if tc.comment != nil {
		buf.WriteString(";\n")
		buf.WriteString(fmt.Sprintf("COMMENT ON TABLE %s IS '%s'", table.Name, *tc.comment))
	}

	for _, columnComment := range tc.columns {
		col, err := table.FindColumnByID(sqlbase.ColumnID(columnComment.subID))
		if err != nil {
			return err
		}

		buf.WriteString(";\n")
		buf.WriteString(fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s'", table.Name, col.Name, columnComment.comment))
	}

	for _, indexComment := range tc.indexes {
		idx, err := table.FindIndexByID(sqlbase.IndexID(indexComment.subID))
		if err != nil {
			return err
		}

		buf.WriteString(";\n")
		buf.WriteString(fmt.Sprintf("COMMENT ON INDEX %s IS '%s'", idx.Name, indexComment.comment))
	}

	return nil
}

// showForeignKeyConstraint returns a valid SQL representation of a FOREIGN KEY
// clause for a given index.
func showForeignKeyConstraint(
	buf *bytes.Buffer,
	dbPrefix string,
	originTable *sqlbase.ImmutableTableDescriptor,
	fk *sqlbase.ForeignKeyConstraint,
	lCtx simpleSchemaResolver,
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
		fkTableName = tree.MakeTableName(tree.Name(fkDb.GetName()), tree.Name(fkTable.Name))
		fkTableName.ExplicitSchema = fkDb.GetName() != dbPrefix
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
	ctx context.Context, tn *tree.Name, desc *sqlbase.ImmutableTableDescriptor,
) (string, error) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteString("CREATE ")
	if desc.Temporary {
		f.WriteString("TEMP ")
	}
	f.WriteString("SEQUENCE ")
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

// showFamilyClause creates the FAMILY clauses for a CREATE statement, writing them
// to tree.FmtCtx f
func showFamilyClause(desc *sqlbase.ImmutableTableDescriptor, f *tree.FmtCtx) {
	for _, fam := range desc.Families {
		activeColumnNames := make([]string, 0, len(fam.ColumnNames))
		for i, colID := range fam.ColumnIDs {
			if _, err := desc.FindActiveColumnByID(colID); err == nil {
				activeColumnNames = append(activeColumnNames, fam.ColumnNames[i])
			}
		}
		if len(desc.VisibleColumns()) == 0 {
			f.WriteString("FAMILY ")
		} else {
			f.WriteString(",\n\tFAMILY ")
		}
		formatQuoteNames(&f.Buffer, fam.Name)
		f.WriteString(" (")
		formatQuoteNames(&f.Buffer, activeColumnNames...)
		f.WriteString(")")
	}
}

// showCreateInterleave returns an INTERLEAVE IN PARENT clause for the specified
// index, if applicable.
//
// The name of the parent table is prefixed by its database name unless
// it is equal to the given dbPrefix. This allows us to elide the prefix
// when the given index is interleaved in a table of the current database.
func showCreateInterleave(
	idx *sqlbase.IndexDescriptor, buf *bytes.Buffer, dbPrefix string, lCtx simpleSchemaResolver,
) error {
	if len(idx.Interleave.Ancestors) == 0 {
		return nil
	}
	intl := idx.Interleave
	parentTableID := intl.Ancestors[len(intl.Ancestors)-1].TableID
	var err error
	var parentName tree.TableName
	if lCtx != nil {
		parentName, err = getParentAsTableName(lCtx, parentTableID, dbPrefix)
		if err != nil {
			return err
		}
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
	codec keys.SQLCodec,
	tableDesc sqlbase.TableDescriptorInterface,
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
				a, codec, tableDesc.TableDesc(), idxDesc, partDesc, values, fakePrefixDatums)
			if err != nil {
				return err
			}
			buf.WriteString(tuple.String())
		}
		buf.WriteString(`)`)
		if err := ShowCreatePartitioning(
			a, codec, tableDesc, idxDesc, &part.Subpartitioning, buf, indent+1,
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
			a, codec, tableDesc.TableDesc(), idxDesc, partDesc, part.FromInclusive, fakePrefixDatums)
		if err != nil {
			return err
		}
		buf.WriteString(fromTuple.String())
		buf.WriteString(" TO ")
		toTuple, _, err := sqlbase.DecodePartitionTuple(
			a, codec, tableDesc.TableDesc(), idxDesc, partDesc, part.ToExclusive, fakePrefixDatums)
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

// showConstraintClause creates the CONSTRAINT clauses for a CREATE statement,
// writing them to tree.FmtCtx f
func showConstraintClause(
	ctx context.Context,
	desc *sqlbase.ImmutableTableDescriptor,
	semaCtx *tree.SemaContext,
	f *tree.FmtCtx,
) error {
	for _, e := range desc.AllActiveAndInactiveChecks() {
		if e.Hidden {
			continue
		}
		f.WriteString(",\n\t")
		if len(e.Name) > 0 {
			f.WriteString("CONSTRAINT ")
			formatQuoteNames(&f.Buffer, e.Name)
			f.WriteString(" ")
		}
		f.WriteString("CHECK (")
		typed, err := schemaexpr.DeserializeTableDescExpr(ctx, semaCtx, desc, e.Expr)
		if err != nil {
			return err
		}
		f.WriteString(tree.SerializeForDisplay(typed))
		f.WriteString(")")
	}
	f.WriteString("\n)")
	return nil
}
