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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
func selectComment(ctx context.Context, p PlanHookState, tableID descpb.ID) (tc *tableComments) {
	query := fmt.Sprintf("SELECT type, object_id, sub_id, comment FROM system.comments WHERE object_id = %d", tableID)

	txn := p.ExtendedEvalContext().Txn
	it, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryIterator(
		ctx, "show-tables-with-comment", txn, query)
	if err != nil {
		log.VEventf(ctx, 1, "%q", err)
	} else {
		var ok bool
		for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
			row := it.Cur()
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
		if err != nil {
			log.VEventf(ctx, 1, "%q", err)
			tc = nil
		}
	}

	return tc
}

// ShowCreateView returns a valid SQL representation of the CREATE VIEW
// statement used to create the given view. It is used in the implementation of
// the crdb_internal.create_statements virtual table.
func ShowCreateView(
	ctx context.Context, semaCtx *tree.SemaContext, tn *tree.TableName, desc catalog.TableDescriptor,
) (string, error) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteString("CREATE ")
	if desc.IsTemporary() {
		f.WriteString("TEMP ")
	}
	f.WriteString("VIEW ")
	f.FormatNode(tn)
	f.WriteString(" (")
	for i, col := range desc.PublicColumns() {
		if i > 0 {
			f.WriteString(", ")
		}
		name := col.GetName()
		f.FormatNameP(&name)
	}
	f.WriteString(") AS ")

	// Deserialize user-defined types in the view query.
	typeReplacedViewQuery, err := formatViewQueryTypesForDisplay(ctx, semaCtx, desc)
	if err != nil {
		log.Warningf(ctx,
			"error deserializing user defined types for view %s (%v): %+v",
			desc.GetName(), desc.GetID(), err)
		f.WriteString(desc.GetViewQuery())
	} else {
		// Convert sequences referenced by ID in the view back to their names.
		sequenceReplacedViewQuery, err := formatViewQuerySequencesForDisplay(
			ctx, semaCtx, typeReplacedViewQuery)
		if err != nil {
			log.Warningf(ctx,
				"error converting sequence IDs to names for view %s (%v): %+v",
				desc.GetName(), desc.GetID(), err)
			f.WriteString(typeReplacedViewQuery)
		} else {
			f.WriteString(sequenceReplacedViewQuery)
		}
	}

	return f.CloseAndGetString(), nil
}

// formatViewQuerySequencesForDisplay walks the view query and
// looks for sequence IDs in the statement. If it finds any,
// it will replace the IDs with the descriptor's fully qualified name.
func formatViewQuerySequencesForDisplay(
	ctx context.Context, semaCtx *tree.SemaContext, viewQuery string,
) (string, error) {
	replaceFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		newExpr, err = schemaexpr.ReplaceIDsWithFQNames(ctx, expr, semaCtx)
		if err != nil {
			return false, expr, err
		}
		return false, newExpr, nil
	}

	stmt, err := parser.ParseOne(viewQuery)
	if err != nil {
		return "", err
	}

	newStmt, err := tree.SimpleStmtVisit(stmt.AST, replaceFunc)
	if err != nil {
		return "", err
	}
	return newStmt.String(), nil
}

// formatViewQueryTypesForDisplay walks the view query and
// look for serialized user-defined types. If it finds any,
// it will deserialize it to display its name.
func formatViewQueryTypesForDisplay(
	ctx context.Context, semaCtx *tree.SemaContext, desc catalog.TableDescriptor,
) (string, error) {
	replaceFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		switch n := expr.(type) {
		case *tree.AnnotateTypeExpr, *tree.CastExpr:
			texpr, err := tree.TypeCheck(ctx, n, semaCtx, types.Any)
			if err != nil {
				return false, expr, err
			}
			if !texpr.ResolvedType().UserDefined() {
				return true, expr, nil
			}

			formattedExpr, err := schemaexpr.FormatExprForDisplay(
				ctx, desc, expr.String(), semaCtx, tree.FmtParsable)
			if err != nil {
				return false, expr, err
			}
			newExpr, err = parser.ParseExpr(formattedExpr)
			if err != nil {
				return false, expr, err
			}
			return false, newExpr, nil
		default:
			return true, expr, nil
		}
	}

	viewQuery := desc.GetViewQuery()
	stmt, err := parser.ParseOne(viewQuery)
	if err != nil {
		return "", err
	}

	newStmt, err := tree.SimpleStmtVisit(stmt.AST, replaceFunc)
	if err != nil {
		return "", err
	}
	return newStmt.String(), nil
}

// showComments prints out the COMMENT statements sufficient to populate a
// table's comments, including its index and column comments.
func showComments(
	tn *tree.TableName, table catalog.TableDescriptor, tc *tableComments, buf *bytes.Buffer,
) error {
	if tc == nil {
		return nil
	}
	f := tree.NewFmtCtx(tree.FmtSimple)
	un := tn.ToUnresolvedObjectName()
	if tc.comment != nil {
		f.WriteString(";\n")
		f.FormatNode(&tree.CommentOnTable{
			Table:   un,
			Comment: tc.comment,
		})
	}

	for _, columnComment := range tc.columns {
		col, err := table.FindColumnWithID(descpb.ColumnID(columnComment.subID))
		if err != nil {
			return err
		}

		f.WriteString(";\n")
		f.FormatNode(&tree.CommentOnColumn{
			ColumnItem: &tree.ColumnItem{
				TableName:  tn.ToUnresolvedObjectName(),
				ColumnName: tree.Name(col.GetName()),
			},
			Comment: &columnComment.comment,
		})
	}

	for _, indexComment := range tc.indexes {
		idx, err := table.FindIndexWithID(descpb.IndexID(indexComment.subID))
		if err != nil {
			return err
		}

		f.WriteString(";\n")
		f.FormatNode(&tree.CommentOnIndex{
			Index: tree.TableIndexName{
				Table: *tn,
				Index: tree.UnrestrictedName(idx.GetName()),
			},
			Comment: &indexComment.comment,
		})
	}

	buf.WriteString(f.CloseAndGetString())
	return nil
}

// showForeignKeyConstraint returns a valid SQL representation of a FOREIGN KEY
// clause for a given index. If the table's schema name is in the searchPath, then the
// schema name will not be included in the result.
func showForeignKeyConstraint(
	buf *bytes.Buffer,
	dbPrefix string,
	originTable catalog.TableDescriptor,
	fk *descpb.ForeignKeyConstraint,
	lCtx simpleSchemaResolver,
	searchPath sessiondata.SearchPath,
) error {
	var refNames []string
	var originNames []string
	var fkTableName tree.TableName
	if lCtx != nil {
		fkTable, err := lCtx.getTableByID(fk.ReferencedTableID)
		if err != nil {
			return err
		}
		fkTableName, err = getTableNameFromTableDescriptor(lCtx, fkTable, dbPrefix)
		if err != nil {
			return err
		}
		fkTableName.ExplicitSchema = !searchPath.Contains(fkTableName.SchemaName.String())
		refNames, err = fkTable.NamesForColumnIDs(fk.ReferencedColumnIDs)
		if err != nil {
			return err
		}
		originNames, err = originTable.NamesForColumnIDs(fk.OriginColumnIDs)
		if err != nil {
			return err
		}
	} else {
		refNames = []string{"???"}
		originNames = []string{"???"}
		fkTableName = tree.MakeTableNameWithSchema(tree.Name(""), tree.PublicSchemaName, tree.Name(fmt.Sprintf("[%d as ref]", fk.ReferencedTableID)))
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
	if fk.Match != descpb.ForeignKeyReference_SIMPLE {
		buf.WriteByte(' ')
		buf.WriteString(fk.Match.String())
	}
	if fk.OnDelete != descpb.ForeignKeyReference_NO_ACTION {
		buf.WriteString(" ON DELETE ")
		buf.WriteString(fk.OnDelete.String())
	}
	if fk.OnUpdate != descpb.ForeignKeyReference_NO_ACTION {
		buf.WriteString(" ON UPDATE ")
		buf.WriteString(fk.OnUpdate.String())
	}
	if fk.Validity != descpb.ConstraintValidity_Validated {
		buf.WriteString(" NOT VALID")
	}
	return nil
}

// ShowCreateSequence returns a valid SQL representation of the
// CREATE SEQUENCE statement used to create the given sequence.
func ShowCreateSequence(
	ctx context.Context, tn *tree.TableName, desc catalog.TableDescriptor,
) (string, error) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteString("CREATE ")
	if desc.IsTemporary() {
		f.WriteString("TEMP ")
	}
	f.WriteString("SEQUENCE ")
	f.FormatNode(tn)
	opts := desc.GetSequenceOpts()
	f.Printf(" MINVALUE %d", opts.MinValue)
	f.Printf(" MAXVALUE %d", opts.MaxValue)
	f.Printf(" INCREMENT %d", opts.Increment)
	f.Printf(" START %d", opts.Start)
	if opts.Virtual {
		f.Printf(" VIRTUAL")
	}
	if opts.CacheSize > 1 {
		f.Printf(" CACHE %d", opts.CacheSize)
	}
	return f.CloseAndGetString(), nil
}

// showFamilyClause creates the FAMILY clauses for a CREATE statement, writing them
// to tree.FmtCtx f
func showFamilyClause(desc catalog.TableDescriptor, f *tree.FmtCtx) {
	for _, fam := range desc.GetFamilies() {
		activeColumnNames := make([]string, 0, len(fam.ColumnNames))
		for i, colID := range fam.ColumnIDs {
			if col, _ := desc.FindColumnWithID(colID); col != nil && col.Public() {
				activeColumnNames = append(activeColumnNames, fam.ColumnNames[i])
			}
		}
		if len(desc.PublicColumns()) == 0 {
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

// showCreateLocality creates the LOCALITY clauses for a CREATE statement, writing them
// to tree.FmtCtx f.
func showCreateLocality(desc catalog.TableDescriptor, f *tree.FmtCtx) error {
	if c := desc.GetLocalityConfig(); c != nil {
		f.WriteString(" LOCALITY ")
		return tabledesc.FormatTableLocalityConfig(c, f)
	}
	return nil
}

// showCreateInterleave returns an INTERLEAVE IN PARENT clause for the specified
// index, if applicable.
//
// The name of the parent table is prefixed by its database name unless
// it is equal to the given dbPrefix. This allows us to elide the prefix
// when the given index is interleaved in a table of the current database.
func showCreateInterleave(
	idx catalog.Index, buf *bytes.Buffer, dbPrefix string, lCtx simpleSchemaResolver,
) error {
	if idx.NumInterleaveAncestors() == 0 {
		return nil
	}
	intl := idx.IndexDesc().Interleave
	parentTableID := intl.Ancestors[len(intl.Ancestors)-1].TableID
	var err error
	var parentName tree.TableName
	if lCtx != nil {
		parentName, err = getParentAsTableName(lCtx, parentTableID, dbPrefix)
		if err != nil {
			return err
		}
	} else {
		parentName = tree.MakeTableNameWithSchema(tree.Name(""), tree.PublicSchemaName, tree.Name(fmt.Sprintf("[%d as parent]", parentTableID)))
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
	formatQuoteNames(buf, idx.IndexDesc().KeyColumnNames[:sharedPrefixLen]...)
	buf.WriteString(")")
	return nil
}

// ShowCreatePartitioning returns a PARTITION BY clause for the specified
// index, if applicable.
func ShowCreatePartitioning(
	a *rowenc.DatumAlloc,
	codec keys.SQLCodec,
	tableDesc catalog.TableDescriptor,
	idx catalog.Index,
	part catalog.Partitioning,
	buf *bytes.Buffer,
	indent int,
	colOffset int,
) error {
	isPrimaryKeyOfPartitionAllByTable :=
		tableDesc.IsPartitionAllBy() && tableDesc.GetPrimaryIndexID() == idx.GetID() && colOffset == 0

	if part.NumColumns() == 0 && !isPrimaryKeyOfPartitionAllByTable {
		return nil
	}
	// Do not print PARTITION BY clauses of non-primary indexes belonging to a table
	// that is PARTITION BY ALL. The ALL will be printed for the PRIMARY INDEX clause.
	if tableDesc.IsPartitionAllBy() && tableDesc.GetPrimaryIndexID() != idx.GetID() {
		return nil
	}
	// Do not print PARTITION ALL BY if we are a REGIONAL BY ROW table.
	if c := tableDesc.GetLocalityConfig(); c != nil {
		switch c.Locality.(type) {
		case *descpb.TableDescriptor_LocalityConfig_RegionalByRow_:
			return nil
		}
	}

	// We don't need real prefixes in the DecodePartitionTuple calls because we
	// only use the tree.Datums part of the output.
	fakePrefixDatums := make([]tree.Datum, colOffset)
	for i := range fakePrefixDatums {
		fakePrefixDatums[i] = tree.DNull
	}

	indentStr := strings.Repeat("\t", indent)
	buf.WriteString(` PARTITION `)
	if isPrimaryKeyOfPartitionAllByTable {
		buf.WriteString(`ALL `)
	}
	buf.WriteString(`BY `)
	if part.NumLists() > 0 {
		buf.WriteString(`LIST`)
	} else if part.NumRanges() > 0 {
		buf.WriteString(`RANGE`)
	} else if isPrimaryKeyOfPartitionAllByTable {
		buf.WriteString(`NOTHING`)
		return nil
	} else {
		return errors.Errorf(`invalid partition descriptor: %v`, part.PartitioningDesc())
	}
	buf.WriteString(` (`)
	for i := 0; i < part.NumColumns(); i++ {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(idx.GetKeyColumnName(colOffset + i))
	}
	buf.WriteString(`) (`)
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	isFirst := true
	err := part.ForEachList(func(name string, values [][]byte, subPartitioning catalog.Partitioning) error {
		if !isFirst {
			buf.WriteString(`, `)
		}
		isFirst = false
		buf.WriteString("\n")
		buf.WriteString(indentStr)
		buf.WriteString("\tPARTITION ")
		fmtCtx.FormatNameP(&name)
		_, _ = fmtCtx.Buffer.WriteTo(buf)
		buf.WriteString(` VALUES IN (`)
		for j, values := range values {
			if j != 0 {
				buf.WriteString(`, `)
			}
			tuple, _, err := rowenc.DecodePartitionTuple(
				a, codec, tableDesc, idx, part, values, fakePrefixDatums)
			if err != nil {
				return err
			}
			buf.WriteString(tuple.String())
		}
		buf.WriteString(`)`)
		return ShowCreatePartitioning(
			a, codec, tableDesc, idx, subPartitioning, buf, indent+1, colOffset+part.NumColumns(),
		)
	})
	if err != nil {
		return err
	}
	isFirst = true
	err = part.ForEachRange(func(name string, from, to []byte) error {
		if !isFirst {
			buf.WriteString(`, `)
		}
		isFirst = false
		buf.WriteString("\n")
		buf.WriteString(indentStr)
		buf.WriteString("\tPARTITION ")
		buf.WriteString(name)
		buf.WriteString(" VALUES FROM ")
		fromTuple, _, err := rowenc.DecodePartitionTuple(
			a, codec, tableDesc, idx, part, from, fakePrefixDatums)
		if err != nil {
			return err
		}
		buf.WriteString(fromTuple.String())
		buf.WriteString(" TO ")
		toTuple, _, err := rowenc.DecodePartitionTuple(
			a, codec, tableDesc, idx, part, to, fakePrefixDatums)
		if err != nil {
			return err
		}
		buf.WriteString(toTuple.String())
		return nil
	})
	if err != nil {
		return err
	}
	buf.WriteString("\n")
	buf.WriteString(indentStr)
	buf.WriteString(")")
	return nil
}

// showConstraintClause creates the CONSTRAINT clauses for a CREATE statement,
// writing them to tree.FmtCtx f
func showConstraintClause(
	ctx context.Context, desc catalog.TableDescriptor, semaCtx *tree.SemaContext, f *tree.FmtCtx,
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
		expr, err := schemaexpr.FormatExprForDisplay(ctx, desc, e.Expr, semaCtx, tree.FmtParsable)
		if err != nil {
			return err
		}
		f.WriteString(expr)
		f.WriteString(")")
		if e.Validity != descpb.ConstraintValidity_Validated {
			f.WriteString(" NOT VALID")
		}
	}
	for _, c := range desc.AllActiveAndInactiveUniqueWithoutIndexConstraints() {
		f.WriteString(",\n\t")
		if len(c.Name) > 0 {
			f.WriteString("CONSTRAINT ")
			formatQuoteNames(&f.Buffer, c.Name)
			f.WriteString(" ")
		}
		f.WriteString("UNIQUE WITHOUT INDEX (")
		colNames, err := desc.NamesForColumnIDs(c.ColumnIDs)
		if err != nil {
			return err
		}
		f.WriteString(strings.Join(colNames, ", "))
		f.WriteString(")")
		if c.IsPartial() {
			f.WriteString(" WHERE ")
			pred, err := schemaexpr.FormatExprForDisplay(ctx, desc, c.Predicate, semaCtx, tree.FmtParsable)
			if err != nil {
				return err
			}
			f.WriteString(pred)
		}
		if c.Validity != descpb.ConstraintValidity_Validated {
			f.WriteString(" NOT VALID")
		}
	}
	f.WriteString("\n)")
	return nil
}
