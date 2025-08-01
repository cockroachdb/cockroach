// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	plpgsql "github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/semenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/pretty"
	"github.com/cockroachdb/errors"
)

// tableComments stores the comment data for a table.
type tableComments struct {
	comment     *string
	columns     []comment
	indexes     []comment
	constraints []comment
}

type comment struct {
	subID   int // the column, or index, or constraint ID that is the subject of this comment.
	comment string
}

// selectComment retrieves all the comments pertaining to a table (comments on the table
// itself but also column and index comments.)
// TODO(chengxiong): consider plumbing the collection through here so that we
// can just fetch comments from collection cache instead of firing extra query.
// An alternative approach would be to leverage a virtual table which internally
// uses the collection.
func selectComment(ctx context.Context, p *planner, tableID descpb.ID) (tc *tableComments) {
	query := fmt.Sprintf("SELECT type, object_id, sub_id, comment FROM system.comments WHERE object_id = %d ORDER BY type, sub_id", tableID)

	txn := p.Txn()
	it, err := p.InternalSQLTxn().QueryIterator(
		ctx, "show-tables-with-comment", txn, query)
	if err != nil {
		log.VEventf(ctx, 1, "%q", err)
	} else {
		var ok bool
		for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
			row := it.Cur()
			commentType := catalogkeys.CommentType(tree.MustBeDInt(row[0]))
			switch commentType {
			case catalogkeys.TableCommentType, catalogkeys.ColumnCommentType,
				catalogkeys.IndexCommentType, catalogkeys.ConstraintCommentType:
				subID := int(tree.MustBeDInt(row[2]))
				cmt := string(tree.MustBeDString(row[3]))

				if tc == nil {
					tc = &tableComments{}
				}

				switch commentType {
				case catalogkeys.TableCommentType:
					tc.comment = &cmt
				case catalogkeys.ColumnCommentType:
					tc.columns = append(tc.columns, comment{subID, cmt})
				case catalogkeys.IndexCommentType:
					tc.indexes = append(tc.indexes, comment{subID, cmt})
				case catalogkeys.ConstraintCommentType:
					tc.constraints = append(tc.constraints, comment{subID, cmt})
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
	ctx context.Context,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	sessionData *sessiondata.SessionData,
	tn *tree.TableName,
	desc catalog.TableDescriptor,
	redactableValues bool,
) (string, error) {
	fmtFlags := tree.FmtSimple
	if redactableValues {
		fmtFlags |= tree.FmtMarkRedactionNode | tree.FmtOmitNameRedaction
	}
	f := tree.NewFmtCtx(fmtFlags)
	f.WriteString("CREATE ")
	if desc.IsTemporary() {
		f.WriteString("TEMP ")
	}
	if desc.MaterializedView() {
		f.WriteString("MATERIALIZED ")
	}
	f.WriteString("VIEW ")
	f.FormatNode(tn)
	f.WriteString(" (")
	cols := desc.PublicColumns()
	for i, col := range cols {
		f.WriteString("\n\t")
		name := col.GetName()
		f.FormatNameP(&name)
		if i == len(cols)-1 {
			f.WriteRune('\n')
		} else {
			f.WriteRune(',')
		}
	}
	f.WriteString(") AS ")

	cfg := tree.DefaultPrettyCfg()
	cfg.UseTabs = true
	cfg.LineWidth = 100 - cfg.TabWidth
	cfg.ValueRedaction = redactableValues
	q, err := formatViewQueryForDisplay(ctx, evalCtx, semaCtx, sessionData, desc, cfg)
	if err != nil {
		return "", err
	}
	for i, line := range strings.Split(q, "\n") {
		if i > 0 {
			f.WriteString("\n\t")
		}
		f.WriteString(line)
	}
	return f.CloseAndGetString(), nil
}

// formatViewQueryForDisplay walks the view query and replaces references to
// user-defined types and sequences with their names. It then round-trips the
// string representation through the parser and the pretty renderer to return
// a human-readable output with the correct level of indentation.
func formatViewQueryForDisplay(
	ctx context.Context,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	sessionData *sessiondata.SessionData,
	desc catalog.TableDescriptor,
	cfg tree.PrettyCfg,
) (query string, err error) {
	defer func() {
		parsed, parseErr := parser.ParseOne(query)
		if parseErr != nil {
			log.Warningf(ctx, "error parsing query for view %s (%v): %+v",
				desc.GetName(), desc.GetID(), err)
			return
		}
		var prettyErr error
		query, prettyErr = cfg.Pretty(parsed.AST)
		if errors.Is(prettyErr, pretty.ErrPrettyMaxRecursionDepthExceeded) {
			// Use simple printing if pretty-printing fails.
			query = tree.AsStringWithFlags(parsed.AST, tree.FmtParsable)
			return
		} else if prettyErr != nil {
			err = prettyErr
			return
		}
	}()

	typeReplacedViewQuery, err := formatViewQueryTypesForDisplay(ctx, evalCtx, semaCtx, sessionData, desc)
	if err != nil {
		log.Warningf(ctx, "error deserializing user defined types for view %s (%v): %+v",
			desc.GetName(), desc.GetID(), err)
		return desc.GetViewQuery(), nil
	}

	// Convert sequences referenced by ID in the view back to their names.
	sequenceReplacedViewQuery, err := formatQuerySequencesForDisplay(ctx, semaCtx, typeReplacedViewQuery, false /* multiStmt */, catpb.Function_SQL)
	if err != nil {
		log.Warningf(ctx, "error converting sequence IDs to names for view %s (%v): %+v",
			desc.GetName(), desc.GetID(), err)
		return typeReplacedViewQuery, nil
	}

	return sequenceReplacedViewQuery, nil
}

// formatQuerySequencesForDisplay walks the view query and
// looks for sequence IDs in the statement. If it finds any,
// it will replace the IDs with the descriptor's fully qualified name.
func formatQuerySequencesForDisplay(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	queries string,
	multiStmt bool,
	lang catpb.Function_Language,
) (string, error) {
	replaceFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		if expr == nil {
			return false, expr, nil
		}
		newExpr, err = schemaexpr.ReplaceSequenceIDsWithFQNames(ctx, expr, semaCtx)
		if err != nil {
			return false, expr, err
		}
		return false, newExpr, nil
	}

	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	switch lang {
	case catpb.Function_SQL:
		var stmts tree.Statements
		if multiStmt {
			parsedStmts, err := parser.Parse(queries)
			if err != nil {
				return "", err
			}
			stmts = make(tree.Statements, len(parsedStmts))
			for i, stmt := range parsedStmts {
				stmts[i] = stmt.AST
			}
		} else {
			stmt, err := parser.ParseOne(queries)
			if err != nil {
				return "", err
			}
			stmts = tree.Statements{stmt.AST}
		}

		for i, stmt := range stmts {
			newStmt, err := tree.SimpleStmtVisit(stmt, replaceFunc)
			if err != nil {
				return "", err
			}
			if i > 0 {
				fmtCtx.WriteString("\n")
			}
			fmtCtx.FormatNode(newStmt)
			if multiStmt {
				fmtCtx.WriteString(";")
			}
		}
	case catpb.Function_PLPGSQL:
		var stmts plpgsqltree.Statement
		plstmt, err := plpgsql.Parse(queries)
		if err != nil {
			return "", errors.Wrap(err, "failed to parse query string")
		}
		stmts = plstmt.AST

		v := plpgsqltree.SQLStmtVisitor{Fn: replaceFunc}
		newStmt := plpgsqltree.Walk(&v, stmts)
		fmtCtx.FormatNode(newStmt)
	}
	return fmtCtx.CloseAndGetString(), nil
}

// formatViewQueryTypesForDisplay walks the view query and
// look for serialized user-defined types. If it finds any,
// it will deserialize it to display its name.
func formatViewQueryTypesForDisplay(
	ctx context.Context,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	sessionData *sessiondata.SessionData,
	desc catalog.TableDescriptor,
) (string, error) {
	replaceFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		// We need to resolve the type to check if it's user-defined. If not,
		// no other work is needed.
		var typRef tree.ResolvableTypeReference
		switch n := expr.(type) {
		case *tree.CastExpr:
			typRef = n.Type
		case *tree.AnnotateTypeExpr:
			typRef = n.Type
		default:
			return true, expr, nil
		}
		var typ *types.T
		typ, err = tree.ResolveType(ctx, typRef, semaCtx.TypeResolver)
		if err != nil {
			return false, expr, err
		}
		if !typ.UserDefined() {
			return true, expr, nil
		}
		formattedExpr, err := schemaexpr.FormatExprForDisplay(
			ctx, desc, expr.String(), evalCtx, semaCtx, sessionData, tree.FmtParsable,
		)
		if err != nil {
			return false, expr, err
		}
		newExpr, err = parser.ParseExpr(formattedExpr)
		if err != nil {
			return false, expr, err
		}
		return false, newExpr, nil
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

// formatFunctionQueryTypesForDisplay is similar to
// formatViewQueryTypesForDisplay but can only be used for functions.
// nil is used as the table descriptor for schemaexpr.FormatExprForDisplay call.
// This is fine assuming that UDFs cannot be created with expression casting a
// column/var to an enum in function body. This is super rare case for now, and
// it's tracked with issue #87475. We should also unify this function with
// formatViewQueryTypesForDisplay.
func formatFunctionQueryTypesForDisplay(
	ctx context.Context,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	sessionData *sessiondata.SessionData,
	queries string,
	lang catpb.Function_Language,
) (string, error) {
	// replaceFunc is a visitor function that replaces user defined type IDs in
	// SQL expressions with their names.
	replaceFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		if expr == nil {
			return false, expr, nil
		}
		// We need to resolve the type to check if it's user-defined. If not,
		// no other work is needed.
		var typRef tree.ResolvableTypeReference
		switch n := expr.(type) {
		case *tree.CastExpr:
			typRef = n.Type
		case *tree.AnnotateTypeExpr:
			typRef = n.Type
		default:
			return true, expr, nil
		}
		var typ *types.T
		typ, err = tree.ResolveType(ctx, typRef, semaCtx.TypeResolver)
		if err != nil {
			return false, expr, err
		}
		if !typ.UserDefined() {
			return true, expr, nil
		}
		formattedExpr, err := schemaexpr.FormatExprForDisplay(
			ctx, nil, expr.String(), evalCtx, semaCtx, sessionData, tree.FmtParsable,
		)
		if err != nil {
			return false, expr, err
		}
		newExpr, err = parser.ParseExpr(formattedExpr)
		if err != nil {
			return false, expr, err
		}
		return false, newExpr, nil
	}
	// replaceTypeFunc is a visitor function that replaces type annotations
	// containing user defined types IDs with their name. This is currently only
	// necessary for some kinds of PLpgSQL statements.
	replaceTypeFunc := func(typ tree.ResolvableTypeReference) (newTyp tree.ResolvableTypeReference, err error) {
		if typ == nil {
			return typ, nil
		}
		// semaCtx may be nil if this is a virtual view being created at
		// init time.
		var typeResolver tree.TypeReferenceResolver
		if semaCtx != nil {
			typeResolver = semaCtx.TypeResolver
		}
		var t *types.T
		t, err = tree.ResolveType(ctx, typ, typeResolver)
		if err != nil {
			return typ, err
		}
		if !t.UserDefined() {
			return typ, nil
		}
		name := t.TypeMeta.Name
		typname := tree.MakeTypeNameWithPrefix(tree.ObjectNamePrefix{
			CatalogName: tree.Name(name.Catalog),
			SchemaName:  tree.Name(name.Schema),
			// Do not include database name, as it makes the type definition less
			// portable when displayed in SHOW CREATE output.
			ExplicitCatalog: false,
			ExplicitSchema:  name.ExplicitSchema,
		}, name.Name)
		ref := typname.ToUnresolvedObjectName()
		return ref, nil
	}

	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	switch lang {
	case catpb.Function_SQL:
		var stmts tree.Statements
		parsedStmts, err := parser.Parse(queries)
		if err != nil {
			return "", errors.Wrap(err, "failed to parse query")
		}
		stmts = make(tree.Statements, len(parsedStmts))
		for i, stmt := range parsedStmts {
			stmts[i] = stmt.AST
		}

		for i, stmt := range stmts {
			newStmt, err := tree.SimpleStmtVisit(stmt, replaceFunc)
			if err != nil {
				return "", err
			}
			if i > 0 {
				fmtCtx.WriteString("\n")
			}
			fmtCtx.FormatNode(newStmt)
			fmtCtx.WriteString(";")
		}
	case catpb.Function_PLPGSQL:
		var stmts plpgsqltree.Statement
		plstmt, err := plpgsql.Parse(queries)
		if err != nil {
			return "", errors.Wrap(err, "failed to parse query string")
		}
		stmts = plstmt.AST

		v := plpgsqltree.SQLStmtVisitor{Fn: replaceFunc}
		newStmt := plpgsqltree.Walk(&v, stmts)
		// Some PLpgSQL statements (i.e., declarations), may contain type
		// annotations containing the UDT. We need to walk the AST to replace them,
		// too.
		v2 := plpgsqltree.TypeRefVisitor{Fn: replaceTypeFunc}
		newStmt = plpgsqltree.Walk(&v2, newStmt)
		fmtCtx.FormatNode(newStmt)
	}

	return fmtCtx.CloseAndGetString(), nil
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
		col, err := catalog.MustFindColumnByPGAttributeNum(table, descpb.PGAttributeNum(columnComment.subID))
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
		idx, err := catalog.MustFindIndexByID(table, descpb.IndexID(indexComment.subID))
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

	for _, constraintComment := range tc.constraints {
		f.WriteString(";\n")
		c, err := catalog.MustFindConstraintByID(table, descpb.ConstraintID(constraintComment.subID))
		if err != nil {
			return err
		}
		f.FormatNode(&tree.CommentOnConstraint{
			Constraint: tree.Name(c.GetName()),
			Table:      tn.ToUnresolvedObjectName(),
			Comment:    &constraintComment.comment,
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
		fkTableName.ExplicitSchema = !searchPath.Contains(fkTableName.SchemaName.String(), false /* includeImplicit */)
		refNames, err = catalog.ColumnNamesForIDs(fkTable, fk.ReferencedColumnIDs)
		if err != nil {
			return err
		}
		originNames, err = catalog.ColumnNamesForIDs(originTable, fk.OriginColumnIDs)
		if err != nil {
			return err
		}
	} else {
		refNames = []string{"???"}
		originNames = []string{"???"}
		fkTableName = tree.MakeTableNameWithSchema(tree.Name(""), catconstants.PublicSchemaName, tree.Name(fmt.Sprintf("[%d as ref]", fk.ReferencedTableID)))
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
	if fk.Match != semenumpb.Match_SIMPLE {
		buf.WriteByte(' ')
		buf.WriteString(tree.CompositeKeyMatchMethodType[fk.Match].String())
	}
	if fk.OnDelete != semenumpb.ForeignKeyAction_NO_ACTION {
		buf.WriteString(" ON DELETE ")
		buf.WriteString(tree.ForeignKeyReferenceActionType[fk.OnDelete].String())
	}
	if fk.OnUpdate != semenumpb.ForeignKeyAction_NO_ACTION {
		buf.WriteString(" ON UPDATE ")
		buf.WriteString(tree.ForeignKeyReferenceActionType[fk.OnUpdate].String())
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
	if opts.AsIntegerType != "" {
		f.Printf(" AS %s", opts.AsIntegerType)
	}
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
	if opts.CacheSize == 1 && opts.NodeCacheSize > 0 {
		f.Printf(" PER NODE CACHE %d", opts.NodeCacheSize)
	}
	return f.CloseAndGetString(), nil
}

// showFamilyClause creates the FAMILY clauses for a CREATE statement, writing them
// to tree.FmtCtx f
func showFamilyClause(desc catalog.TableDescriptor, f *tree.FmtCtx) {
	// Do not show family in SHOW CREATE TABLE if there is only one and
	// it is named "primary".
	families := desc.GetFamilies()
	if len(families) == 1 && families[0].Name == tabledesc.FamilyPrimaryName {
		return
	}
	for _, fam := range families {
		activeColumnNames := make([]string, 0, len(fam.ColumnNames))
		for i, colID := range fam.ColumnIDs {
			if col := catalog.FindColumnByID(desc, colID); col != nil && col.Public() {
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
		return multiregion.FormatTableLocalityConfig(c, f)
	}
	return nil
}

// ShowCreatePartitioning returns a PARTITION BY clause for the specified
// index, if applicable.
func ShowCreatePartitioning(
	a *tree.DatumAlloc,
	codec keys.SQLCodec,
	tableDesc catalog.TableDescriptor,
	idx catalog.Index,
	part catalog.Partitioning,
	buf *bytes.Buffer,
	indent int,
	colOffset int,
	redactableValues bool,
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
		case *catpb.LocalityConfig_RegionalByRow_:
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
		buf.WriteString(tree.NameString(idx.GetKeyColumnName(colOffset + i)))
	}
	buf.WriteString(`) (`)
	fmtFlags := tree.FmtSimple
	if redactableValues {
		fmtFlags |= tree.FmtMarkRedactionNode | tree.FmtOmitNameRedaction
	}
	f := tree.NewFmtCtx(fmtFlags)
	isFirst := true
	err := part.ForEachList(func(name string, values [][]byte, subPartitioning catalog.Partitioning) error {
		if !isFirst {
			buf.WriteString(`, `)
		}
		isFirst = false
		buf.WriteString("\n")
		buf.WriteString(indentStr)
		buf.WriteString("\tPARTITION ")
		f.FormatName(name)
		_, _ = f.Buffer.WriteTo(buf)
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
			f.FormatNode(tuple)
			_, _ = f.Buffer.WriteTo(buf)
		}
		buf.WriteString(`)`)
		return ShowCreatePartitioning(
			a, codec, tableDesc, idx, subPartitioning, buf, indent+1, colOffset+part.NumColumns(),
			redactableValues,
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
		buf.WriteString(tree.NameString(name))
		buf.WriteString(" VALUES FROM ")
		fromTuple, _, err := rowenc.DecodePartitionTuple(
			a, codec, tableDesc, idx, part, from, fakePrefixDatums)
		if err != nil {
			return err
		}
		f.FormatNode(fromTuple)
		_, _ = f.Buffer.WriteTo(buf)
		buf.WriteString(" TO ")
		toTuple, _, err := rowenc.DecodePartitionTuple(
			a, codec, tableDesc, idx, part, to, fakePrefixDatums)
		if err != nil {
			return err
		}
		f.FormatNode(toTuple)
		_, _ = f.Buffer.WriteTo(buf)
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
	ctx context.Context,
	desc catalog.TableDescriptor,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	sessionData *sessiondata.SessionData,
	f *tree.FmtCtx,
) error {
	exprFmtFlags := tree.FmtParsable
	if f.HasFlags(tree.FmtPGCatalog) {
		exprFmtFlags = tree.FmtPGCatalog
	} else {
		if f.HasFlags(tree.FmtMarkRedactionNode) {
			exprFmtFlags |= tree.FmtMarkRedactionNode
		}
		if f.HasFlags(tree.FmtOmitNameRedaction) {
			exprFmtFlags |= tree.FmtOmitNameRedaction
		}
	}
	for _, e := range desc.CheckConstraints() {
		if e.IsHashShardingConstraint() && !e.IsConstraintUnvalidated() {
			continue
		}
		f.WriteString(",\n\t")
		if len(e.GetName()) > 0 {
			f.WriteString("CONSTRAINT ")
			formatQuoteNames(&f.Buffer, e.GetName())
			f.WriteString(" ")
		}
		f.WriteString("CHECK (")
		expr, err := schemaexpr.FormatExprForDisplay(
			ctx, desc, e.GetExpr(), evalCtx, semaCtx, sessionData, exprFmtFlags,
		)
		if err != nil {
			return err
		}
		f.WriteString(expr)
		f.WriteString(")")
		if !e.IsConstraintValidated() {
			f.WriteString(" NOT VALID")
		}
	}
	for _, c := range desc.UniqueConstraintsWithoutIndex() {
		f.WriteString(",\n\t")
		if len(c.GetName()) > 0 {
			f.WriteString("CONSTRAINT ")
			formatQuoteNames(&f.Buffer, c.GetName())
			f.WriteString(" ")
		}
		f.WriteString("UNIQUE WITHOUT INDEX (")
		colNames, err := catalog.ColumnNamesForIDs(desc, c.CollectKeyColumnIDs().Ordered())
		if err != nil {
			return err
		}
		f.WriteString(strings.Join(colNames, ", "))
		f.WriteString(")")
		if c.IsPartial() {
			f.WriteString(" WHERE ")
			pred, err := schemaexpr.FormatExprForDisplay(
				ctx, desc, c.GetPredicate(), evalCtx, semaCtx, sessionData, exprFmtFlags,
			)
			if err != nil {
				return err
			}
			f.WriteString(pred)
		}
		if !c.IsConstraintValidated() {
			f.WriteString(" NOT VALID")
		}
	}
	f.WriteString("\n)")
	return nil
}
