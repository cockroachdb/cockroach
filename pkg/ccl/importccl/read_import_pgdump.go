// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

type postgreStream struct {
	ctx                   context.Context
	s                     *bufio.Scanner
	copy                  *postgreStreamCopy
	unsupportedStmtLogger *unsupportedStmtLogger
}

// newPostgreStream returns a struct that can stream statements from an
// io.Reader.
func newPostgreStream(
	ctx context.Context, r io.Reader, max int, unsupportedStmtLogger *unsupportedStmtLogger,
) *postgreStream {
	s := bufio.NewScanner(r)
	s.Buffer(nil, max)
	p := &postgreStream{ctx: ctx, s: s, unsupportedStmtLogger: unsupportedStmtLogger}
	s.Split(p.split)
	return p
}

func (p *postgreStream) split(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if p.copy == nil {
		return splitSQLSemicolon(data, atEOF)
	}
	return bufio.ScanLines(data, atEOF)
}

// splitSQLSemicolon is a bufio.SplitFunc that splits on SQL semicolon tokens.
func splitSQLSemicolon(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	if pos, ok := parser.SplitFirstStatement(string(data)); ok {
		return pos, data[:pos], nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}
	// Request more data.
	return 0, nil, nil
}

// Next returns the next statement. The type of statement can be one of
// tree.Statement, copyData, or errCopyDone. A nil statement and io.EOF are
// returned when there are no more statements.
func (p *postgreStream) Next() (interface{}, error) {
	if p.copy != nil {
		row, err := p.copy.Next()
		if errors.Is(err, errCopyDone) {
			p.copy = nil
			return errCopyDone, nil
		}
		return row, err
	}

	for p.s.Scan() {
		t := p.s.Text()
		skipOverComments(t)

		stmts, err := parser.Parse(t)
		if err != nil {
			// There are some statements that CRDB is unable to parse. If the user has
			// indicated that they want to skip these stmts during the IMPORT, then do
			// so here.
			if p.unsupportedStmtLogger.ignoreUnsupported && errors.HasType(err, (*tree.UnsupportedError)(nil)) {
				if unsupportedErr := (*tree.UnsupportedError)(nil); errors.As(err, &unsupportedErr) {
					err := p.unsupportedStmtLogger.log(unsupportedErr.FeatureName, true /* isParseError */)
					if err != nil {
						return nil, err
					}
				}
				continue
			}
			return nil, wrapErrorWithUnsupportedHint(err)
		}
		switch len(stmts) {
		case 0:
			// Got whitespace or comments; try again.
		case 1:
			// If the statement is COPY ... FROM STDIN, set p.copy so the next call to
			// this function will read copy data. We still return this COPY statement
			// for this invocation.
			if cf, ok := stmts[0].AST.(*tree.CopyFrom); ok && cf.Stdin {
				// Set p.copy which reconfigures the scanner's split func.
				p.copy = newPostgreStreamCopy(p.s, copyDefaultDelimiter, copyDefaultNull)

				// We expect a single newline character following the COPY statement before
				// the copy data starts.
				if !p.s.Scan() {
					return nil, errors.Errorf("expected empty line")
				}
				if err := p.s.Err(); err != nil {
					return nil, err
				}
				if len(p.s.Bytes()) != 0 {
					return nil, errors.Errorf("expected empty line")
				}
			}
			return stmts[0].AST, nil
		default:
			return nil, errors.Errorf("unexpected: got %d statements", len(stmts))
		}
	}
	if err := p.s.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			err = wrapWithLineTooLongHint(
				errors.HandledWithMessage(err, "line too long"),
			)
		}
		return nil, err
	}
	return nil, io.EOF
}

var (
	ignoreComments = regexp.MustCompile(`^\s*(--.*)`)
)

func skipOverComments(s string) {
	// Look for the first line with no whitespace or comments.
	for {
		m := ignoreComments.FindStringIndex(s)
		if m == nil {
			break
		}
		s = s[m[1]:]
	}
}

type regclassRewriter struct{}

var _ tree.Visitor = regclassRewriter{}

func (regclassRewriter) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	switch t := expr.(type) {
	case *tree.FuncExpr:
		switch t.Func.String() {
		case "nextval":
			if len(t.Exprs) > 0 {
				switch e := t.Exprs[0].(type) {
				case *tree.CastExpr:
					if typ, ok := tree.GetStaticallyKnownType(e.Type); ok && typ.Oid() == oid.T_regclass {
						// tree.Visitor says we should make a copy, but since copyNode is unexported
						// and there's no planner here, I think it's safe to directly modify the
						// statement here.
						t.Exprs[0] = e.Expr
					}
				}
			}
		}
	}
	return true, expr
}

func (regclassRewriter) VisitPost(expr tree.Expr) tree.Expr { return expr }

// removeDefaultRegclass removes `::regclass` casts from sequence operations
// (i.e., nextval) in DEFAULT column expressions.
func removeDefaultRegclass(create *tree.CreateTable) {
	for _, def := range create.Defs {
		switch def := def.(type) {
		case *tree.ColumnTableDef:
			if def.DefaultExpr.Expr != nil {
				def.DefaultExpr.Expr, _ = tree.WalkExpr(regclassRewriter{}, def.DefaultExpr.Expr)
			}
		}
	}
}

type schemaAndTableName struct {
	schema string
	table  string
}

func (s *schemaAndTableName) String() string {
	var ret string
	if s.schema != "" {
		ret += s.schema + "."
	}
	ret += s.table
	return ret
}

type schemaParsingObjects struct {
	createSchema map[string]*tree.CreateSchema
	createTbl    map[schemaAndTableName]*tree.CreateTable
	createSeq    map[schemaAndTableName]*tree.CreateSequence
	tableFKs     map[schemaAndTableName][]*tree.ForeignKeyConstraintTableDef
}

func createPostgresSchemas(
	ctx context.Context,
	parentID descpb.ID,
	schemasToCreate map[string]*tree.CreateSchema,
	execCfg *sql.ExecutorConfig,
	user security.SQLUsername,
) ([]*schemadesc.Mutable, error) {
	createSchema := func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		dbDesc catalog.DatabaseDescriptor, schema *tree.CreateSchema,
	) (*schemadesc.Mutable, error) {
		desc, _, err := sql.CreateUserDefinedSchemaDescriptor(
			ctx, user, schema, txn, descriptors, execCfg, dbDesc, false, /* allocateID */
		)
		if err != nil {
			return nil, err
		}

		// This is true when the schema exists and we are processing a
		// CREATE SCHEMA IF NOT EXISTS statement.
		if desc == nil {
			return nil, nil
		}

		// We didn't allocate an ID above, so we must assign it a mock ID until it
		// is assigned an actual ID later in the import.
		desc.ID = getNextPlaceholderDescID()
		desc.SetOffline("importing")
		return desc, nil
	}
	var schemaDescs []*schemadesc.Mutable
	createSchemaDescs := func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {
		schemaDescs = nil // reset for retries
		_, dbDesc, err := descriptors.GetImmutableDatabaseByID(ctx, txn, parentID, tree.DatabaseLookupFlags{
			Required:    true,
			AvoidCached: true,
		})
		if err != nil {
			return err
		}
		for _, schema := range schemasToCreate {
			scDesc, err := createSchema(ctx, txn, descriptors, dbDesc, schema)
			if err != nil {
				return err
			}
			if scDesc != nil {
				schemaDescs = append(schemaDescs, scDesc)
			}
		}
		return nil
	}
	if err := descs.Txn(
		ctx, execCfg.Settings, execCfg.LeaseManager, execCfg.InternalExecutor,
		execCfg.DB, createSchemaDescs,
	); err != nil {
		return nil, err
	}
	return schemaDescs, nil
}

func createPostgresSequences(
	ctx context.Context,
	parentID descpb.ID,
	createSeq map[schemaAndTableName]*tree.CreateSequence,
	fks fkHandler,
	walltime int64,
	owner security.SQLUsername,
	schemaNameToDesc map[string]*schemadesc.Mutable,
) ([]*tabledesc.Mutable, error) {
	ret := make([]*tabledesc.Mutable, 0)
	for schemaAndTableName, seq := range createSeq {
		schema, err := getSchemaByNameFromMap(schemaAndTableName, schemaNameToDesc)
		if err != nil {
			return nil, err
		}
		desc, err := sql.NewSequenceTableDesc(
			ctx,
			schemaAndTableName.table,
			seq.Options,
			parentID,
			schema.GetID(),
			getNextPlaceholderDescID(),
			hlc.Timestamp{WallTime: walltime},
			descpb.NewDefaultPrivilegeDescriptor(owner),
			tree.PersistencePermanent,
			nil, /* params */
			// If this is multi-region, this will get added by WriteDescriptors.
			false, /* isMultiRegion */
		)
		if err != nil {
			return nil, err
		}
		fks.resolver.tableNameToDesc[schemaAndTableName.String()] = desc
		ret = append(ret, desc)
	}

	return ret, nil
}

func getSchemaByNameFromMap(
	schemaAndTableName schemaAndTableName, schemaNameToDesc map[string]*schemadesc.Mutable,
) (catalog.SchemaDescriptor, error) {
	var schema catalog.SchemaDescriptor
	switch schemaAndTableName.schema {
	case "", "public":
		schema = schemadesc.GetPublicSchema()
	default:
		var ok bool
		if schema, ok = schemaNameToDesc[schemaAndTableName.schema]; !ok {
			return nil, errors.Newf("schema %q not found in the schemas created from the pgdump",
				schema)
		}
	}
	return schema, nil
}

func createPostgresTables(
	evalCtx *tree.EvalContext,
	p sql.JobExecContext,
	createTbl map[schemaAndTableName]*tree.CreateTable,
	fks fkHandler,
	backrefs map[descpb.ID]*tabledesc.Mutable,
	parentDB catalog.DatabaseDescriptor,
	walltime int64,
	schemaNameToDesc map[string]*schemadesc.Mutable,
) ([]*tabledesc.Mutable, error) {
	ret := make([]*tabledesc.Mutable, 0)
	for schemaAndTableName, create := range createTbl {
		if create == nil {
			continue
		}
		schema, err := getSchemaByNameFromMap(schemaAndTableName, schemaNameToDesc)
		if err != nil {
			return nil, err
		}
		removeDefaultRegclass(create)
		desc, err := MakeSimpleTableDescriptor(evalCtx.Ctx(), p.SemaCtx(), p.ExecCfg().Settings,
			create, parentDB, schema, getNextPlaceholderDescID(), fks, walltime)
		if err != nil {
			return nil, err
		}
		fks.resolver.tableNameToDesc[schemaAndTableName.String()] = desc
		backrefs[desc.ID] = desc
		ret = append(ret, desc)
	}

	return ret, nil
}

func resolvePostgresFKs(
	evalCtx *tree.EvalContext,
	parentDB catalog.DatabaseDescriptor,
	tableFKs map[schemaAndTableName][]*tree.ForeignKeyConstraintTableDef,
	fks fkHandler,
	backrefs map[descpb.ID]*tabledesc.Mutable,
	schemaNameToDesc map[string]*schemadesc.Mutable,
) error {
	for schemaAndTableName, constraints := range tableFKs {
		desc := fks.resolver.tableNameToDesc[schemaAndTableName.String()]
		if desc == nil {
			continue
		}
		schema, err := getSchemaByNameFromMap(schemaAndTableName, schemaNameToDesc)
		if err != nil {
			return err
		}
		for _, constraint := range constraints {
			if constraint.Table.Schema() == "" {
				return errors.Errorf("schema expected to be non-empty when resolving postgres FK %s",
					constraint.Name.String())
			}
			constraint.Table.ExplicitSchema = true
			// Add a dummy catalog name to aid in object resolution.
			if constraint.Table.Catalog() == "" {
				constraint.Table.ExplicitCatalog = true
				constraint.Table.CatalogName = "defaultdb"
			}
			if err := sql.ResolveFK(
				evalCtx.Ctx(), nil /* txn */, &fks.resolver,
				parentDB, schema, desc,
				constraint, backrefs, sql.NewTable,
				tree.ValidationDefault, evalCtx,
			); err != nil {
				return err
			}
		}
		if err := fixDescriptorFKState(desc); err != nil {
			return err
		}
	}

	return nil
}

var placeholderDescID = defaultCSVTableID

// getNextPlaceholderDescID returns a monotonically increasing placeholder ID
// that is used when creating table, sequence and schema descriptors during the
// schema parsing phase of a PGDUMP import.
// We assign these descriptors "fake" IDs because it is early in the IMPORT
// execution and we do not want to blow through GenerateUniqueDescID calls only
// to fail during the verification phase before we actually begin ingesting
// data. Thus, we pessimistically wait till all the verification steps in the
// IMPORT have been completed after which we rewrite the descriptor IDs with
// "real" unique IDs.
func getNextPlaceholderDescID() descpb.ID {
	ret := placeholderDescID
	placeholderDescID++
	return ret
}

// readPostgresCreateTable returns table descriptors for all tables or the
// matching table from SQL statements.
func readPostgresCreateTable(
	ctx context.Context,
	input io.Reader,
	evalCtx *tree.EvalContext,
	p sql.JobExecContext,
	match string,
	parentDB catalog.DatabaseDescriptor,
	walltime int64,
	fks fkHandler,
	max int,
	owner security.SQLUsername,
	unsupportedStmtLogger *unsupportedStmtLogger,
) ([]*tabledesc.Mutable, []*schemadesc.Mutable, error) {
	// Modify the CreateTable stmt with the various index additions. We do this
	// instead of creating a full table descriptor first and adding indexes
	// later because MakeSimpleTableDescriptor calls the sql package which calls
	// AllocateIDs which adds the hidden rowid and default primary key. This means
	// we'd have to delete the index and row and modify the column family. This
	// is much easier and probably safer too.
	schemaObjects := schemaParsingObjects{
		createSchema: make(map[string]*tree.CreateSchema),
		createTbl:    make(map[schemaAndTableName]*tree.CreateTable),
		createSeq:    make(map[schemaAndTableName]*tree.CreateSequence),
		tableFKs:     make(map[schemaAndTableName][]*tree.ForeignKeyConstraintTableDef),
	}
	ps := newPostgreStream(ctx, input, max, unsupportedStmtLogger)
	for {
		stmt, err := ps.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, errors.Wrap(err, "postgres parse error")
		}
		if err := readPostgresStmt(ctx, evalCtx, match, fks, &schemaObjects, stmt, p,
			parentDB.GetID(), unsupportedStmtLogger); err != nil {
			return nil, nil, err
		}
	}

	tables := make([]*tabledesc.Mutable, 0, len(schemaObjects.createTbl))
	schemaNameToDesc := make(map[string]*schemadesc.Mutable)
	schemaDescs, err := createPostgresSchemas(ctx, parentDB.GetID(), schemaObjects.createSchema,
		p.ExecCfg(), p.User())
	if err != nil {
		return nil, nil, err
	}

	for _, schemaDesc := range schemaDescs {
		schemaNameToDesc[schemaDesc.GetName()] = schemaDesc
	}

	// Construct sequence descriptors.
	seqs, err := createPostgresSequences(
		ctx,
		parentDB.GetID(),
		schemaObjects.createSeq,
		fks,
		walltime,
		owner,
		schemaNameToDesc,
	)
	if err != nil {
		return nil, nil, err
	}
	tables = append(tables, seqs...)

	// Construct table descriptors.
	backrefs := make(map[descpb.ID]*tabledesc.Mutable)
	tableDescs, err := createPostgresTables(evalCtx, p, schemaObjects.createTbl, fks, backrefs,
		parentDB, walltime, schemaNameToDesc)
	if err != nil {
		return nil, nil, err
	}
	tables = append(tables, tableDescs...)

	// Resolve FKs.
	err = resolvePostgresFKs(
		evalCtx, parentDB, schemaObjects.tableFKs, fks, backrefs, schemaNameToDesc,
	)
	if err != nil {
		return nil, nil, err
	}
	if match != "" && len(tables) != 1 {
		found := make([]string, 0, len(schemaObjects.createTbl))
		for schemaAndTableName := range schemaObjects.createTbl {
			found = append(found, schemaAndTableName.String())
		}
		return nil, nil, errors.Errorf("table %q not found in file (found tables: %s)", match,
			strings.Join(found, ", "))
	}
	if len(tables) == 0 {
		return nil, nil, errors.Errorf("no table definition found")
	}
	return tables, schemaDescs, nil
}

func readPostgresStmt(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	match string,
	fks fkHandler,
	schemaObjects *schemaParsingObjects,
	stmt interface{},
	p sql.JobExecContext,
	parentID descpb.ID,
	unsupportedStmtLogger *unsupportedStmtLogger,
) error {
	ignoreUnsupportedStmts := unsupportedStmtLogger.ignoreUnsupported
	switch stmt := stmt.(type) {
	case *tree.CreateSchema:
		name, err := getSchemaName(&stmt.Schema)
		if err != nil {
			return err
		}
		// If a target table is specified we do not want to create any user defined
		// schemas. This is because we only allow specifying target table's in the
		// public schema.
		if match != "" {
			break
		}
		schemaObjects.createSchema[name] = stmt
	case *tree.CreateTable:
		schemaQualifiedName, err := getSchemaAndTableName(&stmt.Table)
		if err != nil {
			return err
		}
		isMatch := match == "" || match == schemaQualifiedName.String()
		if isMatch {
			schemaObjects.createTbl[schemaQualifiedName] = stmt
		} else {
			schemaObjects.createTbl[schemaQualifiedName] = nil
		}
	case *tree.CreateIndex:
		if stmt.Predicate != nil {
			return unimplemented.NewWithIssue(50225, "cannot import a table with partial indexes")
		}
		schemaQualifiedTableName, err := getSchemaAndTableName(&stmt.Table)
		if err != nil {
			return err
		}
		create := schemaObjects.createTbl[schemaQualifiedTableName]
		if create == nil {
			break
		}
		var idx tree.TableDef = &tree.IndexTableDef{
			Name:             stmt.Name,
			Columns:          stmt.Columns,
			Storing:          stmt.Storing,
			Inverted:         stmt.Inverted,
			Interleave:       stmt.Interleave,
			PartitionByIndex: stmt.PartitionByIndex,
		}
		if stmt.Unique {
			idx = &tree.UniqueConstraintTableDef{IndexTableDef: *idx.(*tree.IndexTableDef)}
		}
		create.Defs = append(create.Defs, idx)
	case *tree.AlterSchema:
		switch stmt.Cmd {
		default:
		}
	case *tree.AlterTable:
		schemaQualifiedTableName, err := getSchemaAndTableName2(stmt.Table)
		if err != nil {
			return err
		}
		create := schemaObjects.createTbl[schemaQualifiedTableName]
		if create == nil {
			break
		}
		for _, cmd := range stmt.Cmds {
			switch cmd := cmd.(type) {
			case *tree.AlterTableAddConstraint:
				switch con := cmd.ConstraintDef.(type) {
				case *tree.ForeignKeyConstraintTableDef:
					if !fks.skip {
						if con.Table.Schema() == "" {
							con.Table.SchemaName = tree.PublicSchemaName
						}
						schemaObjects.tableFKs[schemaQualifiedTableName] = append(schemaObjects.tableFKs[schemaQualifiedTableName], con)
					}
				default:
					create.Defs = append(create.Defs, cmd.ConstraintDef)
				}
			case *tree.AlterTableSetDefault:
				found := false
				for i, def := range create.Defs {
					def, ok := def.(*tree.ColumnTableDef)
					// If it's not a column definition, or the column name doesn't match,
					// we're not interested in this column.
					if !ok || def.Name != cmd.Column {
						continue
					}
					def.DefaultExpr.Expr = cmd.Default
					create.Defs[i] = def
					found = true
					break
				}
				if !found {
					return colinfo.NewUndefinedColumnError(cmd.Column.String())
				}
			case *tree.AlterTableSetVisible:
				found := false
				for i, def := range create.Defs {
					def, ok := def.(*tree.ColumnTableDef)
					// If it's not a column definition, or the column name doesn't match,
					// we're not interested in this column.
					if !ok || def.Name != cmd.Column {
						continue
					}
					def.Hidden = !cmd.Visible
					create.Defs[i] = def
					found = true
					break
				}
				if !found {
					return colinfo.NewUndefinedColumnError(cmd.Column.String())
				}
			case *tree.AlterTableAddColumn:
				if cmd.IfNotExists {
					if ignoreUnsupportedStmts {
						err := unsupportedStmtLogger.log(stmt.String(), false /* isParseError */)
						if err != nil {
							return err
						}
						continue
					}
					return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported statement: %s", stmt))
				}
				create.Defs = append(create.Defs, cmd.ColumnDef)
			case *tree.AlterTableSetNotNull:
				found := false
				for i, def := range create.Defs {
					def, ok := def.(*tree.ColumnTableDef)
					// If it's not a column definition, or the column name doesn't match,
					// we're not interested in this column.
					if !ok || def.Name != cmd.Column {
						continue
					}
					def.Nullable.Nullability = tree.NotNull
					create.Defs[i] = def
					found = true
					break
				}
				if !found {
					return colinfo.NewUndefinedColumnError(cmd.Column.String())
				}
			default:
				if ignoreUnsupportedStmts {
					err := unsupportedStmtLogger.log(stmt.String(), false /* isParseError */)
					if err != nil {
						return err
					}
					continue
				}
				return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported statement: %s", stmt))
			}
		}
	case *tree.AlterTableOwner:
		if ignoreUnsupportedStmts {
			return unsupportedStmtLogger.log(stmt.String(), false /* isParseError */)
		}
		return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported statement: %s", stmt))
	case *tree.CreateSequence:
		schemaQualifiedTableName, err := getSchemaAndTableName(&stmt.Name)
		if err != nil {
			return err
		}
		if match == "" || match == schemaQualifiedTableName.String() {
			schemaObjects.createSeq[schemaQualifiedTableName] = stmt
		}
	case *tree.AlterSequence:
		if ignoreUnsupportedStmts {
			return unsupportedStmtLogger.log(stmt.String(), false /* isParseError */)
		}
		return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported %T statement: %s", stmt, stmt))
	// Some SELECT statements mutate schema. Search for those here.
	case *tree.Select:
		switch sel := stmt.Select.(type) {
		case *tree.SelectClause:
			for _, selExpr := range sel.Exprs {
				switch expr := selExpr.Expr.(type) {
				case *tree.FuncExpr:
					// Look for function calls that mutate schema (this is actually a thing).
					semaCtx := tree.MakeSemaContext()
					if _, err := expr.TypeCheck(ctx, &semaCtx, nil /* desired */); err != nil {
						// If the expression does not type check, it may be a case of using
						// a column that does not exist yet in a setval call (as is the case
						// of PGDUMP output from ogr2ogr). We're not interested in setval
						// calls during schema reading so it is safe to ignore this for now.
						if f := expr.Func.String(); pgerror.GetPGCode(err) == pgcode.UndefinedColumn && f == "setval" {
							continue
						}
						return err
					}
					ov := expr.ResolvedOverload()
					// Search for a SQLFn, which returns a SQL string to execute.
					fn := ov.SQLFn
					if fn == nil {
						err := errors.Errorf("unsupported function call: %s in stmt: %s",
							expr.Func.String(), stmt.String())
						if ignoreUnsupportedStmts {
							err := unsupportedStmtLogger.log(err.Error(), false /* isParseError */)
							if err != nil {
								return err
							}
							continue
						}
						return wrapErrorWithUnsupportedHint(err)
					}
					// Attempt to convert all func exprs to datums.
					datums := make(tree.Datums, len(expr.Exprs))
					for i, ex := range expr.Exprs {
						d, ok := ex.(tree.Datum)
						if !ok {
							// We got something that wasn't a datum so we can't call the
							// overload. Since this is a SQLFn and the user would have
							// expected us to execute it, we have to error.
							return errors.Errorf("unsupported statement: %s", stmt)
						}
						datums[i] = d
					}
					// Now that we have all of the datums, we can execute the overload.
					fnSQL, err := fn(evalCtx, datums)
					if err != nil {
						return err
					}
					// We have some sql. Parse and process it.
					fnStmts, err := parser.Parse(fnSQL)
					if err != nil {
						return err
					}
					for _, fnStmt := range fnStmts {
						switch ast := fnStmt.AST.(type) {
						case *tree.AlterTable:
							if err := readPostgresStmt(ctx, evalCtx, match, fks, schemaObjects, ast, p,
								parentID, unsupportedStmtLogger); err != nil {
								return err
							}
						default:
							// We only support ALTER statements returned from a SQLFn.
							return errors.Errorf("unsupported statement: %s", stmt)
						}
					}
				default:
					err := errors.Errorf("unsupported %T SELECT expr: %s", expr, expr)
					if ignoreUnsupportedStmts {
						err := unsupportedStmtLogger.log(err.Error(), false /* isParseError */)
						if err != nil {
							return err
						}
						continue
					}
					return wrapErrorWithUnsupportedHint(err)
				}
			}
		default:
			err := errors.Errorf("unsupported %T SELECT %s", sel, sel)
			if ignoreUnsupportedStmts {
				err := unsupportedStmtLogger.log(err.Error(), false /* isParseError */)
				if err != nil {
					return err
				}
				return nil
			}
			return wrapErrorWithUnsupportedHint(err)
		}
	case *tree.DropTable:
		names := stmt.Names

		// If we find a table with the same name in the target DB we are importing
		// into and same public schema, then we throw an error telling the user to
		// drop the conflicting existing table to proceed.
		// Otherwise, we silently ignore the drop statement and continue with the import.
		for _, name := range names {
			tableName := name.ToUnresolvedObjectName().String()
			if err := p.ExecCfg().DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				err := catalogkv.CheckObjectCollision(
					ctx,
					txn,
					p.ExecCfg().Codec,
					parentID,
					keys.PublicSchemaID,
					tree.NewUnqualifiedTableName(tree.Name(tableName)),
				)
				if err != nil {
					return errors.Wrapf(err, `drop table "%s" and then retry the import`, tableName)
				}
				return nil
			}); err != nil {
				return err
			}
		}
	case *tree.BeginTransaction, *tree.CommitTransaction:
		// Ignore transaction statements as they have no meaning during an IMPORT.
		// TODO(during review): Should we guard these statements under the
		// ignore_unsupported flag as well?
	case *tree.Insert, *tree.CopyFrom, *tree.Delete, copyData:
		// handled during the data ingestion pass.
	case *tree.CreateExtension, *tree.CommentOnDatabase, *tree.CommentOnTable,
		*tree.CommentOnIndex, *tree.CommentOnColumn, *tree.SetVar, *tree.Analyze:
		// These are the statements that can be parsed by CRDB but are not
		// supported, or are not required to be processed, during an IMPORT.
		// - ignore txns.
		// - ignore SETs and DMLs.
		// - ANALYZE is syntactic sugar for CreateStatistics. It can be ignored
		// because the auto stats stuff will pick up the changes and run if needed.
		if ignoreUnsupportedStmts {
			return unsupportedStmtLogger.log(fmt.Sprintf("%s", stmt), false /* isParseError */)
		}
		return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported %T statement: %s", stmt, stmt))
	case error:
		if !errors.Is(stmt, errCopyDone) {
			return stmt
		}
	default:
		if ignoreUnsupportedStmts {
			return unsupportedStmtLogger.log(fmt.Sprintf("%s", stmt), false /* isParseError */)
		}
		return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported %T statement: %s", stmt, stmt))
	}
	return nil
}

func getSchemaName(sc *tree.ObjectNamePrefix) (string, error) {
	if sc.ExplicitCatalog {
		return "", unimplemented.Newf("import into database specified in dump file",
			"explicit catalog schemas unsupported: %s", sc.CatalogName.String()+sc.SchemaName.String())
	}
	return sc.SchemaName.String(), nil
}

func getSchemaAndTableName(tn *tree.TableName) (schemaAndTableName, error) {
	var ret schemaAndTableName
	ret.schema = tree.PublicSchema
	if tn.Schema() != "" {
		ret.schema = tn.Schema()
	}
	ret.table = tn.Table()
	return ret, nil
}

// getTableName variant for UnresolvedObjectName.
func getSchemaAndTableName2(u *tree.UnresolvedObjectName) (schemaAndTableName, error) {
	var ret schemaAndTableName
	ret.schema = tree.PublicSchema
	if u.NumParts >= 2 && u.Parts[1] != "" {
		ret.schema = u.Parts[1]
	}
	ret.table = u.Parts[0]
	return ret, nil
}

type pgDumpReader struct {
	tableDescs            map[string]catalog.TableDescriptor
	tables                map[string]*row.DatumRowConverter
	descs                 map[string]*execinfrapb.ReadImportDataSpec_ImportTable
	kvCh                  chan row.KVBatch
	opts                  roachpb.PgDumpOptions
	walltime              int64
	colMap                map[*row.DatumRowConverter](map[string]int)
	jobID                 int64
	unsupportedStmtLogger *unsupportedStmtLogger
	evalCtx               *tree.EvalContext
}

var _ inputConverter = &pgDumpReader{}

// newPgDumpReader creates a new inputConverter for pg_dump files.
func newPgDumpReader(
	ctx context.Context,
	jobID int64,
	kvCh chan row.KVBatch,
	opts roachpb.PgDumpOptions,
	walltime int64,
	descs map[string]*execinfrapb.ReadImportDataSpec_ImportTable,
	evalCtx *tree.EvalContext,
) (*pgDumpReader, error) {
	tableDescs := make(map[string]catalog.TableDescriptor, len(descs))
	converters := make(map[string]*row.DatumRowConverter, len(descs))
	colMap := make(map[*row.DatumRowConverter](map[string]int))
	for name, table := range descs {
		if table.Desc.IsTable() {
			tableDesc := tabledesc.NewBuilder(table.Desc).BuildImmutableTable()
			colSubMap := make(map[string]int, len(table.TargetCols))
			targetCols := make(tree.NameList, len(table.TargetCols))
			for i, colName := range table.TargetCols {
				targetCols[i] = tree.Name(colName)
			}
			for i, col := range tableDesc.VisibleColumns() {
				colSubMap[col.GetName()] = i
			}
			conv, err := row.NewDatumRowConverter(ctx, tableDesc, targetCols, evalCtx, kvCh,
				nil /* seqChunkProvider */)
			if err != nil {
				return nil, err
			}
			converters[name] = conv
			colMap[conv] = colSubMap
			tableDescs[name] = tableDesc
		} else if table.Desc.IsSequence() {
			seqDesc := tabledesc.NewBuilder(table.Desc).BuildImmutableTable()
			tableDescs[name] = seqDesc
		}
	}
	return &pgDumpReader{
		kvCh:       kvCh,
		tableDescs: tableDescs,
		tables:     converters,
		descs:      descs,
		opts:       opts,
		walltime:   walltime,
		colMap:     colMap,
		jobID:      jobID,
		evalCtx:    evalCtx,
	}, nil
}

func (m *pgDumpReader) start(ctx ctxgroup.Group) {
}

func (m *pgDumpReader) readFiles(
	ctx context.Context,
	dataFiles map[int32]string,
	resumePos map[int32]int64,
	format roachpb.IOFileFormat,
	makeExternalStorage cloud.ExternalStorageFactory,
	user security.SQLUsername,
) error {
	// Setup logger to handle unsupported DML statements seen in the PGDUMP file.
	m.unsupportedStmtLogger = makeUnsupportedStmtLogger(ctx, user,
		m.jobID, format.PgDump.IgnoreUnsupported, format.PgDump.IgnoreUnsupportedLog, dataIngestion,
		makeExternalStorage)

	err := readInputFiles(ctx, dataFiles, resumePos, format, m.readFile, makeExternalStorage, user)
	if err != nil {
		return err
	}

	return m.unsupportedStmtLogger.flush()
}

func wrapErrorWithUnsupportedHint(err error) error {
	return errors.WithHintf(err,
		"To ignore unsupported statements and log them for review post IMPORT, see the options listed"+
			" in the docs: %s", "https://www.cockroachlabs.com/docs/stable/import.html#import-options")
}

func (m *pgDumpReader) readFile(
	ctx context.Context, input *fileReader, inputIdx int32, resumePos int64, rejected chan string,
) error {
	tableNameToRowsProcessed := make(map[string]int64)
	var inserts, count int64
	rowLimit := m.opts.RowLimit
	ps := newPostgreStream(ctx, input, int(m.opts.MaxRowSize), m.unsupportedStmtLogger)
	semaCtx := tree.MakeSemaContext()
	for _, conv := range m.tables {
		conv.KvBatch.Source = inputIdx
		conv.FractionFn = input.ReadFraction
		conv.CompletedRowFn = func() int64 {
			return count
		}
	}

	for {
		stmt, err := ps.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Wrap(err, "postgres parse error")
		}
		switch i := stmt.(type) {
		case *tree.Insert:
			n, ok := i.Table.(*tree.TableName)
			if !ok {
				return errors.Errorf("unexpected: %T", i.Table)
			}
			name, err := getSchemaAndTableName(n)
			if err != nil {
				return errors.Wrapf(err, "%s", i)
			}
			conv, ok := m.tables[name.String()]
			if !ok {
				// not importing this table.
				continue
			}
			if ok && conv == nil {
				return errors.Errorf("missing schema info for requested table %q", name)
			}
			expectedColLen := len(i.Columns)
			if expectedColLen == 0 {
				// Case where the targeted columns are not specified in the PGDUMP file, but in
				// the command "IMPORT INTO table (targetCols) PGDUMP DATA (filename)"
				expectedColLen = len(conv.VisibleCols)
			}
			timestamp := timestampAfterEpoch(m.walltime)
			values, ok := i.Rows.Select.(*tree.ValuesClause)
			if !ok {
				if m.unsupportedStmtLogger.ignoreUnsupported {
					logLine := fmt.Sprintf("%s: unsupported by IMPORT\n",
						i.Rows.Select.String())
					err := m.unsupportedStmtLogger.log(logLine, false /* isParseError */)
					if err != nil {
						return err
					}
					continue
				}
				return wrapErrorWithUnsupportedHint(errors.Errorf("unsupported: %s", i.Rows.Select))
			}
			inserts++
			startingCount := count
			var targetColMapIdx []int
			if len(i.Columns) != 0 {
				targetColMapIdx = make([]int, len(i.Columns))
				conv.TargetColOrds = util.FastIntSet{}
				for j := range i.Columns {
					colName := string(i.Columns[j])
					idx, ok := m.colMap[conv][colName]
					if !ok {
						return errors.Newf("targeted column %q not found", colName)
					}
					conv.TargetColOrds.Add(idx)
					targetColMapIdx[j] = idx
				}
				// For any missing columns, fill those to NULL.
				// These will get filled in with the correct default / computed expression
				// provided conv.IsTargetCol is not set for the given column index.
				for idx := range conv.VisibleCols {
					if !conv.TargetColOrds.Contains(idx) {
						conv.Datums[idx] = tree.DNull
					}
				}
			}
			for _, tuple := range values.Rows {
				count++
				tableNameToRowsProcessed[name.String()]++
				if count <= resumePos {
					continue
				}
				if rowLimit != 0 && tableNameToRowsProcessed[name.String()] > rowLimit {
					break
				}
				if got := len(tuple); expectedColLen != got {
					return errors.Errorf("expected %d values, got %d: %v", expectedColLen, got, tuple)
				}
				for j, expr := range tuple {
					idx := j
					if len(i.Columns) != 0 {
						idx = targetColMapIdx[j]
					}
					typed, err := expr.TypeCheck(ctx, &semaCtx, conv.VisibleColTypes[idx])
					if err != nil {
						return errors.Wrapf(err, "reading row %d (%d in insert statement %d)",
							count, count-startingCount, inserts)
					}
					converted, err := typed.Eval(conv.EvalCtx)
					if err != nil {
						return errors.Wrapf(err, "reading row %d (%d in insert statement %d)",
							count, count-startingCount, inserts)
					}
					conv.Datums[idx] = converted
				}
				if err := conv.Row(ctx, inputIdx, count+int64(timestamp)); err != nil {
					return err
				}
			}
		case *tree.CopyFrom:
			if !i.Stdin {
				return errors.New("expected STDIN option on COPY FROM")
			}
			name, err := getSchemaAndTableName(&i.Table)
			if err != nil {
				return errors.Wrapf(err, "%s", i)
			}
			conv, importing := m.tables[name.String()]
			if importing && conv == nil {
				return errors.Errorf("missing schema info for requested table %q", name)
			}
			var targetColMapIdx []int
			if conv != nil {
				targetColMapIdx = make([]int, len(i.Columns))
				conv.TargetColOrds = util.FastIntSet{}
				for j := range i.Columns {
					colName := string(i.Columns[j])
					idx, ok := m.colMap[conv][colName]
					if !ok {
						return errors.Newf("targeted column %q not found", colName)
					}
					conv.TargetColOrds.Add(idx)
					targetColMapIdx[j] = idx
				}
			}
			for {
				row, err := ps.Next()
				// We expect an explicit copyDone here. io.EOF is unexpected.
				if err == io.EOF {
					return makeRowErr("", count, pgcode.ProtocolViolation,
						"unexpected EOF")
				}
				if row == errCopyDone {
					break
				}
				count++
				tableNameToRowsProcessed[name.String()]++
				if err != nil {
					return wrapRowErr(err, "", count, pgcode.Uncategorized, "")
				}
				if !importing {
					continue
				}
				if count <= resumePos {
					continue
				}
				switch row := row.(type) {
				case copyData:
					if expected, got := conv.TargetColOrds.Len(), len(row); expected != got {
						return makeRowErr("", count, pgcode.Syntax,
							"expected %d values, got %d", expected, got)
					}
					if rowLimit != 0 && tableNameToRowsProcessed[name.String()] > rowLimit {
						break
					}
					for i, s := range row {
						idx := targetColMapIdx[i]
						if s == nil {
							conv.Datums[idx] = tree.DNull
						} else {
							// We use ParseAndRequireString instead of ParseDatumStringAs
							// because postgres dumps arrays in COPY statements using their
							// internal string representation.
							conv.Datums[idx], _, err = tree.ParseAndRequireString(conv.VisibleColTypes[idx], *s, conv.EvalCtx)
							if err != nil {
								col := conv.VisibleCols[idx]
								return wrapRowErr(err, "", count, pgcode.Syntax,
									"parse %q as %s", col.GetName(), col.GetType().SQLString())
							}
						}
					}
					if err := conv.Row(ctx, inputIdx, count); err != nil {
						return err
					}
				default:
					return makeRowErr("", count, pgcode.Uncategorized,
						"unexpected: %v", row)
				}
			}
		case *tree.Select:
			// Look for something of the form "SELECT pg_catalog.setval(...)". Any error
			// or unexpected value silently breaks out of this branch. We are silent
			// instead of returning an error because we expect input to be well-formatted
			// by pg_dump, and thus if it isn't, we don't try to figure out what to do.
			sc, ok := i.Select.(*tree.SelectClause)
			if !ok {
				err := errors.Errorf("unsupported %T Select: %v", i.Select, i.Select)
				if m.unsupportedStmtLogger.ignoreUnsupported {
					err := m.unsupportedStmtLogger.log(err.Error(), false /* isParseError */)
					if err != nil {
						return err
					}
					continue
				}
				return wrapErrorWithUnsupportedHint(err)
			}
			if len(sc.Exprs) != 1 {
				err := errors.Errorf("unsupported %d select args: %v", len(sc.Exprs), sc.Exprs)
				if m.unsupportedStmtLogger.ignoreUnsupported {
					err := m.unsupportedStmtLogger.log(err.Error(), false /* isParseError */)
					if err != nil {
						return err
					}
					continue
				}
				return wrapErrorWithUnsupportedHint(err)
			}
			fn, ok := sc.Exprs[0].Expr.(*tree.FuncExpr)
			if !ok {
				err := errors.Errorf("unsupported select arg %T: %v", sc.Exprs[0].Expr, sc.Exprs[0].Expr)
				if m.unsupportedStmtLogger.ignoreUnsupported {
					err := m.unsupportedStmtLogger.log(err.Error(), false /* isParseError */)
					if err != nil {
						return err
					}
					continue
				}
				return wrapErrorWithUnsupportedHint(err)
			}

			switch funcName := strings.ToLower(fn.Func.String()); funcName {
			case "search_path", "pg_catalog.set_config":
				err := errors.Errorf("unsupported %d fn args in select: %v", len(fn.Exprs), fn.Exprs)
				if m.unsupportedStmtLogger.ignoreUnsupported {
					err := m.unsupportedStmtLogger.log(err.Error(), false /* isParseError */)
					if err != nil {
						return err
					}
					continue
				}
				return wrapErrorWithUnsupportedHint(err)
			case "setval", "pg_catalog.setval":
				if args := len(fn.Exprs); args < 2 || args > 3 {
					err := errors.Errorf("unsupported %d fn args in select: %v", len(fn.Exprs), fn.Exprs)
					if m.unsupportedStmtLogger.ignoreUnsupported {
						err := m.unsupportedStmtLogger.log(err.Error(), false /* isParseError */)
						if err != nil {
							return err
						}
						continue
					}
					return err
				}
				seqname, ok := fn.Exprs[0].(*tree.StrVal)
				if !ok {
					if nested, nestedOk := fn.Exprs[0].(*tree.FuncExpr); nestedOk && nested.Func.String() == "pg_get_serial_sequence" {
						// ogr2ogr dumps set the seq for the PK by a) looking up the seqname
						// and then b) running an aggregate on the just-imported data to
						// determine the max value. We're not going to do any of that, but
						// we can just ignore all of this because we mapped their "serial"
						// to our rowid anyway so there is no seq to maintain.
						continue
					}
					return errors.Errorf("unsupported setval %T arg: %v", fn.Exprs[0], fn.Exprs[0])
				}
				seqval, ok := fn.Exprs[1].(*tree.NumVal)
				if !ok {
					err := errors.Errorf("unsupported setval %T arg: %v", fn.Exprs[1], fn.Exprs[1])
					if m.unsupportedStmtLogger.ignoreUnsupported {
						err := m.unsupportedStmtLogger.log(err.Error(), false /* isParseError */)
						if err != nil {
							return err
						}
						continue
					}
					return wrapErrorWithUnsupportedHint(err)
				}
				val, err := seqval.AsInt64()
				if err != nil {
					return errors.Wrap(err, "unsupported setval arg")
				}
				isCalled := false
				if len(fn.Exprs) == 3 {
					called, ok := fn.Exprs[2].(*tree.DBool)
					if !ok {
						err := errors.Errorf("unsupported setval %T arg: %v", fn.Exprs[2], fn.Exprs[2])
						if m.unsupportedStmtLogger.ignoreUnsupported {
							err := m.unsupportedStmtLogger.log(err.Error(), false /* isParseError */)
							if err != nil {
								return err
							}
							continue
						}
						return err
					}
					isCalled = bool(*called)
				}
				name, err := parser.ParseTableName(seqname.RawString())
				if err != nil {
					break
				}

				seqName := name.Parts[0]
				if name.Schema() != "" {
					seqName = fmt.Sprintf("%s.%s", name.Schema(), name.Object())
				}
				seq := m.tableDescs[seqName]
				if seq == nil {
					break
				}
				key, val, err := sql.MakeSequenceKeyVal(m.evalCtx.Codec, seq, val, isCalled)
				if err != nil {
					return wrapRowErr(err, "", count, pgcode.Uncategorized, "")
				}
				kv := roachpb.KeyValue{Key: key}
				kv.Value.SetInt(val)
				m.kvCh <- row.KVBatch{
					Source: inputIdx, KVs: []roachpb.KeyValue{kv}, Progress: input.ReadFraction(),
				}
			case "addgeometrycolumn":
				// handled during schema extraction.
			default:
				err := errors.Errorf("unsupported function %s in stmt %s", funcName, i.Select.String())
				if m.unsupportedStmtLogger.ignoreUnsupported {
					err := m.unsupportedStmtLogger.log(err.Error(), false /* isParseError */)
					if err != nil {
						return err
					}
					continue
				}
				return wrapErrorWithUnsupportedHint(err)
			}
		case *tree.CreateExtension, *tree.CommentOnDatabase, *tree.CommentOnTable,
			*tree.CommentOnIndex, *tree.CommentOnColumn, *tree.AlterSequence:
			// handled during schema extraction.
		case *tree.SetVar, *tree.BeginTransaction, *tree.CommitTransaction, *tree.Analyze:
			// handled during schema extraction.
		case *tree.CreateTable, *tree.CreateSchema, *tree.AlterTable, *tree.AlterTableOwner,
			*tree.CreateIndex, *tree.CreateSequence, *tree.DropTable:
			// handled during schema extraction.
		default:
			err := errors.Errorf("unsupported %T statement: %v", i, i)
			if m.unsupportedStmtLogger.ignoreUnsupported {
				err := m.unsupportedStmtLogger.log(err.Error(), false /* isParseError */)
				if err != nil {
					return err
				}
				continue
			}
			return wrapErrorWithUnsupportedHint(err)
		}
	}
	for _, conv := range m.tables {
		if err := conv.SendBatch(ctx); err != nil {
			return err
		}
	}
	return nil
}

func wrapWithLineTooLongHint(err error) error {
	return errors.WithHintf(
		err,
		"use `max_row_size` to increase the maximum line limit (default: %s).",
		humanizeutil.IBytes(defaultScanBuffer),
	)
}
