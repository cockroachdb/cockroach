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
	"io"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
	s    *bufio.Scanner
	copy *postgreStreamCopy
}

// newPostgreStream returns a struct that can stream statements from an
// io.Reader.
func newPostgreStream(r io.Reader, max int) *postgreStream {
	s := bufio.NewScanner(r)
	s.Buffer(nil, max)
	p := &postgreStream{s: s}
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
		// Regardless if we can parse the statement, check that it's not something
		// we want to ignore.
		if isIgnoredStatement(t) {
			continue
		}

		stmts, err := parser.Parse(t)
		if err != nil {
			return nil, err
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
	ignoreComments   = regexp.MustCompile(`^\s*(--.*)`)
	ignoreStatements = []*regexp.Regexp{
		regexp.MustCompile("(?i)^alter function"),
		regexp.MustCompile("(?i)^alter sequence .* owned by"),
		regexp.MustCompile("(?i)^comment on"),
		regexp.MustCompile("(?i)^create extension"),
		regexp.MustCompile("(?i)^create function"),
		regexp.MustCompile("(?i)^create trigger"),
		regexp.MustCompile("(?i)^grant .* on sequence"),
		regexp.MustCompile("(?i)^revoke .* on sequence"),
	}
)

func isIgnoredStatement(s string) bool {
	// Look for the first line with no whitespace or comments.
	for {
		m := ignoreComments.FindStringIndex(s)
		if m == nil {
			break
		}
		s = s[m[1]:]
	}
	s = strings.TrimSpace(s)
	for _, re := range ignoreStatements {
		if re.MatchString(s) {
			return true
		}
	}
	return false
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

// readPostgresCreateTable returns table descriptors for all tables or the
// matching table from SQL statements.
func readPostgresCreateTable(
	ctx context.Context,
	input io.Reader,
	evalCtx *tree.EvalContext,
	p sql.JobExecContext,
	match string,
	parentID descpb.ID,
	walltime int64,
	fks fkHandler,
	max int,
	owner security.SQLUsername,
) ([]*tabledesc.Mutable, error) {
	// Modify the CreateTable stmt with the various index additions. We do this
	// instead of creating a full table descriptor first and adding indexes
	// later because MakeSimpleTableDescriptor calls the sql package which calls
	// AllocateIDs which adds the hidden rowid and default primary key. This means
	// we'd have to delete the index and row and modify the column family. This
	// is much easier and probably safer too.
	createTbl := make(map[string]*tree.CreateTable)
	createSeq := make(map[string]*tree.CreateSequence)
	tableFKs := make(map[string][]*tree.ForeignKeyConstraintTableDef)
	ps := newPostgreStream(input, max)
	for {
		stmt, err := ps.Next()
		if err == io.EOF {
			ret := make([]*tabledesc.Mutable, 0, len(createTbl))
			for name, seq := range createSeq {
				id := descpb.ID(int(defaultCSVTableID) + len(ret))
				desc, err := sql.NewSequenceTableDesc(
					ctx,
					name,
					seq.Options,
					parentID,
					keys.PublicSchemaID,
					id,
					hlc.Timestamp{WallTime: walltime},
					descpb.NewDefaultPrivilegeDescriptor(owner),
					tree.PersistencePermanent,
					nil, /* params */
				)
				if err != nil {
					return nil, err
				}
				fks.resolver[desc.Name] = desc
				ret = append(ret, desc)
			}
			backrefs := make(map[descpb.ID]*tabledesc.Mutable)
			for _, create := range createTbl {
				if create == nil {
					continue
				}
				removeDefaultRegclass(create)
				id := descpb.ID(int(defaultCSVTableID) + len(ret))
				desc, err := MakeSimpleTableDescriptor(evalCtx.Ctx(), p.SemaCtx(), p.ExecCfg().Settings, create, parentID, keys.PublicSchemaID, id, fks, walltime)
				if err != nil {
					return nil, err
				}
				fks.resolver[desc.Name] = desc
				backrefs[desc.ID] = desc
				ret = append(ret, desc)
			}
			for name, constraints := range tableFKs {
				desc := fks.resolver[name]
				if desc == nil {
					continue
				}
				for _, constraint := range constraints {
					if err := sql.ResolveFK(
						evalCtx.Ctx(), nil /* txn */, fks.resolver, desc, constraint, backrefs, sql.NewTable, tree.ValidationDefault, evalCtx,
					); err != nil {
						return nil, err
					}
				}
				if err := fixDescriptorFKState(desc); err != nil {
					return nil, err
				}
			}
			if match != "" && len(ret) != 1 {
				found := make([]string, 0, len(createTbl))
				for name := range createTbl {
					found = append(found, name)
				}
				return nil, errors.Errorf("table %q not found in file (found tables: %s)", match, strings.Join(found, ", "))
			}
			if len(ret) == 0 {
				return nil, errors.Errorf("no table definition found")
			}
			return ret, nil
		}
		if err != nil {
			return nil, errors.Wrap(err, "postgres parse error")
		}
		if err := readPostgresStmt(ctx, evalCtx, match, fks, createTbl, createSeq, tableFKs, stmt, p, parentID); err != nil {
			return nil, err
		}
	}
}

func readPostgresStmt(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	match string,
	fks fkHandler,
	createTbl map[string]*tree.CreateTable,
	createSeq map[string]*tree.CreateSequence,
	tableFKs map[string][]*tree.ForeignKeyConstraintTableDef,
	stmt interface{},
	p sql.JobExecContext,
	parentID descpb.ID,
) error {
	switch stmt := stmt.(type) {
	case *tree.CreateTable:
		name, err := getTableName(&stmt.Table)
		if err != nil {
			return err
		}
		if match != "" && match != name {
			createTbl[name] = nil
		} else {
			createTbl[name] = stmt
		}
	case *tree.CreateIndex:
		if stmt.Predicate != nil {
			return unimplemented.NewWithIssue(50225, "cannot import a table with partial indexes")
		}
		name, err := getTableName(&stmt.Table)
		if err != nil {
			return err
		}
		create := createTbl[name]
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
	case *tree.AlterTable:
		name, err := getTableName2(stmt.Table)
		if err != nil {
			return err
		}
		create := createTbl[name]
		if create == nil {
			break
		}
		for _, cmd := range stmt.Cmds {
			switch cmd := cmd.(type) {
			case *tree.AlterTableAddConstraint:
				switch con := cmd.ConstraintDef.(type) {
				case *tree.ForeignKeyConstraintTableDef:
					if !fks.skip {
						tableFKs[name] = append(tableFKs[name], con)
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
			case *tree.AlterTableAddColumn:
				if cmd.IfNotExists {
					return errors.Errorf("unsupported statement: %s", stmt)
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
			case *tree.AlterTableValidateConstraint:
				// ignore
			default:
				return errors.Errorf("unsupported statement: %s", stmt)
			}
		}
	case *tree.AlterTableOwner:
		// ignore
	case *tree.CreateSequence:
		name, err := getTableName(&stmt.Name)
		if err != nil {
			return err
		}
		if match == "" || match == name {
			createSeq[name] = stmt
		}
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
						switch f := expr.Func.String(); f {
						case "set_config", "setval":
							continue
						default:
							return errors.Errorf("unsupported function call: %s", expr.Func.String())
						}
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
							if err := readPostgresStmt(ctx, evalCtx, match, fks, createTbl, createSeq, tableFKs, ast, p, parentID); err != nil {
								return err
							}
						default:
							// We only support ALTER statements returned from a SQLFn.
							return errors.Errorf("unsupported statement: %s", stmt)
						}
					}
				default:
					return errors.Errorf("unsupported %T SELECT expr: %s", expr, expr)
				}
			}
		default:
			return errors.Errorf("unsupported %T SELECT: %s", sel, sel)
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
				err := backupccl.CheckObjectExists(
					ctx,
					txn,
					p.ExecCfg().Codec,
					parentID,
					keys.PublicSchemaID,
					tableName,
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
		// ignore txns.
	case *tree.SetVar, *tree.Insert, *tree.CopyFrom, copyData, *tree.Delete:
		// ignore SETs and DMLs.
	case *tree.Analyze:
		// ANALYZE is syntatictic sugar for CreateStatistics. It can be ignored because
		// the auto stats stuff will pick up the changes and run if needed.
	case error:
		if !errors.Is(stmt, errCopyDone) {
			return stmt
		}
	default:
		return errors.Errorf("unsupported %T statement: %s", stmt, stmt)
	}
	return nil
}

func getTableName(tn *tree.TableName) (string, error) {
	if sc := tn.Schema(); sc != "" && sc != "public" {
		return "", unimplemented.NewWithIssueDetailf(
			26443,
			"import non-public schema",
			"non-public schemas unsupported: %s", sc,
		)
	}
	return tn.Table(), nil
}

// getTableName variant for UnresolvedObjectName.
func getTableName2(u *tree.UnresolvedObjectName) (string, error) {
	if u.NumParts >= 2 && u.Parts[1] != "public" {
		return "", unimplemented.NewWithIssueDetailf(
			26443,
			"import non-public schema",
			"non-public schemas unsupported: %s", u.Parts[1],
		)
	}
	return u.Parts[0], nil
}

type pgDumpReader struct {
	tableDescs map[string]catalog.TableDescriptor
	tables     map[string]*row.DatumRowConverter
	descs      map[string]*execinfrapb.ReadImportDataSpec_ImportTable
	kvCh       chan row.KVBatch
	opts       roachpb.PgDumpOptions
	walltime   int64
	colMap     map[*row.DatumRowConverter](map[string]int)
	evalCtx    *tree.EvalContext
}

var _ inputConverter = &pgDumpReader{}

// newPgDumpReader creates a new inputConverter for pg_dump files.
func newPgDumpReader(
	ctx context.Context,
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
			tableDesc := tabledesc.NewImmutable(*table.Desc)
			colSubMap := make(map[string]int, len(table.TargetCols))
			targetCols := make(tree.NameList, len(table.TargetCols))
			for i, colName := range table.TargetCols {
				targetCols[i] = tree.Name(colName)
			}
			for i, col := range tableDesc.VisibleColumns() {
				colSubMap[col.Name] = i
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
			seqDesc := tabledesc.NewImmutable(*table.Desc)
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
	return readInputFiles(ctx, dataFiles, resumePos, format, m.readFile, makeExternalStorage, user)
}

func (m *pgDumpReader) readFile(
	ctx context.Context, input *fileReader, inputIdx int32, resumePos int64, rejected chan string,
) error {
	tableNameToRowsProcessed := make(map[string]int64)
	var inserts, count int64
	rowLimit := m.opts.RowLimit
	ps := newPostgreStream(input, int(m.opts.MaxRowSize))
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
			name, err := getTableName(n)
			if err != nil {
				return errors.Wrapf(err, "%s", i)
			}
			conv, ok := m.tables[name]
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
				return errors.Errorf("unsupported: %s", i.Rows.Select)
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
				tableNameToRowsProcessed[name]++
				if count <= resumePos {
					continue
				}
				if rowLimit != 0 && tableNameToRowsProcessed[name] > rowLimit {
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
			name, err := getTableName(&i.Table)
			if err != nil {
				return errors.Wrapf(err, "%s", i)
			}
			conv, importing := m.tables[name]
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
				tableNameToRowsProcessed[name]++
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
					if rowLimit != 0 && tableNameToRowsProcessed[name] > rowLimit {
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
									"parse %q as %s", col.Name, col.Type.SQLString())
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
				return errors.Errorf("unsupported %T Select: %v", i.Select, i.Select)
			}
			if len(sc.Exprs) != 1 {
				return errors.Errorf("unsupported %d select args: %v", len(sc.Exprs), sc.Exprs)
			}
			fn, ok := sc.Exprs[0].Expr.(*tree.FuncExpr)
			if !ok {
				return errors.Errorf("unsupported select arg %T: %v", sc.Exprs[0].Expr, sc.Exprs[0].Expr)
			}

			switch funcName := strings.ToLower(fn.Func.String()); funcName {
			case "search_path", "pg_catalog.set_config":
				continue
			case "setval", "pg_catalog.setval":
				if args := len(fn.Exprs); args < 2 || args > 3 {
					return errors.Errorf("unsupported %d fn args: %v", len(fn.Exprs), fn.Exprs)
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
					return errors.Errorf("unsupported setval %T arg: %v", fn.Exprs[1], fn.Exprs[1])
				}
				val, err := seqval.AsInt64()
				if err != nil {
					return errors.Wrap(err, "unsupported setval arg")
				}
				isCalled := false
				if len(fn.Exprs) == 3 {
					called, ok := fn.Exprs[2].(*tree.DBool)
					if !ok {
						return errors.Errorf("unsupported setval %T arg: %v", fn.Exprs[2], fn.Exprs[2])
					}
					isCalled = bool(*called)
				}
				name, err := parser.ParseTableName(seqname.RawString())
				if err != nil {
					break
				}
				seq := m.tableDescs[name.Parts[0]]
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
				return errors.Errorf("unsupported function: %s", funcName)
			}
		case *tree.SetVar, *tree.BeginTransaction, *tree.CommitTransaction, *tree.Analyze:
			// ignored.
		case *tree.CreateTable, *tree.AlterTable, *tree.AlterTableOwner, *tree.CreateIndex, *tree.CreateSequence, *tree.DropTable:
			// handled during schema extraction.
		case *tree.Delete:
			switch stmt := i.Table.(type) {
			case *tree.AliasedTableExpr:
				// ogr2ogr has `DELETE FROM geometry_columns / geography_columns ...` statements.
				// We're not planning to support this functionality in CRDB, so it is safe to ignore it when countered in PGDUMP.
				if tn, ok := stmt.Expr.(*tree.TableName); !(ok && (tn.Table() == "geometry_columns" || tn.Table() == "geography_columns")) {
					return errors.Errorf("unsupported DELETE FROM %T statement: %s", stmt, stmt)
				}
			default:
				return errors.Errorf("unsupported %T statement: %s", i, i)
			}
		default:
			return errors.Errorf("unsupported %T statement: %v", i, i)
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
