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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
		stmts, err := parser.Parse(t)
		if err != nil {
			// Something non-parseable may be something we don't yet parse but still
			// want to ignore.
			if isIgnoredStatement(t) {
				continue
			}
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
			err = errors.HandledWithMessage(err, "line too long")
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
		regexp.MustCompile("(?i)^alter table .* owner to"),
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
	p sql.PlanHookState,
	match string,
	parentID sqlbase.ID,
	walltime int64,
	fks fkHandler,
	max int,
) ([]*sqlbase.TableDescriptor, error) {
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
	params := p.RunParams(ctx)
	for {
		stmt, err := ps.Next()
		if err == io.EOF {
			ret := make([]*sqlbase.TableDescriptor, 0, len(createTbl))
			for name, seq := range createSeq {
				id := sqlbase.ID(int(defaultCSVTableID) + len(ret))
				desc, err := sql.MakeSequenceTableDesc(
					name,
					seq.Options,
					parentID,
					keys.PublicSchemaID,
					id,
					hlc.Timestamp{WallTime: walltime},
					sqlbase.NewDefaultPrivilegeDescriptor(),
					false, /* temporary */
					&params,
				)
				if err != nil {
					return nil, err
				}
				fks.resolver[desc.Name] = &desc
				ret = append(ret, desc.TableDesc())
			}
			backrefs := make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor)
			for _, create := range createTbl {
				if create == nil {
					continue
				}
				removeDefaultRegclass(create)
				id := sqlbase.ID(int(defaultCSVTableID) + len(ret))
				desc, err := MakeSimpleTableDescriptor(evalCtx.Ctx(), p.SemaCtx(), p.ExecCfg().Settings, create, parentID, id, fks, walltime)
				if err != nil {
					return nil, err
				}
				fks.resolver[desc.Name] = desc
				backrefs[desc.ID] = desc
				ret = append(ret, desc.TableDesc())
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
				if err := fixDescriptorFKState(desc.TableDesc()); err != nil {
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
		switch stmt := stmt.(type) {
		case *tree.CreateTable:
			name, err := getTableName(&stmt.Table)
			if err != nil {
				return nil, err
			}
			if match != "" && match != name {
				createTbl[name] = nil
			} else {
				createTbl[name] = stmt
			}
		case *tree.CreateIndex:
			name, err := getTableName(&stmt.Table)
			if err != nil {
				return nil, err
			}
			create := createTbl[name]
			if create == nil {
				break
			}
			var idx tree.TableDef = &tree.IndexTableDef{
				Name:        stmt.Name,
				Columns:     stmt.Columns,
				Storing:     stmt.Storing,
				Inverted:    stmt.Inverted,
				Interleave:  stmt.Interleave,
				PartitionBy: stmt.PartitionBy,
			}
			if stmt.Unique {
				idx = &tree.UniqueConstraintTableDef{IndexTableDef: *idx.(*tree.IndexTableDef)}
			}
			create.Defs = append(create.Defs, idx)
		case *tree.AlterTable:
			name, err := getTableName2(stmt.Table)
			if err != nil {
				return nil, err
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
					for i, def := range create.Defs {
						def, ok := def.(*tree.ColumnTableDef)
						if !ok || def.Name != cmd.Column {
							continue
						}
						def.DefaultExpr.Expr = cmd.Default
						create.Defs[i] = def
					}
				case *tree.AlterTableValidateConstraint:
					// ignore
				default:
					return nil, errors.Errorf("unsupported statement: %s", stmt)
				}
			}
		case *tree.CreateSequence:
			name, err := getTableName(&stmt.Name)
			if err != nil {
				return nil, err
			}
			if match == "" || match == name {
				createSeq[name] = stmt
			}
		}
	}
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
	tables map[string]*row.DatumRowConverter
	descs  map[string]*execinfrapb.ReadImportDataSpec_ImportTable
	kvCh   chan row.KVBatch
	opts   roachpb.PgDumpOptions
}

var _ inputConverter = &pgDumpReader{}

// newPgDumpReader creates a new inputConverter for pg_dump files.
func newPgDumpReader(
	ctx context.Context,
	kvCh chan row.KVBatch,
	opts roachpb.PgDumpOptions,
	descs map[string]*execinfrapb.ReadImportDataSpec_ImportTable,
	evalCtx *tree.EvalContext,
) (*pgDumpReader, error) {
	converters := make(map[string]*row.DatumRowConverter, len(descs))
	for name, table := range descs {
		if table.Desc.IsTable() {
			conv, err := row.NewDatumRowConverter(ctx, table.Desc, nil /* targetColNames */, evalCtx, kvCh)
			if err != nil {
				return nil, err
			}
			converters[name] = conv
		}
	}
	return &pgDumpReader{
		kvCh:   kvCh,
		tables: converters,
		descs:  descs,
		opts:   opts,
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
	user string,
) error {
	return readInputFiles(ctx, dataFiles, resumePos, format, m.readFile, makeExternalStorage, user)
}

func (m *pgDumpReader) readFile(
	ctx context.Context, input *fileReader, inputIdx int32, resumePos int64, rejected chan string,
) error {
	var inserts, count int64
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
			values, ok := i.Rows.Select.(*tree.ValuesClause)
			if !ok {
				return errors.Errorf("unsupported: %s", i.Rows.Select)
			}
			inserts++
			startingCount := count
			for _, tuple := range values.Rows {
				count++
				if count <= resumePos {
					continue
				}
				if expected, got := len(conv.VisibleCols), len(tuple); expected != got {
					return errors.Errorf("expected %d values, got %d: %v", expected, got, tuple)
				}
				for i, expr := range tuple {
					typed, err := expr.TypeCheck(ctx, &semaCtx, conv.VisibleColTypes[i])
					if err != nil {
						return errors.Wrapf(err, "reading row %d (%d in insert statement %d)",
							count, count-startingCount, inserts)
					}
					converted, err := typed.Eval(conv.EvalCtx)
					if err != nil {
						return errors.Wrapf(err, "reading row %d (%d in insert statement %d)",
							count, count-startingCount, inserts)
					}
					conv.Datums[i] = converted
				}
				if err := conv.Row(ctx, inputIdx, count); err != nil {
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
			if conv != nil {
				if expected, got := len(conv.VisibleCols), len(i.Columns); expected != got {
					return errors.Errorf("expected %d columns, got %d", expected, got)
				}
				for colI, col := range i.Columns {
					if string(col) != conv.VisibleCols[colI].Name {
						return errors.Errorf("COPY columns do not match table columns for table %s", name)
					}
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
					if expected, got := len(conv.VisibleCols), len(row); expected != got {
						return makeRowErr("", count, pgcode.Syntax,
							"expected %d values, got %d", expected, got)
					}
					for i, s := range row {
						if s == nil {
							conv.Datums[i] = tree.DNull
						} else {
							conv.Datums[i], err = sqlbase.ParseDatumStringAs(conv.VisibleColTypes[i], *s, conv.EvalCtx)
							if err != nil {
								col := conv.VisibleCols[i]
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
				break
			}
			if len(sc.Exprs) != 1 {
				break
			}
			fn, ok := sc.Exprs[0].Expr.(*tree.FuncExpr)
			if !ok || len(fn.Exprs) < 2 {
				break
			}
			if name := strings.ToLower(fn.Func.String()); name != "setval" && name != "pg_catalog.setval" {
				break
			}
			seqname, ok := fn.Exprs[0].(*tree.StrVal)
			if !ok {
				break
			}
			seqval, ok := fn.Exprs[1].(*tree.NumVal)
			if !ok {
				break
			}
			val, err := seqval.AsInt64()
			if err != nil {
				break
			}
			isCalled := false
			if len(fn.Exprs) > 2 {
				called, ok := fn.Exprs[2].(*tree.DBool)
				if !ok {
					break
				}
				isCalled = bool(*called)
			}
			name, err := parser.ParseTableName(seqname.RawString())
			if err != nil {
				break
			}
			seq := m.descs[name.Parts[0]]
			if seq == nil {
				break
			}
			key, val, err := sql.MakeSequenceKeyVal(keys.TODOSQLCodec, seq.Desc, val, isCalled)
			if err != nil {
				return wrapRowErr(err, "", count, pgcode.Uncategorized, "")
			}
			kv := roachpb.KeyValue{Key: key}
			kv.Value.SetInt(val)
			m.kvCh <- row.KVBatch{
				Source: inputIdx, KVs: []roachpb.KeyValue{kv}, Progress: input.ReadFraction(),
			}
		default:
			if log.V(3) {
				log.Infof(ctx, "ignoring %T stmt: %v", i, i)
			}
			continue
		}
	}
	for _, conv := range m.tables {
		if err := conv.SendBatch(ctx); err != nil {
			return err
		}
	}
	return nil
}
