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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

type postgreStream struct {
	s    *bufio.Scanner
	max  int
	copy *postgreStreamCopy
}

// newPostgreStream returns a struct that can stream statements from an
// io.Reader.
func newPostgreStream(r io.Reader, max int) *postgreStream {
	s := bufio.NewScanner(r)
	s.Buffer(nil, max)
	p := &postgreStream{
		s: s,
	}
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

	sc := parser.MakeScanner(string(data))
	if pos := sc.Until(';'); pos > 0 {
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
		if err == errCopyDone {
			p.copy = nil
			return errCopyDone, nil
		}
		return row, err
	}

	for p.s.Scan() {
		stmts, err := parser.Parse(p.s.Text())
		if err != nil {
			return nil, err
		}
		switch len(stmts) {
		case 0:
			// Got whitespace or comments; try again.
		case 1:
			if cf, ok := stmts[0].(*tree.CopyFrom); ok && cf.Stdin {
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
			return stmts[0], nil
		default:
			return nil, errors.New("unexpected")
		}
	}
	if err := p.s.Err(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

// readPostgresCreateTable returns table descriptors for all tables or the
// matching table from SQL statements.
func readPostgresCreateTable(
	input io.Reader,
	evalCtx *tree.EvalContext,
	settings *cluster.Settings,
	match string,
	parentID sqlbase.ID,
	walltime int64,
) ([]*sqlbase.TableDescriptor, error) {
	// Modify the CreateTable stmt with the various index additions. We do this
	// instead of creating a full table descriptor first and adding indexes
	// later because MakeSimpleTableDescriptor calls the sql package which calls
	// AllocateIDs which adds the hidden rowid and default primary key. This means
	// we'd have to delete the index and row and modify the column family. This
	// is much easier and probably safer too.
	createTbl := make(map[string]*tree.CreateTable)
	ps := newPostgreStream(input, defaultScanBuffer)
	for {
		stmt, err := ps.Next()
		if err == io.EOF {
			ret := make([]*sqlbase.TableDescriptor, 0, len(createTbl))
			for _, create := range createTbl {
				if create != nil {
					id := sqlbase.ID(int(defaultCSVTableID) + len(ret))
					desc, err := MakeSimpleTableDescriptor(evalCtx.Ctx(), settings, create, parentID, id, walltime)
					if err != nil {
						return nil, err
					}
					ret = append(ret, desc)
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
			name, err := getTableName(stmt.Table)
			if err != nil {
				return nil, err
			}
			if match != "" && match != name {
				createTbl[name] = nil
			} else {
				createTbl[name] = stmt
			}
		case *tree.CreateIndex:
			name, err := getTableName(stmt.Table)
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
			name, err := getTableName(stmt.Table)
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
					create.Defs = append(create.Defs, cmd.ConstraintDef)
					continue
				default:
					return nil, errors.Errorf("unsupported statement: %s", stmt)
				}
			}
		}
	}
}

func getTableName(n tree.NormalizableTableName) (string, error) {
	tn, err := n.Normalize()
	if err != nil {
		return "", err
	}
	return tn.Table(), nil
}

type pgDumpReader struct {
	tables map[string]*rowConverter
	kvCh   chan kvBatch
}

var _ inputConverter = &pgDumpReader{}

// newPgDumpReader creates a new inputConverter for pg_dump files.
func newPgDumpReader(
	kvCh chan kvBatch, tables map[string]*sqlbase.TableDescriptor, evalCtx *tree.EvalContext,
) (*pgDumpReader, error) {
	converters := make(map[string]*rowConverter, len(tables))
	for name, table := range tables {
		conv, err := newRowConverter(table, evalCtx, kvCh)
		if err != nil {
			return nil, err
		}
		converters[name] = conv
	}
	return &pgDumpReader{kvCh: kvCh, tables: converters}, nil
}

func (m *pgDumpReader) start(ctx ctxgroup.Group) {
}

func (m *pgDumpReader) inputFinished(ctx context.Context) {
	close(m.kvCh)
}

func (m *pgDumpReader) readFile(
	ctx context.Context, input io.Reader, inputIdx int32, inputName string, progressFn progressFn,
) error {
	var inserts, count int64
	ps := newPostgreStream(input, defaultScanBuffer)
	semaCtx := &tree.SemaContext{}
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
			n, ok := i.Table.(*tree.NormalizableTableName)
			if !ok {
				return errors.Errorf("unexpected: %T", i.Table)
			}
			name, err := getTableName(*n)
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
			for _, tuple := range values.Tuples {
				count++
				if expected, got := len(conv.visibleCols), len(tuple.Exprs); expected != got {
					return errors.Errorf("expected %d values, got %d: %v", expected, got, tuple)
				}
				for i, expr := range tuple.Exprs {
					typed, err := expr.TypeCheck(semaCtx, conv.visibleColTypes[i])
					if err != nil {
						return errors.Wrapf(err, "reading row %d (%d in insert statement %d)",
							count, count-startingCount, inserts)
					}
					converted, err := typed.Eval(conv.evalCtx)
					if err != nil {
						return errors.Wrapf(err, "reading row %d (%d in insert statement %d)",
							count, count-startingCount, inserts)
					}
					conv.datums[i] = converted
				}
				if err := conv.row(ctx, inputIdx, count); err != nil {
					return err
				}
			}
		case *tree.CopyFrom:
			if !i.Stdin {
				return errors.New("expected STDIN option on COPY FROM")
			}
			name, err := getTableName(i.Table)
			if err != nil {
				return errors.Wrapf(err, "%s", i)
			}
			conv, importing := m.tables[name]
			if importing && conv == nil {
				return errors.Errorf("missing schema info for requested table %q", name)
			}
			if conv != nil {
				if expected, got := len(conv.visibleCols), len(i.Columns); expected != got {
					return errors.Errorf("expected %d values, got %d", expected, got)
				}
				for colI, col := range i.Columns {
					if string(col) != conv.visibleCols[colI].Name {
						return errors.Errorf("COPY columns do not match table columns for table %s", name)
					}
				}
			}
			for {
				row, err := ps.Next()
				// We expect an explicit copyDone here. io.EOF is unexpected.
				if err == io.EOF {
					return makeRowErr(inputName, count, "unexpected EOF")
				}
				if row == errCopyDone {
					break
				}
				count++
				if err != nil {
					return makeRowErr(inputName, count, "%s", err)
				}
				if !importing {
					continue
				}
				switch row := row.(type) {
				case copyData:
					for i, s := range row {
						if s == nil {
							conv.datums[i] = tree.DNull
						} else {
							conv.datums[i], err = tree.ParseDatumStringAs(conv.visibleColTypes[i], *s, conv.evalCtx)
							if err != nil {
								col := conv.visibleCols[i]
								return makeRowErr(inputName, count, "parse %q as %s: %s:", col.Name, col.Type.SQLString(), err)
							}
						}
					}
					if err := conv.row(ctx, inputIdx, count); err != nil {
						return err
					}
				default:
					return makeRowErr(inputName, count, "unexpected: %v", row)
				}
			}
		default:
			if log.V(3) {
				log.Infof(ctx, "ignoring %T stmt: %v", i, i)
			}
			continue
		}
	}
	for _, conv := range m.tables {
		if err := conv.sendBatch(ctx); err != nil {
			return err
		}
	}
	return nil
}
