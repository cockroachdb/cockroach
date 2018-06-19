// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
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
	r     io.Reader
	b     []byte
	queue []tree.Statement
	max   int
}

const (
	defaultPgStreamInit = 1024 * 64
	defaultPgStreamMax  = 1024 * 1024 * 10
)

// newPostgreStream returns a struct that can stream statements from an
// io.Reader by parsing chunks of statements at a time. initialCap is the
// initial buffer size. It will grow up to maxCap. maxCap must be larger
// than the expected largest statement.
func newPostgreStream(r io.Reader, initialCap, maxCap int) *postgreStream {
	return &postgreStream{
		r:   r,
		b:   make([]byte, 0, initialCap),
		max: maxCap,
	}
}

func (p *postgreStream) Next() (tree.Statement, error) {
	if len(p.queue) == 0 {
		sl, err := p.read()
		if err != nil {
			return nil, err
		}
		p.queue = sl
	}
	// len(p.queue) is guaranteed to be > 0 here because EOF would have been
	// returned earlier if nothing was read.
	s := p.queue[0]
	p.queue = p.queue[1:]
	return s, nil
}

func (p *postgreStream) read() (tree.StatementList, error) {
	for {
		// First attempt to read the possibly empty p.b.
		bs := string(p.b)
		s := parser.MakeScanner(bs)
		pos := s.Until(';')
		// Find the last semicolon.
		for pos != 0 {
			npos := s.Until(';')
			if npos != 0 {
				pos = npos
			} else {
				break
			}
		}
		// We found something. Shift over the unused p.b to its beginning using the
		// same underlying location and parse what we found.
		if pos != 0 {
			n := copy(p.b, p.b[pos:])
			p.b = p.b[:n]
			return parser.Parse(bs[:pos])
		}

		// We didn't find a semicolon. Need to read more into p.b. See if p.b already
		// has more cap space.
		start := len(p.b)
		sz := cap(p.b)
		// If len(b) == cap(b) then we need a bigger slice.
		if len(p.b) == cap(p.b) {
			sz = cap(p.b) * 2
			if sz > p.max {
				sz = p.max
			}
			if sz < cap(p.b) {
				sz = cap(p.b)
			}
		}
		p.b = append(p.b, make([]byte, sz-start)...)

		// Read in data after whatever was already there.
		n, err := io.ReadFull(p.r, p.b[start:])
		if err == io.ErrUnexpectedEOF {
			p.b = p.b[:start+n]
		} else if err != nil {
			return nil, err
		} else if n == 0 {
			return nil, errors.Errorf("buffer too small: %d", sz)
		}
	}
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
	ps := newPostgreStream(input, defaultPgStreamInit, defaultPgStreamMax)
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
	ps := newPostgreStream(input, defaultPgStreamInit, defaultPgStreamMax)
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
