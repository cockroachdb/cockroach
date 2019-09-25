// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// cr2pg is a program that reads CockroachDB-formatted SQL files on stdin,
// modifies them to be Postgres compatible, and outputs them to stdout.
package main

import (
	"bufio"
	"context"
	"io"
	"log"
	"os"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx := context.Background()
	g, ctx := errgroup.WithContext(ctx)
	done := ctx.Done()

	readStmts := make(chan tree.Statement, 100)
	writeStmts := make(chan tree.Statement, 100)
	// Divide up work between parsing, filtering, and writing.
	g.Go(func() error {
		defer close(readStmts)
		const maxStatementSize = 1024 * 1024 * 32
		stream := newSQLStream(os.Stdin, maxStatementSize)
		for {
			stmt, err := stream.next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			select {
			case readStmts <- stmt:
			case <-done:
				return nil
			}
		}
		return nil
	})
	g.Go(func() error {
		defer close(writeStmts)
		newstmts := make([]tree.Statement, 8)
		for stmt := range readStmts {
			newstmts = newstmts[:1]
			newstmts[0] = stmt
			switch stmt := stmt.(type) {
			case *tree.CreateTable:
				stmt.Interleave = nil
				stmt.PartitionBy = nil
				var newdefs tree.TableDefs
				for _, def := range stmt.Defs {
					switch def := def.(type) {
					case *tree.FamilyTableDef:
						// skip
					case *tree.IndexTableDef:
						// Postgres doesn't support
						// indexes in CREATE TABLE,
						// so split them out to their
						// own statement.
						newstmts = append(newstmts, &tree.CreateIndex{
							Name:     def.Name,
							Table:    stmt.Table,
							Inverted: def.Inverted,
							Columns:  def.Columns,
							Storing:  def.Storing,
						})
					case *tree.UniqueConstraintTableDef:
						if def.PrimaryKey {
							// Postgres doesn't support descending PKs.
							for i, col := range def.Columns {
								if col.Direction != tree.Ascending {
									return errors.New("PK directions not supported by postgres")
								}
								def.Columns[i].Direction = tree.DefaultDirection
							}
							// Unset Name here because
							// constaint names cannot
							// be shared among tables,
							// so multiple PK constraints
							// named "primary" is an error.
							def.Name = ""
							newdefs = append(newdefs, def)
							break
						}
						newstmts = append(newstmts, &tree.CreateIndex{
							Name:     def.Name,
							Table:    stmt.Table,
							Unique:   true,
							Inverted: def.Inverted,
							Columns:  def.Columns,
							Storing:  def.Storing,
						})
					default:
						newdefs = append(newdefs, def)
					}
				}
				stmt.Defs = newdefs
			}
			for _, stmt := range newstmts {
				select {
				case writeStmts <- stmt:
				case <-done:
					return nil
				}
			}
		}
		return nil
	})
	g.Go(func() error {
		w := bufio.NewWriterSize(os.Stdout, 1024*1024)
		fmtctx := tree.NewFmtCtx(tree.FmtSimple)
		for stmt := range writeStmts {
			stmt.Format(fmtctx)
			_, _ = w.WriteString(fmtctx.CloseAndGetString())
			_, _ = w.WriteString(";\n\n")
		}
		w.Flush()
		return nil
	})
	if err := g.Wait(); err != nil {
		log.Fatal(err)
	}
}

// sqlStream converts an io.Reader into a stream of tree.Statement. Modified
// from importccl/read_import_pgdump.go.
type sqlStream struct {
	s *bufio.Scanner
}

// newSQLStream returns a struct that can stream statements from an
// io.Reader.
func newSQLStream(r io.Reader, max int) *sqlStream {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 0, max), max)
	p := &sqlStream{s: s}
	s.Split(splitSQLSemicolon)
	return p
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

func (p *sqlStream) next() (tree.Statement, error) {
	for p.s.Scan() {
		t := p.s.Text()
		stmts, err := parser.Parse(t)
		if err != nil {
			return nil, err
		}
		switch len(stmts) {
		case 0:
			// Got whitespace or comments; try again.
		case 1:
			return stmts[0].AST, nil
		default:
			return nil, errors.Errorf("unexpected: got %d statements", len(stmts))
		}
	}
	if err := p.s.Err(); err != nil {
		if err == bufio.ErrTooLong {
			err = errors.New("line too long")
		}
		return nil, err
	}
	return nil, io.EOF
}
