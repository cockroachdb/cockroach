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

	"github.com/cockroachdb/cockroach/pkg/cmd/cr2pg/sqlstream"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
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
		stream := sqlstream.NewStream(os.Stdin)
		for {
			stmt, err := stream.Next()
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
				stmt.PartitionByTable = nil
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
