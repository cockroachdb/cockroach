// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

type importHook interface {
	Done() bool
	Next() (original string, attempt string)
	Err() error
	Close()
}

type statementSummary struct {
	numAudit  int
	numDanger int
}

func summarizeStatements(stmts []SingleStatement) statementSummary {
	var ret statementSummary
	for _, stmt := range stmts {
		for _, iss := range stmt.Issues {
			switch iss.Level {
			case "info":
				ret.numAudit++
			default:
				ret.numDanger++
			}
		}
	}
	return ret
}

func (m *ImportMetadata) CombineStatements() {
	stmts := m.Statements[:0]
	type tn struct {
		schema string
		table  string
	}
	crdbTables := make(map[tn]int)

	for _, mStmt := range m.Statements {
		if mStmt.parsed != nil {
			switch stmt := mStmt.parsed.AST.(type) {
			case *tree.CreateTable:
				k := tn{schema: stmt.Table.Schema(), table: stmt.Table.Table()}
				crdbTables[k] = len(stmts)
			/*case *tree.CreateIndex:
			  k := tn{schema: stmt.Table.Schema(), table: stmt.Table.Table()}
			  // TODO: need to think about ordering here.
			  if idx, ok := crdbTables[k]; ok {
			  	stmts[idx].Original += "\n-- combined CREATE INDEX statement\n" + mStmt.Original
			  	p := stmts[idx].parsed.AST.(*tree.CreateTable)
			  	p.Defs = append(p.Defs, &tree.IndexTableDef{
			  		Name:             stmt.Name,
			  		Columns:          stmt.Columns,
			  		Sharded:          stmt.Sharded,
			  		Storing:          stmt.Storing,
			  		Inverted:         stmt.Inverted,
			  		PartitionByIndex: stmt.PartitionByIndex,
			  		StorageParams:    stmt.StorageParams,
			  		Predicate:        stmt.Predicate,
			  	})
			  	cfg := tree.DefaultPrettyCfg()
			  	stmts[idx].Cockroach = cfg.Pretty(p)
			  	stmts[idx].Issues = append(stmts[idx].Issues, mStmt.Issues...)
			  	continue
			  }*/
			case *tree.AlterTable:
				if len(stmt.Cmds) == 1 {
					switch cmd := stmt.Cmds[0].(type) {
					// Cannot add all statements in case of, e.g., circular dependencies.
					case *tree.AlterTableAddConstraint:
						switch def := cmd.ConstraintDef.(type) {
						case *tree.UniqueConstraintTableDef:
							if def.PrimaryKey {
								k := tn{schema: stmt.Table.Schema(), table: stmt.Table.Object()}
								if idx, ok := crdbTables[k]; ok {
									stmts[idx].Original += "\n-- combined PRIMARY KEY statement\n" + mStmt.Original
									p := stmts[idx].parsed.AST.(*tree.CreateTable)
									p.Defs = append(p.Defs, def)
									cfg := tree.DefaultPrettyCfg()
									stmts[idx].Cockroach = cfg.Pretty(p)
									stmts[idx].Issues = append(stmts[idx].Issues, mStmt.Issues...)
								}
							}
							continue
						}
					}
				}
			}
		}
		stmts = append(stmts, mStmt)
	}
	m.Statements = stmts
}

var sanitizeDBRe = regexp.MustCompile("[^a-zA-Z0-9]+")

var undefinedUserRegex = regexp.MustCompile(`role/user "([^"]*)" does not exist`)

func attemptImport(
	ctx context.Context, conn *pgx.Conn, pgURL string, id string, h importHook,
) error {
	defer h.Close()

	var meta ImportMetadata

	for !h.Done() {
		original, attempt := h.Next()
		stmts, err := readSingleStatement(original, attempt)
		if err != nil {
			return errors.Wrapf(err, "error parsing statement")
		}
		for _, stmt := range stmts {
			if stmt.Original == "" {
				continue
			}
			meta.Statements = append(meta.Statements, stmt)
		}
	}
	if err := h.Err(); err != nil {
		return errors.Wrapf(err, "error from hook")
	}

	// Attempt to combine statements.
	meta.CombineStatements()

	partialSanitize := sanitizeDBRe.ReplaceAllString(strings.Split(id, ".")[0], "")
	dbName := fmt.Sprintf("temp_%s_%d", partialSanitize, time.Now().UnixNano())
	meta.Database = dbName
	if err := func() error {
		// attempt on new connection
		conn, err := pgx.Connect(ctx, pgURL)
		if err != nil {
			return errors.Wrap(err, "error connecting to CockroachDB")
		}
		defer func() {
			_ = conn.Close(ctx)
		}()

		// TODO: sanitize
		if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName)); err != nil {
			return errors.Wrapf(err, "error creating temporary DB")
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf("USE %s", dbName)); err != nil {
			return errors.Wrapf(err, "error creating temporary DB")
		}
		// Attempt to import every parsable CockroachDB statement.
		for idx, stmt := range meta.Statements {
			if strings.TrimSpace(stmt.Cockroach) == "" {
				continue
			}
			if stmt.parsed == nil {
				continue
			}
			fmt.Printf("executing %s\n", stmt.Cockroach)
			if _, err := conn.Exec(ctx, stmt.Cockroach); err != nil {
				typ := "unknown"
				msg := []string{err.Error()}
				identifier := ""
				if err, ok := err.(*pgconn.PgError); ok {
					switch pgcode.MakeCode(err.Code) {
					case pgcode.FeatureNotSupported:
						typ = "unimplemented"
					case pgcode.UndefinedObject:
						if matches := undefinedUserRegex.FindStringSubmatch(err.Message); len(matches) >= 2 {
							identifier = matches[1]
							typ = "missing_user"
						}
					}
					msg = append(msg, err.Hint)
				}
				iss := Issue{
					Level:      "danger",
					Type:       typ,
					Identifier: identifier,
					Text:       strings.Join(msg, " "),
				}
				meta.Statements[idx].Issues = append(
					meta.Statements[idx].Issues,
					iss,
				)
			}
		}
		return nil
	}(); err != nil {
		return err
	}

	if summ := summarizeStatements(meta.Statements); summ.numDanger > 0 {
		meta.Status = "danger"
		meta.Message = fmt.Sprintf("at least one error found")
		/*if _, err := conn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName)); err != nil {
			return errors.Wrapf(err, "error dropping temporary DB")
		}*/
	} else if summ.numAudit > 0 {
		meta.Status = "warning"
		meta.Message = fmt.Sprintf("at least one audit required")
	} else {
		if _, err := conn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", partialSanitize)); err != nil {
			return errors.Wrapf(err, "error dropping existing DB")
		}
		meta.Database = partialSanitize

		if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER DATABASE %s RENAME TO %s", dbName, partialSanitize)); err != nil {
			meta.Status = "danger"
			meta.Message = fmt.Sprintf("error renaming db: %s", err)
		} else {
			meta.Status = "success"
			meta.Message = fmt.Sprintf("import complete! %d statements executed. database: %s", len(meta.Statements), partialSanitize)
		}
	}

	j, err := json.Marshal(meta)
	if err != nil {
		return errors.Wrapf(err, "error marshalling metadata")
	}

	if _, err := conn.Exec(
		ctx,
		`INSERT INTO defaultdb.public.import_meta (id, ts, data) VALUES ($1, now(), $2)`,
		id,
		string(j),
	); err != nil {
		return errors.Wrapf(err, "error inserting metadata")
	}

	return nil
}

func readSingleStatement(original, attempt string) ([]SingleStatement, error) {
	if len(attempt) == 0 {
		return []SingleStatement{{
			Original:  original,
			Cockroach: "",
		}}, nil
	}
	stmts, err := parser.Parse(attempt)
	if err != nil {
		errText := []string{err.Error()}
		errText = append(errText, errors.GetAllHints(err)...)
		iss := Issue{
			Level: "danger",
			Type:  "unknown",
			Text:  strings.Join(errText, " "),
		}
		if unsupportedErr := (*tree.UnsupportedError)(nil); errors.As(err, &unsupportedErr) {
			iss.Type = "unimplemented"
		}

		// TODO: find issue for unsupported error; hyperlink?
		return []SingleStatement{{
			Original:  original,
			Cockroach: attempt,
			Issues:    []Issue{iss},
		}}, nil
	}

	var ret []SingleStatement
	for i, stmt := range stmts {
		var issues []Issue
		switch stmt := stmt.AST.(type) {
		case *tree.CopyFrom, *tree.Insert:
			continue
		case *tree.CreateTable:
			// If the target table columns have data type INT or INTEGER, they need to
			// be updated to conform to the session variable `default_int_size`.
			for _, def := range stmt.Defs {
				if d, ok := def.(*tree.ColumnTableDef); ok {
					if d.HasDefaultExpr() {
						// gross but i'm too lazy to write the visitor
						if strings.HasPrefix(d.DefaultExpr.Expr.String(), "nextval(") {
							issues = append(
								issues,
								Issue{
									Level:      "info",
									Type:       "sequence",
									Identifier: string(d.Name),
									Text:       fmt.Sprintf("Column %s uses a sequence, whereas the preference is for UUIDs.", d.Name),
								},
							)
						}
					}
					/*
						if dType, ok := d.Type.(*types.T); ok {
							if dType.Equivalent(types.Int) && !isSeq {
								issues = append(
									issues,
									Issue{
										Level:      "info",
										Type:       "int_default",
										Identifier: string(d.Name),
										Text:       fmt.Sprintf("The INT for column %s will be replaced with INT8 (defaults as INT4 in PostgreSQL). You can mark these explicitly with INT4 or INT8, or leave it to utilize the default.", d.Name),
									},
								)
							}
						}*/
				}
			}
		}
		if i > 0 {
			original = "-- repeat as multiple statements found\n" + original
		}
		cockroach := attempt // TODO: maybe try format this!
		if len(stmts) > 1 {
			cfg := tree.DefaultPrettyCfg()
			cockroach = cfg.Pretty(stmt.AST)
		}
		ret = append(ret, SingleStatement{
			Original:  original,
			Cockroach: cockroach,
			Issues:    issues,
			parsed:    &stmts[i],
		})
	}

	return ret, nil
}
