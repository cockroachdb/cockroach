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
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4"
)

type importAttempt struct {
	ID             string         `json:"id"`
	UnixNano       int64          `json:"unix_nano"`
	ImportMetadata ImportMetadata `json:"import_metadata"`
}

func getHandler(pgURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := func() error {
			fmt.Printf("received get request\n")
			ctx := r.Context()

			conn, err := pgx.Connect(ctx, pgURL)
			if err != nil {
				return errors.Wrap(err, "error connecting to CockroachDB")
			}
			defer func() {
				_ = conn.Close(ctx)
				_ = r.Body.Close()
			}()

			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Access-Control-Allow-Origin", "*")

			q := "SELECT id, ts, data FROM import_meta ORDER BY ts DESC LIMIT 1"
			var params []interface{}
			if r.URL.Query().Has("id") && r.URL.Query().Get("id") != "" {
				q = "SELECT id, ts, data FROM import_meta WHERE id = $1 ORDER BY ts DESC LIMIT 1"
				id := r.URL.Query().Get("id")
				params = append(params, id)
				fmt.Printf("processing id %s\n", id)
			}
			// Just select the latest one...
			return writeOutput(conn, conn.QueryRow(ctx, q, params...), w)
		}(); err != nil {
			fmt.Printf("error: %v\n", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	}
}

func sqlHandler(pgURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := func() error {
			if r.Method == "OPTIONS" {
				headers := w.Header()
				headers.Add("Access-Control-Allow-Origin", "*")
				headers.Add("Vary", "Origin")
				headers.Add("Vary", "Access-Control-Request-Method")
				headers.Add("Vary", "Access-Control-Request-Headers")
				headers.Add("Access-Control-Allow-Headers", "Content-Type, Origin, Accept, token")
				headers.Add("Access-Control-Allow-Methods", "POST,OPTIONS")
				w.WriteHeader(http.StatusOK)
				return nil
			}

			ctx := r.Context()

			conn, err := pgx.Connect(ctx, pgURL)
			if err != nil {
				return err
			}
			defer func() {
				_ = conn.Close(ctx)
				_ = r.Body.Close()
			}()

			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Access-Control-Allow-Origin", "*")

			type reqJSON struct {
				Database string `json:"database"`
				SQL      string `json:"sql"`
			}

			var req reqJSON
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				return err
			}

			// SQL Injection Topia
			if _, err := conn.Exec(ctx, fmt.Sprintf("USE %s", req.Database)); err != nil {
				return err
			}

			type respJSON struct {
				Columns []string   `json:"columns"`
				Rows    [][]string `json:"rows"`
				Error   string     `json:"error"`
			}
			var resp respJSON

			rows, err := conn.Query(ctx, req.SQL)
			if err != nil {
				resp.Error = err.Error()
			} else {
				defer rows.Close()
				for _, d := range rows.FieldDescriptions() {
					resp.Columns = append(resp.Columns, string(d.Name))
				}
				for rows.Next() {
					var row []string
					vals, err := rows.Values()
					if err != nil {
						errText := []string{err.Error()}
						errText = append(errText, errors.GetAllHints(err)...)
						resp.Error = strings.Join(errText, " ")
						break
					}
					for _, v := range vals {
						row = append(row, fmt.Sprintf("%v", v))
					}
					resp.Rows = append(resp.Rows, row)
				}
				if err := rows.Err(); err != nil {
					resp.Error = err.Error()
				}
			}

			if err := json.NewEncoder(w).Encode(resp); err != nil {
				return err
			}
			return nil
		}(); err != nil {
			fmt.Printf("error: %v\n", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	}
}

func uploadFileHandler(pgURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := func() error {
			if r.Method == "OPTIONS" {
				headers := w.Header()
				headers.Add("Access-Control-Allow-Origin", "*")
				headers.Add("Vary", "Origin")
				headers.Add("Vary", "Access-Control-Request-Method")
				headers.Add("Vary", "Access-Control-Request-Headers")
				headers.Add("Access-Control-Allow-Headers", "Content-Type, Origin, Accept, token")
				headers.Add("Access-Control-Allow-Methods", "POST,OPTIONS")
				w.WriteHeader(http.StatusOK)
				return nil
			}

			fmt.Printf("received upload request\n")
			ctx := r.Context()

			conn, err := pgx.Connect(ctx, pgURL)
			if err != nil {
				return err
			}
			defer func() {
				_ = conn.Close(ctx)
				_ = r.Body.Close()
			}()

			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Access-Control-Allow-Origin", "*")

			if err := r.ParseMultipartForm(1 << 20); err != nil {
				return err
			}

			id := r.Form.Get("id")
			if id == "" {
				return errors.Newf("no id")
			}
			fileData, _, err := r.FormFile("file")
			if err != nil {
				return err
			}

			h, err := newFileHook(fileData, func() { _ = fileData.Close() })
			if err != nil {
				return err
			}
			if err := attemptImport(ctx, conn, *flagPGURL, id, h); err != nil {
				return err
			}

			return writeOutput(conn, conn.QueryRow(ctx, "SELECT id, ts, data FROM import_meta WHERE id = $1 ORDER BY ts DESC LIMIT 1", id), w)
		}(); err != nil {
			fmt.Printf("error: %v\n", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	}
}

func fixSerialHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := func() error {
			if r.Method == "OPTIONS" {
				headers := w.Header()
				headers.Add("Access-Control-Allow-Origin", "*")
				headers.Add("Vary", "Origin")
				headers.Add("Vary", "Access-Control-Request-Method")
				headers.Add("Vary", "Access-Control-Request-Headers")
				headers.Add("Access-Control-Allow-Headers", "Content-Type, Origin, Accept, token")
				headers.Add("Access-Control-Allow-Methods", "POST,OPTIONS")
				w.WriteHeader(http.StatusOK)
				return nil
			}

			defer func() {
				_ = r.Body.Close()
			}()

			var req struct {
				Statement SingleStatement `json:"statement"`
				ID        string          `json:"id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				return err
			}

			rets, err := readSingleStatement(req.Statement.Original, req.Statement.Cockroach)
			if err != nil {
				return err
			}
			if len(rets) > 1 {
				return errors.AssertionFailedf("multiple statements found: %s", req.Statement.Cockroach)
			}
			ret := rets[0]
			if ret.parsed != nil {
				changed := false

				switch stmt := ret.parsed.AST.(type) {
				case *tree.CreateTable:
					// If the target table columns have data type INT or INTEGER, they need to
					// be updated to conform to the session variable `default_int_size`.
					for _, def := range stmt.Defs {
						if d, ok := def.(*tree.ColumnTableDef); ok {
							if d.HasDefaultExpr() {
								if strings.HasPrefix(d.DefaultExpr.Expr.String(), "nextval(") {
									foundIdx := -1
									for i, iss := range ret.Issues {
										if string(d.Name) == req.ID && iss.Identifier == req.ID {
											foundIdx = i
										}
									}
									if foundIdx >= 0 {
										ret.Issues = append(ret.Issues[:foundIdx], ret.Issues[foundIdx+1:]...)
										d.DefaultExpr.Expr = &tree.FuncExpr{Func: tree.WrapFunction("gen_random_uuid")}
										d.Type = types.Uuid
										changed = true
									}
								}
							}
						}
					}
				}

				if changed {
					cfg := tree.DefaultPrettyCfg()
					cfg.Simplify = false
					cfg.UseTabs = false
					rets, err := readSingleStatement(req.Statement.Original, cfg.Pretty(ret.parsed.AST))
					if err != nil {
						return err
					}
					if len(rets) > 1 {
						return errors.AssertionFailedf("multiple statements found: %s", req.Statement.Cockroach)
					}
					ret = rets[0]
				}
			}

			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Access-Control-Allow-Origin", "*")

			if err := json.NewEncoder(w).Encode(ret); err != nil {
				return err
			}
			return nil
		}(); err != nil {
			fmt.Printf("error: %v\n", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	}
}

func retryHandler(pgURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := func() error {
			if r.Method == "OPTIONS" {
				headers := w.Header()
				headers.Add("Access-Control-Allow-Origin", "*")
				headers.Add("Vary", "Origin")
				headers.Add("Vary", "Access-Control-Request-Method")
				headers.Add("Vary", "Access-Control-Request-Headers")
				headers.Add("Access-Control-Allow-Headers", "Content-Type, Origin, Accept, token")
				headers.Add("Access-Control-Allow-Methods", "POST,OPTIONS")
				w.WriteHeader(http.StatusOK)
				return nil
			}

			fmt.Printf("received retry request\n")
			ctx := r.Context()

			conn, err := pgx.Connect(ctx, pgURL)
			if err != nil {
				return err
			}
			defer func() {
				_ = conn.Close(ctx)
				_ = r.Body.Close()
			}()

			var ia importAttempt
			if err := json.NewDecoder(r.Body).Decode(&ia); err != nil {
				return err
			}

			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Access-Control-Allow-Origin", "*")

			h, err := newJSONHook(ia.ImportMetadata)
			if err != nil {
				return err
			}
			if err := attemptImport(ctx, conn, pgURL, ia.ID, h); err != nil {
				return err
			}

			return writeOutput(conn, conn.QueryRow(ctx, "SELECT id, ts, data FROM import_meta WHERE id = $1 ORDER BY ts DESC LIMIT 1", ia.ID), w)
		}(); err != nil {
			fmt.Printf("error: %v\n", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	}
}

func writeOutput(conn *pgx.Conn, row pgx.Row, w http.ResponseWriter) error {
	var ret importAttempt
	var ts time.Time
	var im []byte
	if err := row.Scan(&ret.ID, &ts, &im); err != nil {
		return errors.Wrap(err, "failed to extract element")
	}
	if err := json.Unmarshal(im, &ret.ImportMetadata); err != nil {
		return errors.Wrap(err, "failed to marshal import metadata")
	}
	ret.UnixNano = ts.UnixNano()
	if err := json.NewEncoder(w).Encode(ret); err != nil {
		return errors.Wrap(err, "failed to encode json output")
	}
	return nil
}

type jsonHook struct {
	im                ImportMetadata
	stmtIdx           int
	original, attempt string
	err               error
}

func newJSONHook(im ImportMetadata) (importHook, error) {
	return &jsonHook{
		im:      im,
		stmtIdx: -1,
	}, nil
}

func (f *jsonHook) Done() bool {
	if f.err != nil {
		return true
	}
	f.stmtIdx++
	if f.stmtIdx >= len(f.im.Statements) {
		return true
	}
	f.original, f.attempt = f.im.Statements[f.stmtIdx].Original, f.im.Statements[f.stmtIdx].Cockroach
	return false
}

func (f *jsonHook) Next() (string, string) {
	return f.original, f.attempt
}

func (f *jsonHook) Err() error {
	return f.err
}

func (f *jsonHook) Close() {
}
