// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlsmith

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"time"

	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func (s *Smither) bulkIOEnabled() bool {
	return s.bulkSrv != nil
}

// enableBulkIO enables bulk IO statements. The smither database must pass
// the CCL check (either it must have a license or the check is disabled
// in a test). It works by starting an in-memory fileserver to hold the
// data for backups and exports.
func (s *Smither) enableBulkIO() {
	s.bulkSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.lock.Lock()
		defer s.lock.Unlock()
		localfile := r.URL.Path
		switch r.Method {
		case "PUT":
			b, err := ioutil.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			s.bulkFiles[localfile] = b
			w.WriteHeader(201)
		case "GET", "HEAD":
			b, ok := s.bulkFiles[localfile]
			if !ok {
				http.Error(w, fmt.Sprintf("not found: %s", localfile), 404)
				return
			}
			w.Write(b)
		case "DELETE":
			delete(s.bulkFiles, localfile)
			w.WriteHeader(204)
		default:
			http.Error(w, "unsupported method", 400)
		}
	}))
	s.bulkFiles = map[string][]byte{}
	s.bulkBackups = map[string]tree.TargetList{}
}

func makeAsOf(s *scope) tree.AsOfClause {
	var expr tree.Expr
	switch s.schema.rnd.Intn(10) {
	case 1:
		expr = tree.NewStrVal("-2s")
	case 2:
		expr = tree.NewStrVal(timeutil.Now().Add(-2 * time.Second).Format(tree.TimestampOutputFormat))
	case 3:
		coltype := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INTERVAL}
		expr = sqlbase.RandDatum(s.schema.rnd, coltype, false /* nullOk */)
	case 4:
		coltype := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_TIMESTAMP}
		datum := sqlbase.RandDatum(s.schema.rnd, coltype, false /* nullOk */)
		str := strings.TrimSuffix(datum.String(), `+00:00'`)
		str = strings.TrimPrefix(str, `'`)
		expr = tree.NewStrVal(str)
	default:
		// Most of the time leave this empty.
	}
	return tree.AsOfClause{
		Expr: expr,
	}
}

func makeBackup(s *scope) (tree.Statement, bool) {
	name := fmt.Sprintf("%s/%s", s.schema.bulkSrv.URL, s.schema.name("backup"))
	var targets tree.TargetList
	seen := map[tree.TableName]bool{}
	for len(targets.Tables) < 1 || coin() {
		table, ok := s.schema.getRandTable()
		if !ok {
			return nil, false
		}
		if seen[*table.TableName] {
			continue
		}
		seen[*table.TableName] = true
		targets.Tables = append(targets.Tables, table.TableName)
	}
	s.schema.lock.Lock()
	s.schema.bulkBackups[name] = targets
	s.schema.lock.Unlock()

	var opts tree.KVOptions
	if coin() {
		opts = tree.KVOptions{
			tree.KVOption{
				Key: "revision_history",
			},
		}
	}

	return &tree.Backup{
		Targets: targets,
		To:      tree.NewStrVal(name),
		AsOf:    makeAsOf(s),
		Options: opts,
	}, true
}

func makeRestore(s *scope) (tree.Statement, bool) {
	var name string
	var targets tree.TargetList
	s.schema.lock.Lock()
	for name, targets = range s.schema.bulkBackups {
		break
	}
	// Only restore each backup once.
	delete(s.schema.bulkBackups, name)
	s.schema.lock.Unlock()

	if name == "" {
		return nil, false
	}
	// Choose some random subset of tables.
	s.schema.rnd.Shuffle(len(targets.Tables), func(i, j int) {
		targets.Tables[i], targets.Tables[j] = targets.Tables[j], targets.Tables[i]
	})
	targets.Tables = targets.Tables[:1+s.schema.rnd.Intn(len(targets.Tables))]

	db := s.schema.name("db")
	if _, err := s.schema.db.Exec(fmt.Sprintf(`CREATE DATABASE %s`, db)); err != nil {
		panic(err)
		return nil, false
	}

	return &tree.Restore{
		Targets: targets,
		From:    tree.Exprs{tree.NewStrVal(name)},
		AsOf:    makeAsOf(s),
		Options: tree.KVOptions{
			tree.KVOption{
				Key:   "into_db",
				Value: tree.NewStrVal(string(db)),
			},
		},
	}, true
}

const exportSchema = "/create.sql"

func makeExport(s *scope) (tree.Statement, bool) {
	// TODO(mjibson): Although this code works, in practice these
	// exports will almost always be empty because the tables it
	// chooses are also usually empty. Figure out a way to either
	// encourage more INSERTS or force them before an EXPORT is executed.

	if !s.schema.bulkIOEnabled() {
		return nil, false
	}
	table, ok := s.schema.getRandTable()
	if !ok {
		return nil, false
	}
	// Get the schema so we can save it for IMPORT. Since the EXPORT
	// is executed at some later time, there's no guarantee that this
	// schema will be the same one as in the export. A column could
	// be added or dropped by another parallel go routine.
	var schema string
	if err := s.schema.db.QueryRow(fmt.Sprintf(
		`select create_statement from [show create table %s]`,
		table.TableName.String(),
	)).Scan(&schema); err != nil {
		return nil, false
	}
	stmt := &tree.Select{
		Select: &tree.SelectClause{
			Exprs:       tree.SelectExprs{tree.StarSelectExpr()},
			From:        &tree.From{Tables: tree.TableExprs{table.TableName}},
			TableSelect: true,
		},
	}
	exp := s.schema.name("exp")
	name := fmt.Sprintf("%s/%s", s.schema.bulkSrv.URL, exp)
	s.schema.lock.Lock()
	s.schema.bulkFiles[fmt.Sprintf("/%s%s", exp, exportSchema)] = []byte(schema)
	s.schema.bulkExports = append(s.schema.bulkExports, string(exp))
	s.schema.lock.Unlock()

	return &tree.Export{
		Query:      stmt,
		FileFormat: "CSV",
		File:       tree.NewStrVal(name),
	}, true
}

var importCreateTableRE = regexp.MustCompile(`CREATE TABLE (.*) \(`)

func makeImport(s *scope) (tree.Statement, bool) {
	if !s.schema.bulkIOEnabled() {
		return nil, false
	}

	s.schema.lock.Lock()
	if len(s.schema.bulkExports) == 0 {
		s.schema.lock.Unlock()
		return nil, false
	}
	exp := s.schema.bulkExports[0]
	s.schema.bulkExports = s.schema.bulkExports[1:]

	// Find all CSV files created by the EXPORT.
	var files tree.Exprs
	for name := range s.schema.bulkFiles {
		if strings.Contains(name, exp+"/") && !strings.HasSuffix(name, exportSchema) {
			files = append(files, tree.NewStrVal(s.schema.bulkSrv.URL+name))
		}
	}
	s.schema.lock.Unlock()
	// An empty table will produce an EXPORT with zero files.
	if len(files) == 0 {
		return nil, false
	}

	// Fix the table name in the existing schema file.
	tab := s.schema.name("tab")
	s.schema.lock.Lock()
	schema := fmt.Sprintf("/%s%s", exp, exportSchema)
	s.schema.bulkFiles[schema] = importCreateTableRE.ReplaceAll(
		s.schema.bulkFiles[schema],
		[]byte(fmt.Sprintf("CREATE TABLE %s (", tab)),
	)
	s.schema.lock.Unlock()

	return &tree.Import{
		Table:      tree.NewUnqualifiedTableName(tab),
		CreateFile: tree.NewStrVal(s.schema.bulkSrv.URL + schema),
		FileFormat: "CSV",
		Files:      files,
		Options: tree.KVOptions{
			tree.KVOption{
				Key:   "nullif",
				Value: tree.NewStrVal(""),
			},
		},
	}, true
}
