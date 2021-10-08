// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlsmith

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
			_, _ = w.Write(b)
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

func makeAsOf(s *Smither) tree.AsOfClause {
	var expr tree.Expr
	switch s.rnd.Intn(10) {
	case 1:
		expr = tree.NewStrVal("-2s")
	case 2:
		expr = tree.NewStrVal(timeutil.Now().Add(-2 * time.Second).Format(timeutil.FullTimeFormat))
	case 3:
		expr = randgen.RandDatum(s.rnd, types.Interval, false /* nullOk */)
	case 4:
		datum := randgen.RandDatum(s.rnd, types.Timestamp, false /* nullOk */)
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

func makeBackup(s *Smither) (tree.Statement, bool) {
	name := fmt.Sprintf("%s/%s", s.bulkSrv.URL, s.name("backup"))
	var targets tree.TargetList
	seen := map[tree.TableName]bool{}
	for len(targets.Tables) < 1 || s.coin() {
		table, ok := s.getRandTable()
		if !ok {
			return nil, false
		}
		if seen[*table.TableName] {
			continue
		}
		seen[*table.TableName] = true
		targets.Tables = append(targets.Tables, table.TableName)
	}
	s.lock.Lock()
	s.bulkBackups[name] = targets
	s.lock.Unlock()

	return &tree.Backup{
		Targets: &targets,
		To:      tree.StringOrPlaceholderOptList{tree.NewStrVal(name)},
		AsOf:    makeAsOf(s),
		Options: tree.BackupOptions{CaptureRevisionHistory: s.coin()},
	}, true
}

func makeRestore(s *Smither) (tree.Statement, bool) {
	var name string
	var targets tree.TargetList
	s.lock.Lock()
	for name, targets = range s.bulkBackups {
		break
	}
	// Only restore each backup once.
	delete(s.bulkBackups, name)
	s.lock.Unlock()

	if name == "" {
		return nil, false
	}
	// Choose some random subset of tables.
	s.rnd.Shuffle(len(targets.Tables), func(i, j int) {
		targets.Tables[i], targets.Tables[j] = targets.Tables[j], targets.Tables[i]
	})
	targets.Tables = targets.Tables[:1+s.rnd.Intn(len(targets.Tables))]

	db := s.name("db")
	if _, err := s.db.Exec(fmt.Sprintf(`CREATE DATABASE %s`, db)); err != nil {
		return nil, false
	}

	return &tree.Restore{
		Targets: targets,
		From:    []tree.StringOrPlaceholderOptList{{tree.NewStrVal(name)}},
		AsOf:    makeAsOf(s),
		Options: tree.RestoreOptions{
			IntoDB: tree.NewStrVal("into_db"),
		},
	}, true
}

const exportSchema = "/create.sql"

func makeExport(s *Smither) (tree.Statement, bool) {
	// TODO(mjibson): Although this code works, in practice these
	// exports will almost always be empty because the tables it
	// chooses are also usually empty. Figure out a way to either
	// encourage more INSERTS or force them before an EXPORT is executed.

	if !s.bulkIOEnabled() {
		return nil, false
	}
	table, ok := s.getRandTable()
	if !ok {
		return nil, false
	}
	// Get the schema so we can save it for IMPORT. Since the EXPORT
	// is executed at some later time, there's no guarantee that this
	// schema will be the same one as in the export. A column could
	// be added or dropped by another parallel go routine.
	var schema string
	if err := s.db.QueryRow(fmt.Sprintf(
		`select create_statement from [show create table %s]`,
		table.TableName.String(),
	)).Scan(&schema); err != nil {
		return nil, false
	}
	stmt := &tree.Select{
		Select: &tree.SelectClause{
			Exprs:       tree.SelectExprs{tree.StarSelectExpr()},
			From:        tree.From{Tables: tree.TableExprs{table.TableName}},
			TableSelect: true,
		},
	}
	exp := s.name("exp")
	name := fmt.Sprintf("%s/%s", s.bulkSrv.URL, exp)
	s.lock.Lock()
	s.bulkFiles[fmt.Sprintf("/%s%s", exp, exportSchema)] = []byte(schema)
	s.bulkExports = append(s.bulkExports, string(exp))
	s.lock.Unlock()

	return &tree.Export{
		Query:      stmt,
		FileFormat: "CSV",
		File:       tree.NewStrVal(name),
	}, true
}

var importCreateTableRE = regexp.MustCompile(`CREATE TABLE (.*) \(`)

func makeImport(s *Smither) (tree.Statement, bool) {
	if !s.bulkIOEnabled() {
		return nil, false
	}

	s.lock.Lock()
	if len(s.bulkExports) == 0 {
		s.lock.Unlock()
		return nil, false
	}
	exp := s.bulkExports[0]
	s.bulkExports = s.bulkExports[1:]

	// Find all CSV files created by the EXPORT.
	var files tree.Exprs
	for name := range s.bulkFiles {
		if strings.Contains(name, exp+"/") && !strings.HasSuffix(name, exportSchema) {
			files = append(files, tree.NewStrVal(s.bulkSrv.URL+name))
		}
	}
	s.lock.Unlock()
	// An empty table will produce an EXPORT with zero files.
	if len(files) == 0 {
		return nil, false
	}

	// Fix the table name in the existing schema file.
	tab := s.name("tab")
	s.lock.Lock()
	schema := fmt.Sprintf("/%s%s", exp, exportSchema)
	s.bulkFiles[schema] = importCreateTableRE.ReplaceAll(
		s.bulkFiles[schema],
		[]byte(fmt.Sprintf("CREATE TABLE %s (", tab)),
	)
	s.lock.Unlock()

	return &tree.Import{
		Table:      tree.NewUnqualifiedTableName(tab),
		CreateFile: tree.NewStrVal(s.bulkSrv.URL + schema),
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
