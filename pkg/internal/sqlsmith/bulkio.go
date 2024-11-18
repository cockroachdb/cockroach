// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlsmith

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
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
			b, err := io.ReadAll(r.Body)
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
	s.bulkBackups = map[string]tree.BackupTargetList{}
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
	if !s.bulkIOEnabled() {
		return nil, false
	}
	name := fmt.Sprintf("%s/%s", s.bulkSrv.URL, s.name("backup"))
	var targets tree.BackupTargetList
	seen := map[tree.TableName]bool{}
	for len(targets.Tables.TablePatterns) < 1 || s.coin() {
		table, ok := s.getRandTable()
		if !ok {
			return nil, false
		}
		if seen[*table.TableName] {
			continue
		}
		seen[*table.TableName] = true
		targets.Tables.TablePatterns = append(targets.Tables.TablePatterns, table.TableName)
	}
	s.lock.Lock()
	s.bulkBackups[name] = targets
	s.lock.Unlock()

	coinD := tree.DBoolFalse
	if s.coin() {
		coinD = tree.DBoolTrue
	}

	return &tree.Backup{
		Targets: &targets,
		To:      tree.StringOrPlaceholderOptList{tree.NewStrVal(name)},
		AsOf:    makeAsOf(s),
		Options: tree.BackupOptions{CaptureRevisionHistory: coinD},
	}, true
}

func makeRestore(s *Smither) (tree.Statement, bool) {
	var name string
	var targets tree.BackupTargetList
	func() {
		s.lock.Lock()
		defer s.lock.Unlock()
		// TODO(yuzefovich): picking a backup target here is non-deterministic.
		for name, targets = range s.bulkBackups {
			break
		}
		// Only restore each backup once.
		delete(s.bulkBackups, name)
	}()

	if name == "" {
		return nil, false
	}
	// Choose some random subset of tables.
	s.rnd.Shuffle(len(targets.Tables.TablePatterns), func(i, j int) {
		targets.Tables.TablePatterns[i], targets.Tables.TablePatterns[j] = targets.Tables.TablePatterns[j], targets.Tables.TablePatterns[i]
	})
	targets.Tables.TablePatterns = targets.Tables.TablePatterns[:1+s.rnd.Intn(len(targets.Tables.TablePatterns))]

	db := s.name("db")
	if _, err := s.db.Exec(fmt.Sprintf(`CREATE DATABASE %s`, db)); err != nil {
		return nil, false
	}

	return &tree.Restore{
		Targets: targets,
		Subdir:  tree.NewStrVal("LATEST"),
		From:    tree.StringOrPlaceholderOptList{tree.NewStrVal(name)},
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
	defer s.lock.Unlock()
	s.bulkFiles[fmt.Sprintf("/%s%s", exp, exportSchema)] = []byte(schema)
	s.bulkExports = append(s.bulkExports, string(exp))

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

	// Find all CSV files created by the EXPORT.
	files, exp := func() (tree.Exprs, string) {
		s.lock.Lock()
		defer s.lock.Unlock()
		if len(s.bulkExports) == 0 {
			return tree.Exprs{}, ""
		}
		expr := s.bulkExports[0]
		s.bulkExports = s.bulkExports[1:]
		var fileNames []string
		for name := range s.bulkFiles {
			if strings.Contains(name, expr+"/") && !strings.HasSuffix(name, exportSchema) {
				fileNames = append(fileNames, name)
			}
		}
		sort.Strings(fileNames)
		var f tree.Exprs
		for _, name := range fileNames {
			f = append(f, tree.NewStrVal(s.bulkSrv.URL+name))
		}
		return f, expr
	}()
	// An empty table will produce an EXPORT with zero files.
	if len(files) == 0 {
		return nil, false
	}

	// Fix the table name in the existing schema file.
	tab := s.name("tab")
	tableSchema := func() []byte {
		s.lock.Lock()
		defer s.lock.Unlock()
		schema := fmt.Sprintf("/%s%s", exp, exportSchema)
		return importCreateTableRE.ReplaceAll(
			s.bulkFiles[schema],
			[]byte(fmt.Sprintf("CREATE TABLE %s (", tab)),
		)
	}()

	// Create the table to be imported into.
	_, err := s.db.Exec(string(tableSchema))
	if err != nil {
		return nil, false
	}

	return &tree.Import{
		Table:      tree.NewUnqualifiedTableName(tab),
		Into:       true,
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
