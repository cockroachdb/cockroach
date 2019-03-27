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
	"strings"
	"sync"
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
	var lock sync.Mutex
	files := map[string][]byte{}
	s.bulkSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		defer lock.Unlock()
		localfile := r.URL.Path
		switch r.Method {
		case "PUT":
			b, err := ioutil.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			files[localfile] = b
			w.WriteHeader(201)
		case "GET", "HEAD":
			b, ok := files[localfile]
			if !ok {
				http.Error(w, fmt.Sprintf("not found: %s", localfile), 404)
				return
			}
			w.Write(b)
		case "DELETE":
			delete(files, localfile)
			w.WriteHeader(204)
		default:
			http.Error(w, "unsupported method", 400)
		}
	}))
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
