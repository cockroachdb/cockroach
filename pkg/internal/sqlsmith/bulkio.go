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
	"strings"
	"sync"
	"time"

	_ "github.com/cockroachdb/cockroach/pkg/ccl" // ccl init hooks to enable Bulk IO
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
			_, _ = w.Write(b)
		case "DELETE":
			delete(files, localfile)
			w.WriteHeader(204)
		default:
			http.Error(w, "unsupported method", 400)
		}
	}))
	s.bulkBackups = map[string]tree.TargetList{}
}

func makeAsOf(s *Smither) tree.AsOfClause {
	var expr tree.Expr
	switch s.rnd.Intn(10) {
	case 1:
		expr = tree.NewStrVal("-2s")
	case 2:
		expr = tree.NewStrVal(timeutil.Now().Add(-2 * time.Second).Format(tree.TimestampOutputFormat))
	case 3:
		expr = sqlbase.RandDatum(s.rnd, types.Interval, false /* nullOk */)
	case 4:
		datum := sqlbase.RandDatum(s.rnd, types.Timestamp, false /* nullOk */)
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

	var opts tree.KVOptions
	if s.coin() {
		opts = tree.KVOptions{
			tree.KVOption{
				Key: "revision_history",
			},
		}
	}

	return &tree.Backup{
		Targets: targets,
		To:      tree.PartitionedBackup{tree.NewStrVal(name)},
		AsOf:    makeAsOf(s),
		Options: opts,
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
		From:    []tree.PartitionedBackup{{tree.NewStrVal(name)}},
		AsOf:    makeAsOf(s),
		Options: tree.KVOptions{
			tree.KVOption{
				Key:   "into_db",
				Value: tree.NewStrVal(string(db)),
			},
		},
	}, true
}
