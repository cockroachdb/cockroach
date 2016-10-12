// Copyright 2016 The Cockroach Authors.
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
//
// Author: Matt Jibson (mjibson@cockroachlabs.com)

package sql_test

import (
	gosql "database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/rsg"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var (
	flagRSGTime       = flag.Duration("rsg", 0, "random syntax generator test duration")
	flagRSGGoRoutines = flag.Int("rsg-routines", 1, "number of Go routines executing random statements in each RSG test")
)

func TestRandomSyntaxGeneration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const rootStmt = "stmt"

	testRandomSyntax(t, nil, func(db *gosql.DB, r *rsg.RSG) bool {
		s := r.Generate(rootStmt, 20)
		// Don't start transactions since closing them is tricky. Just issuing a
		// ROLLBACK after all queries doesn't work due to the parellel uses of db,
		// which can start another immediately after the ROLLBACK and cause problems
		// for the following statement. The CREATE DATABASE below would fail with
		// errors about an aborted transaction and thus panic.
		if strings.HasPrefix(s, "BEGIN") || strings.HasPrefix(s, "START") {
			return false
		}
		// TODO(mjibson): figure out why these run slowly. May be a bug.
		if strings.HasPrefix(s, "REVOKE") || strings.HasPrefix(s, "GRANT") {
			return false
		}
		// But the create should always succeed.
		_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS ident`)
		if err != nil {
			panic(err)
		}
		_, err = db.Exec(s)
		return err == nil
	})
}

func TestRandomSyntaxSelect(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const rootStmt = "target_list"

	testRandomSyntax(t, func(db *gosql.DB) error {
		_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS ident; CREATE TABLE IF NOT EXISTS ident.ident (ident decimal);`)
		return err
	}, func(db *gosql.DB, r *rsg.RSG) bool {
		targets := r.Generate(rootStmt, 30)
		var where, from string
		// Only generate complex clauses half the time.
		if rand.Intn(2) == 0 {
			where = r.Generate("where_clause", 30)
			from = r.Generate("from_clause", 30)
		} else {
			from = "FROM ident"
		}
		s := fmt.Sprintf("SELECT %s %s %s", targets, from, where)
		_, err := db.Exec(s)
		return err == nil
	})
}

func TestRandomSyntaxFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var names []string
	for b := range parser.Builtins {
		names = append(names, b)
	}

	testRandomSyntax(t, nil, func(db *gosql.DB, r *rsg.RSG) bool {
		name := names[r.Intn(len(names))]
		variations := parser.Builtins[name]
		fn := variations[r.Intn(len(variations))]
		var args []string
		switch ft := fn.Types.(type) {
		case parser.ArgTypes:
			for _, typ := range ft {
				var v interface{}
				switch typ.(type) {
				case *parser.DInt:
					i := r.Int63()
					i -= r.Int63()
					v = i
				case *parser.DFloat, *parser.DDecimal:
					v = r.Float64()
				case *parser.DString:
					v = `'string'`
				case *parser.DBytes:
					v = `b'bytes'`
				case *parser.DTimestamp, *parser.DTimestampTZ:
					t := time.Unix(0, r.Int63())
					v = fmt.Sprintf(`'%s'`, t.Format(time.RFC3339Nano))
				case *parser.DBool:
					if r.Intn(2) == 0 {
						v = "false"
					} else {
						v = "true"
					}
				case *parser.DDate:
					i := r.Int63()
					i -= r.Int63()
					d := parser.NewDDate(parser.DDate(i))
					v = fmt.Sprintf(`'%s'`, d)
				case *parser.DInterval:
					d := duration.Duration{Nanos: r.Int63()}
					v = fmt.Sprintf(`'%s'`, &parser.DInterval{Duration: d})
				case *parser.DTuple:
					v = "NULL"
				default:
					panic(fmt.Errorf("unknown arg type: %T", typ))
				}
				args = append(args, fmt.Sprint(v))
			}
		default:
			return false
		}
		s := fmt.Sprintf("SELECT %s(%s)", name, strings.Join(args, ", "))
		funcdone := make(chan bool, 1)
		go func() {
			_, err := db.Exec(s)
			funcdone <- err == nil
		}()
		select {
		case success := <-funcdone:
			return success
		case <-time.After(time.Second * 5):
			panic(fmt.Errorf("func exec timeout: %s", s))
		}
	})
}

func TestRandomSyntaxFuncCommon(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const rootStmt = "func_expr_common_subexpr"

	testRandomSyntax(t, nil, func(db *gosql.DB, r *rsg.RSG) bool {
		expr := r.Generate(rootStmt, 30)
		s := fmt.Sprintf("SELECT %s", expr)
		_, err := db.Exec(s)
		return err == nil
	})
}

// testRandomSyntax performs all of the RSG setup and teardown for common random syntax testing operations. It takes f, a closure where the random expression should be generated and executed. It returns a boolean indicating if the statement executed successfully. This is used to verify that at least 1 success occurs (otherwise it is likely a bad test).
func testRandomSyntax(
	t *testing.T, setup func(db *gosql.DB) error, f func(db *gosql.DB, r *rsg.RSG) (success bool),
) {
	if *flagRSGTime == 0 {
		t.Skip("enable with '-rsg <duration>'")
	}

	params, _ := createTestServerParams()
	params.UseDatabase = "ident"
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

	if setup != nil {
		err := setup(db)
		if err != nil {
			t.Fatal(err)
		}
	}

	y, err := ioutil.ReadFile(filepath.Join("parser", "sql.y"))
	if err != nil {
		t.Fatal(err)
	}
	r, err := rsg.NewRSG(timeutil.Now().UnixNano(), string(y))
	if err != nil {
		t.Fatal(err)
	}
	// Broadcast channel for all workers.
	done := make(chan bool)
	var wg sync.WaitGroup
	var lock syncutil.Mutex
	var total, success int
	worker := func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			s := f(db, r)
			lock.Lock()
			total++
			if s {
				success++
			}
			lock.Unlock()
		}
	}
	for i := 0; i < *flagRSGGoRoutines; i++ {
		go worker()
		wg.Add(1)
	}
	time.Sleep(*flagRSGTime)
	close(done)
	wg.Wait()
	t.Logf("%d executions, %d successful", total, success)
	if success == 0 {
		t.Fatal("0 successful executions")
	}
}
