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
	"math"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/internal/rsg"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

var (
	flagRSGTime       = flag.Duration("rsg", 0, "random syntax generator test duration")
	flagRSGGoRoutines = flag.Int("rsg-routines", 1, "number of Go routines executing random statements in each RSG test")
)

func TestRandomSyntaxGeneration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const rootStmt = "target_list"

	testRandomSyntax(t, func(db *gosql.DB, r *rsg.RSG) {
		s := r.Generate(rootStmt, 20)
		if strings.HasPrefix(s, "REVOKE") || strings.HasPrefix(s, "GRANT") {
			return
		}
		_, _ = db.Exec(`ROLLBACK`)
		_, _ = db.Exec(`CREATE DATABASE IF NOT EXISTS name; SET DATABASE name;`)
		_, _ = db.Exec(s)
	})
}

func TestRandomSyntaxSelect(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const rootStmt = "stmt"

	testRandomSyntax(t, func(db *gosql.DB, r *rsg.RSG) {
		if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS ident; CREATE TABLE IF NOT EXISTS ident.ident (ident decimal);`); err != nil {
			panic(err)
		}

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
		_, _ = db.Exec(`ROLLBACK`)
		_, _ = db.Exec(`SET DATABASE = ident`)
		_, _ = db.Exec(s)
	})
}

func TestRandomSyntaxFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var names []string
	for b := range parser.Builtins {
		names = append(names, b)
	}

	testRandomSyntax(t, func(db *gosql.DB, r *rsg.RSG) {
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
					i := r.Intn(math.MaxInt64)
					i -= r.Intn(math.MaxInt64)
					v = i
				case *parser.DFloat, *parser.DDecimal:
					v = r.Float64()
				case *parser.DString:
					v = `'string'`
				case *parser.DBytes:
					v = `b'bytes'`
				case *parser.DTimestamp:
					t := time.Unix(0, int64(r.Intn(math.MaxInt64)))
					v = fmt.Sprintf(`'%s'`, t.Format(time.RFC3339Nano))
				default:
					panic(fmt.Errorf("unknown arg type: %T", typ))
				}
				args = append(args, fmt.Sprint(v))
			}
		default:
			return
		}
		s := fmt.Sprintf("SELECT %s(%s)", name, strings.Join(args, ", "))
		_, _ = db.Exec("ROLLBACK")
		funcdone := make(chan bool, 1)
		go func() {
			_, _ = db.Exec(s)
			funcdone <- true
		}()
		select {
		case <-funcdone:
		case <-time.After(time.Second * 5):
			panic(fmt.Errorf("func exec timeout: %s", s))
		}
	})
}

func TestRandomSyntaxFuncCommon(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const rootStmt = "func_expr_common_subexpr"

	testRandomSyntax(t, func(db *gosql.DB, r *rsg.RSG) {
		expr := r.Generate(rootStmt, 30)
		s := fmt.Sprintf("SELECT %s", expr)
		_, _ = db.Exec(`ROLLBACK`)
		_, _ = db.Exec(s)
	})
}

func testRandomSyntax(t *testing.T, f func(db *gosql.DB, r *rsg.RSG)) {
	if *flagRSGTime == 0 {
		t.Skip("enable with '-rsg <duration>'")
	}

	params, _ := createTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

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
	worker := func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			f(db, r)
		}
	}
	for i := 0; i < *flagRSGGoRoutines; i++ {
		go worker()
		wg.Add(1)
	}
	time.Sleep(*flagRSGTime)
	close(done)
	wg.Wait()
}
