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

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/rsg"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var (
	flagRSGTime       = flag.Duration("rsg", 0, "random syntax generator test duration")
	flagRSGGoRoutines = flag.Int("rsg-routines", 1, "number of Go routines executing random statements in each RSG test")
)

func parseStatementList(sql string) (parser.StatementList, error) {
	var p parser.Parser
	return p.Parse(sql)
}

func verifyFormat(sql string) error {
	stmts, err := parseStatementList(sql)
	if err != nil {
		// Cannot serialize a statement list without parsing it.
		return nil
	}
	formattedSQL := parser.AsStringWithFlags(stmts, parser.FmtSimpleWithPasswords)
	formattedStmts, err := parseStatementList(formattedSQL)
	if err != nil {
		return errors.Wrapf(err, "cannot parse output of Format: sql=%q, formattedSQL=%q", sql, formattedSQL)
	}
	formattedFormattedSQL := parser.AsStringWithFlags(formattedStmts, parser.FmtSimpleWithPasswords)
	if formattedSQL != formattedFormattedSQL {
		return errors.Errorf("Parse followed by Format is not idempotent: %q -> %q != %q", sql, formattedSQL, formattedFormattedSQL)
	}
	// TODO(eisen): ensure that the reconstituted SQL not only parses but also has
	// the same meaning as the original.
	return nil
}

type verifyFormatDB struct {
	db              *gosql.DB
	verifyFormatErr error
}

func (db *verifyFormatDB) exec(sql string) error {
	if err := verifyFormat(sql); err != nil {
		db.verifyFormatErr = err
		return err
	}
	_, err := db.db.Exec(sql)
	return err
}

func TestRandomSyntaxGeneration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const rootStmt = "stmt"

	testRandomSyntax(t, nil, func(db *verifyFormatDB, r *rsg.RSG) error {
		s := r.Generate(rootStmt, 20)
		// Don't start transactions since closing them is tricky. Just issuing a
		// ROLLBACK after all queries doesn't work due to the parellel uses of db,
		// which can start another immediately after the ROLLBACK and cause problems
		// for the following statement. The CREATE DATABASE below would fail with
		// errors about an aborted transaction and thus panic.
		if strings.HasPrefix(s, "BEGIN") || strings.HasPrefix(s, "START") {
			return errors.New("transactions are unsupported")
		}
		if strings.HasPrefix(s, "REVOKE") || strings.HasPrefix(s, "GRANT") {
			return errors.New("TODO(mjibson): figure out why these run slowly, may be a bug")
		}
		// Recreate the database on every run in case it was dropped or renamed in
		// a previous run. Should always succeed.
		if err := db.exec(`CREATE DATABASE IF NOT EXISTS ident`); err != nil {
			panic(err)
		}
		return db.exec(s)
	})
}

func TestRandomSyntaxSelect(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const rootStmt = "target_list"

	testRandomSyntax(t, func(db *verifyFormatDB) error {
		return db.exec(`CREATE DATABASE IF NOT EXISTS ident; CREATE TABLE IF NOT EXISTS ident.ident (ident decimal);`)
	}, func(db *verifyFormatDB, r *rsg.RSG) error {
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
		return db.exec(s)
	})
}

type namedBuiltin struct {
	name    string
	builtin parser.Builtin
}

func TestRandomSyntaxFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	done := make(chan struct{})
	defer close(done)
	namedBuiltinChan := make(chan namedBuiltin)
	go func() {
		for {
			for name, variations := range parser.Builtins {
				switch strings.ToLower(name) {
				case "crdb_internal.force_panic", "crdb_internal.force_log_fatal":
					continue
				case "repeat":
					// TODO(mjibson): wait for fix of #10545
					continue
				}
				for _, builtin := range variations {
					select {
					case <-done:
						return
					case namedBuiltinChan <- namedBuiltin{name: name, builtin: builtin}:
					}
				}
			}
		}
	}()

	testRandomSyntax(t, nil, func(db *verifyFormatDB, r *rsg.RSG) error {
		nb := <-namedBuiltinChan
		var args []string
		switch ft := nb.builtin.Types.(type) {
		case parser.ArgTypes:
			for _, arg := range ft {
				args = append(args, r.GenerateRandomArg(arg.Typ))
			}
		case parser.HomogeneousType:
			for i := r.Intn(5); i > 0; i-- {
				var typ parser.Type
				switch r.Intn(4) {
				case 0:
					typ = parser.TypeString
				case 1:
					typ = parser.TypeFloat
				case 2:
					typ = parser.TypeBool
				case 3:
					typ = parser.TypeTimestampTZ
				}
				args = append(args, r.GenerateRandomArg(typ))
			}
		case parser.VariadicType:
			for i := r.Intn(5); i > 0; i-- {
				args = append(args, r.GenerateRandomArg(ft.Typ))
			}
		default:
			panic(fmt.Sprintf("unknown fn.Types: %T", ft))
		}
		s := fmt.Sprintf("SELECT %s(%s)", nb.name, strings.Join(args, ", "))
		funcdone := make(chan error, 1)
		go func() {
			funcdone <- db.exec(s)
		}()
		select {
		case err := <-funcdone:
			return err
		case <-time.After(time.Second * 10):
			panic(fmt.Sprintf("func exec timeout: %s", s))
		}
	})
}

func TestRandomSyntaxFuncCommon(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const rootStmt = "func_expr_common_subexpr"

	testRandomSyntax(t, nil, func(db *verifyFormatDB, r *rsg.RSG) error {
		expr := r.Generate(rootStmt, 30)
		s := fmt.Sprintf("SELECT %s", expr)
		return db.exec(s)
	})
}

// testRandomSyntax performs all of the RSG setup and teardown for common
// random syntax testing operations. It takes a closure where the random
// expression should be generated and executed. It returns an error indicating
// if the statement executed successfully. This is used to verify that at
// least 1 success occurs (otherwise it is likely a bad test).
func testRandomSyntax(
	t *testing.T, setup func(*verifyFormatDB) error, fn func(*verifyFormatDB, *rsg.RSG) error,
) {
	if *flagRSGTime == 0 {
		t.Skip("enable with '-rsg <duration>'")
	}

	params, _ := createTestServerParams()
	params.UseDatabase = "ident"
	// Use a low memory limit to quickly halt runaway functions.
	params.SQLMemoryPoolSize = 3 * 1024 * 1024 // 3MB
	s, rawDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	db := &verifyFormatDB{db: rawDB}

	if setup != nil {
		err := setup(db)
		if err != nil {
			t.Fatal(err)
		}
	}

	yBytes, err := ioutil.ReadFile(filepath.Join("parser", "sql.y"))
	if err != nil {
		t.Fatal(err)
	}
	r, err := rsg.NewRSG(timeutil.Now().UnixNano(), string(yBytes))
	if err != nil {
		t.Fatal(err)
	}
	// Broadcast channel for all workers.
	done := make(chan struct{})
	time.AfterFunc(*flagRSGTime, func() {
		close(done)
	})
	var wg sync.WaitGroup
	var countsMu struct {
		syncutil.Mutex
		total, success int
	}
	worker := func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			err := fn(db, r)
			countsMu.Lock()
			countsMu.total++
			if err == nil {
				countsMu.success++
			}
			countsMu.Unlock()
		}
	}
	wg.Add(*flagRSGGoRoutines)
	for i := 0; i < *flagRSGGoRoutines; i++ {
		go worker()
	}
	wg.Wait()
	t.Logf("%d executions, %d successful", countsMu.total, countsMu.success)
	if countsMu.success == 0 {
		t.Fatal("0 successful executions")
	} else if db.verifyFormatErr != nil {
		t.Fatal(db.verifyFormatErr)
	}
}
