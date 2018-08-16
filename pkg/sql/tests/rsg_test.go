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

package tests_test

import (
	"context"
	gosql "database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/rsg"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var (
	flagRSGTime        = flag.Duration("rsg", 0, "random syntax generator test duration")
	flagRSGGoRoutines  = flag.Int("rsg-routines", 1, "number of Go routines executing random statements in each RSG test")
	flagRSGExecTimeout = flag.Duration("rsg-exec-timeout", 15*time.Second, "timeout duration when executing a statement")
)

func parseStatementList(sql string) (tree.StatementList, error) {
	var p parser.Parser
	return p.Parse(sql)
}

func verifyFormat(sql string) error {
	stmts, err := parseStatementList(sql)
	if err != nil {
		// Cannot serialize a statement list without parsing it.
		return nil
	}
	formattedSQL := tree.AsStringWithFlags(&stmts, tree.FmtShowPasswords)
	formattedStmts, err := parseStatementList(formattedSQL)
	if err != nil {
		return errors.Wrapf(err, "cannot parse output of Format: sql=%q, formattedSQL=%q", sql, formattedSQL)
	}
	formattedFormattedSQL := tree.AsStringWithFlags(&formattedStmts, tree.FmtShowPasswords)
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
	mu              struct {
		syncutil.Mutex
		// active holds the currently executing statements.
		active map[string]int
	}
}

// Incr records sql in the active map and returns a func to decrement it.
func (db *verifyFormatDB) Incr(sql string) func() {
	db.mu.Lock()
	if db.mu.active == nil {
		db.mu.active = make(map[string]int)
	}
	db.mu.active[sql]++
	db.mu.Unlock()

	return func() {
		db.mu.Lock()
		db.mu.active[sql]--
		if db.mu.active[sql] == 0 {
			delete(db.mu.active, sql)
		}
		db.mu.Unlock()
	}
}

func (db *verifyFormatDB) exec(ctx context.Context, sql string) error {
	if err := verifyFormat(sql); err != nil {
		db.verifyFormatErr = err
		return err
	}

	defer db.Incr(sql)()

	funcdone := make(chan error, 1)
	go func() {
		_, err := db.db.ExecContext(ctx, sql)
		funcdone <- err
	}()
	select {
	case err := <-funcdone:
		return errors.Wrap(err, sql)
	case <-time.After(*flagRSGExecTimeout):
		db.mu.Lock()
		defer db.mu.Unlock()
		b := make([]byte, 1024*1024)
		n := runtime.Stack(b, true)
		fmt.Printf("%s\n", b[:n])
		panic(errors.Errorf("timeout: %q. currently executing: %v", sql, db.mu.active))
	}
}

func TestRandomSyntaxGeneration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const rootStmt = "stmt"

	testRandomSyntax(t, false, nil, func(ctx context.Context, db *verifyFormatDB, r *rsg.RSG) error {
		s := r.Generate(rootStmt, 20)
		// Don't start transactions since closing them is tricky. Just issuing a
		// ROLLBACK after all queries doesn't work due to the parellel uses of db,
		// which can start another immediately after the ROLLBACK and cause problems
		// for the following statement. The CREATE DATABASE below would fail with
		// errors about an aborted transaction and thus panic.
		if strings.HasPrefix(s, "BEGIN") || strings.HasPrefix(s, "START") {
			return errors.New("transactions are unsupported")
		}
		if strings.HasPrefix(s, "SET SESSION CHARACTERISTICS AS TRANSACTION") {
			return errors.New("setting session characteristics is unsupported")
		}
		if strings.Contains(s, "READ ONLY") || strings.Contains(s, "read_only") {
			return errors.New("READ ONLY settings are unsupported")
		}
		if strings.Contains(s, "REVOKE") || strings.Contains(s, "GRANT") {
			return errors.New("REVOKE and GRANT are unsupported")
		}
		// Recreate the database on every run in case it was dropped or renamed in
		// a previous run. Should always succeed.
		if err := db.exec(ctx, `CREATE DATABASE IF NOT EXISTS ident`); err != nil {
			return err
		}
		return db.exec(ctx, s)
	})
}

func TestRandomSyntaxSelect(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const rootStmt = "target_list"

	testRandomSyntax(t, false, func(ctx context.Context, db *verifyFormatDB) error {
		return db.exec(ctx, `CREATE DATABASE IF NOT EXISTS ident; CREATE TABLE IF NOT EXISTS ident.ident (ident decimal);`)
	}, func(ctx context.Context, db *verifyFormatDB, r *rsg.RSG) error {
		targets := r.Generate(rootStmt, 300)
		var where, from string
		// Only generate complex clauses half the time.
		if rand.Intn(2) == 0 {
			where = r.Generate("where_clause", 300)
			from = r.Generate("from_clause", 300)
		} else {
			from = "FROM ident"
		}
		s := fmt.Sprintf("SELECT %s %s %s", targets, from, where)
		return db.exec(ctx, s)
	})
}

type namedBuiltin struct {
	name    string
	builtin tree.Overload
}

func TestRandomSyntaxFunctions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	done := make(chan struct{})
	defer close(done)
	namedBuiltinChan := make(chan namedBuiltin)
	go func() {
		for {
			for _, name := range builtins.AllBuiltinNames {
				switch strings.ToLower(name) {
				case "crdb_internal.force_panic", "crdb_internal.force_log_fatal", "pg_sleep":
					continue
				}
				_, variations := builtins.GetBuiltinProperties(name)
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

	testRandomSyntax(t, false, nil, func(ctx context.Context, db *verifyFormatDB, r *rsg.RSG) error {
		nb := <-namedBuiltinChan
		var args []string
		switch ft := nb.builtin.Types.(type) {
		case tree.ArgTypes:
			for _, arg := range ft {
				args = append(args, r.GenerateRandomArg(arg.Typ))
			}
		case tree.HomogeneousType:
			for i := r.Intn(5); i > 0; i-- {
				var typ types.T
				switch r.Intn(4) {
				case 0:
					typ = types.String
				case 1:
					typ = types.Float
				case 2:
					typ = types.Bool
				case 3:
					typ = types.TimestampTZ
				}
				args = append(args, r.GenerateRandomArg(typ))
			}
		case tree.VariadicType:
			for _, t := range ft.FixedTypes {
				args = append(args, r.GenerateRandomArg(t))
			}
			for i := r.Intn(5); i > 0; i-- {
				args = append(args, r.GenerateRandomArg(ft.VarType))
			}
		default:
			panic(fmt.Sprintf("unknown fn.Types: %T", ft))
		}
		var limit string
		switch strings.ToLower(nb.name) {
		case "generate_series":
			limit = " LIMIT 100"
		}
		s := fmt.Sprintf("SELECT %s(%s) %s", nb.name, strings.Join(args, ", "), limit)
		return db.exec(ctx, s)
	})
}

func TestRandomSyntaxFuncCommon(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const rootStmt = "func_expr_common_subexpr"

	testRandomSyntax(t, false, nil, func(ctx context.Context, db *verifyFormatDB, r *rsg.RSG) error {
		expr := r.Generate(rootStmt, 30)
		s := fmt.Sprintf("SELECT %s", expr)
		return db.exec(ctx, s)
	})
}

func TestRandomSyntaxSchemaChangeDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()

	roots := []string{
		"create_database_stmt",
		"drop_database_stmt",
		"alter_rename_database_stmt",
	}

	testRandomSyntax(t, true, func(ctx context.Context, db *verifyFormatDB) error {
		return db.exec(ctx, `
			CREATE DATABASE ident;
		`)
	}, func(ctx context.Context, db *verifyFormatDB, r *rsg.RSG) error {
		n := r.Intn(len(roots))
		s := r.Generate(roots[n], 30)
		return db.exec(ctx, s)
	})
}

func TestRandomSyntaxSchemaChangeColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	roots := []string{
		"alter_table_cmd",
	}

	testRandomSyntax(t, true, func(ctx context.Context, db *verifyFormatDB) error {
		return db.exec(ctx, `
			CREATE DATABASE ident;
			CREATE TABLE ident.ident (ident decimal);
		`)
	}, func(ctx context.Context, db *verifyFormatDB, r *rsg.RSG) error {
		n := r.Intn(len(roots))
		s := fmt.Sprintf("ALTER TABLE ident.ident %s", r.Generate(roots[n], 500))
		return db.exec(ctx, s)
	})
}

// testRandomSyntax performs all of the RSG setup and teardown for common
// random syntax testing operations. It takes a closure where the random
// expression should be generated and executed. It returns an error indicating
// if the statement executed successfully. This is used to verify that at
// least 1 success occurs (otherwise it is likely a bad test).
func testRandomSyntax(
	t *testing.T,
	allowDuplicates bool,
	setup func(context.Context, *verifyFormatDB) error,
	fn func(context.Context, *verifyFormatDB, *rsg.RSG) error,
) {
	if *flagRSGTime == 0 {
		t.Skip("enable with '-rsg <duration>'")
	}
	ctx := context.Background()

	params, _ := tests.CreateTestServerParams()
	params.UseDatabase = "ident"
	// Use a low memory limit to quickly halt runaway functions.
	params.SQLMemoryPoolSize = 3 * 1024 * 1024 // 3MB
	s, rawDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	db := &verifyFormatDB{db: rawDB}

	if setup != nil {
		err := setup(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
	}

	yBytes, err := ioutil.ReadFile(filepath.Join("..", "parser", "sql.y"))
	if err != nil {
		t.Fatal(err)
	}
	r, err := rsg.NewRSG(timeutil.Now().UnixNano(), string(yBytes), allowDuplicates)
	if err != nil {
		t.Fatal(err)
	}
	// Broadcast channel for all workers.
	done := make(chan struct{})
	time.AfterFunc(*flagRSGTime, func() {
		close(done)
	})
	var countsMu struct {
		syncutil.Mutex
		total, success int
	}
	ctx, cancel := context.WithCancel(ctx)
	// Print status updates. We want this go routine to continue until all the
	// workers are done, even if their ctx has been canceled, so the ctx for
	// this func is a separate one with its own cancel.
	go func(ctx context.Context) {
		start := timeutil.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
			countsMu.Lock()
			fmt.Printf("%v of %v: %d executions, %d successful\n",
				timeutil.Since(start).Round(time.Second),
				*flagRSGTime,
				countsMu.total,
				countsMu.success,
			)
			countsMu.Unlock()
		}
	}(ctx)
	ctx, timeoutCancel := context.WithTimeout(ctx, *flagRSGTime)
	err = ctxgroup.GroupWorkers(ctx, *flagRSGGoRoutines, func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			err := fn(ctx, db, r)
			countsMu.Lock()
			countsMu.total++
			if err == nil {
				countsMu.success++
			}
			countsMu.Unlock()
		}
	})
	timeoutCancel()
	// cancel the timer printing's ctx
	cancel()
	t.Logf("%d executions, %d successful", countsMu.total, countsMu.success)
	if err != nil {
		t.Fatal(err)
	}
	if countsMu.success == 0 {
		t.Fatal("0 successful executions")
	}
	if db.verifyFormatErr != nil {
		t.Fatal(db.verifyFormatErr)
	}
}
