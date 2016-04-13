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
// Author: Radu Berinde (radu@cockroachlabs.com)
//
// The parallel_test adds an orchestration layer on top of the logic_test code
// with the capability of running multiple test data files in parallel.
//
// Each test lives in a separate subdir under testdata/paralleltest. Each test
// dir contains a "main" file along with a set of files in logic test format.
//
// The format of the main file is very simple (for now). Each line is of the
// form:
//     run <file1> <file2> ..
// This will run the given logic test files in parallel and wait for them to
// complete.  The same file can be specified multiple times.

package sql_test

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
)

var (
	paralleltestdata = flag.String("partestdata", "partestdata/[^.]*", "test data glob")
)

type testDB struct {
	db      *sql.DB
	cleanup func()
}

type parallelTest struct {
	*testing.T
	srv     *testServer
	clients []testDB
}

func (t *parallelTest) close() {
	for i := len(t.clients) - 1; i >= 0; i-- {
		t.clients[i].cleanup()
		t.clients[i].db.Close()
	}
	t.clients = nil
	if t.srv != nil {
		cleanupTestServer(t.srv)
		t.srv = nil
	}
}

func (t *parallelTest) addClient(createDB bool) {
	pgURL, cleanupFunc := sqlutils.PGUrl(t.T, &t.srv.TestServer, security.RootUser, "TestParallel")
	db, err := sql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}

	if createDB {
		if _, err := db.Exec("CREATE DATABASE test;"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("SET DATABASE = test;"); err != nil {
		t.Fatal(err)
	}

	t.clients = append(t.clients, testDB{db: db, cleanup: cleanupFunc})
}

func (t *parallelTest) processTestFile(path string, db *sql.DB, ch chan bool) {
	if ch != nil {
		defer func() { ch <- true }()
	}

	// Set up a dummy logicTest structure to use that code.
	l := &logicTest{T: t.T, srv: t.srv, db: db, user: security.RootUser}
	if err := l.processTestFile(path); err != nil {
		t.Error(err)
	}
}

func (t *parallelTest) run(dir string) {
	defer t.close()
	t.setup()

	// Add the main client and set up the database.
	t.addClient(true)

	// Open the main faile.
	fmt.Printf("Running test %s\n", dir)
	mainFile := filepath.Join(dir, "main")
	file, err := os.Open(mainFile)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	s := newLineScanner(file)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) == 0 {
			continue
		}
		cmd := fields[0]
		if strings.HasPrefix(cmd, "#") {
			// Skip comment lines.
			continue
		}
		switch cmd {
		case "run":
			testFiles := fields[1:]
			for len(t.clients) < len(testFiles) {
				t.addClient(false)
			}
			if testing.Verbose() || log.V(1) {
				fmt.Printf("%s:%d: running %s\n", mainFile, s.line, strings.Join(testFiles, ","))
			}
			ch := make(chan bool)
			for i, f := range testFiles {
				go t.processTestFile(filepath.Join(dir, f), t.clients[i].db, ch)
			}
			// Wait for all clients to complete.
			for range testFiles {
				<-ch
			}
		default:
			t.Fatalf("%s:%d: unknown command: %s", mainFile, s.line, cmd)
		}
	}
}

func (t *parallelTest) setup() {
	ctx := server.NewTestContext()
	ctx.MaxOffset = logicMaxOffset
	ctx.TestingKnobs.ExecutorTestingKnobs.WaitForGossipUpdate = true
	ctx.TestingKnobs.ExecutorTestingKnobs.CheckStmtStringChange = true
	t.srv = setupTestServerWithContext(t.T, ctx)
}

func TestParallel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	glob := string(*paralleltestdata)
	paths, err := filepath.Glob(glob)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatalf("No testfiles found (glob: %s)", glob)
	}
	total := 0
	for _, p := range paths {
		pt := parallelTest{T: t}
		pt.run(p)
		total++
	}
	fmt.Printf("%d parallel tests passed\n", total)
}
