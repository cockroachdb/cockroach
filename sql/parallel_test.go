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
	gosql "database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v1"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
)

var (
	paralleltestdata = flag.String("partestdata", "partestdata/[^.]*", "test data glob")
)

type testDB struct {
	db      *gosql.DB
	cleanup func()
}

type parallelTest struct {
	*testing.T
	srv     serverutils.TestServerInterface
	clients []testDB
}

func (t *parallelTest) close() {
	for i := len(t.clients) - 1; i >= 0; i-- {
		t.clients[i].cleanup()
		t.clients[i].db.Close()
	}
	t.clients = nil
	if t.srv != nil {
		t.srv.Stopper().Stop()
		t.srv = nil
	}
}

func (t *parallelTest) addClient(createDB bool) {
	pgURL, cleanupFunc := sqlutils.PGUrl(t.T, t.srv.ServingAddr(), security.RootUser,
		"TestParallel")
	db, err := gosql.Open("postgres", pgURL.String())
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

func (t *parallelTest) processTestFile(path string, db *gosql.DB, ch chan bool) {
	if ch != nil {
		defer func() { ch <- true }()
	}

	// Set up a dummy logicTest structure to use that code.
	l := &logicTest{T: t.T, srv: t.srv, db: db, user: security.RootUser}
	if err := l.processTestFile(path); err != nil {
		t.Error(err)
	}
}

type parTestRunEntry struct {
	Node int    `yaml:"client"`
	File string `yaml:"file"`
}

type parTestSpec struct {
	Run [][]parTestRunEntry `yaml:"run"`
}

func (t *parallelTest) run(dir string) {
	defer t.close()
	t.setup()

	// Add the main client and set up the database.
	t.addClient(true)

	// Open the main file.
	fmt.Printf("Running test %s\n", dir)

	mainFile := filepath.Join(dir, "test.yaml")
	yamlData, err := ioutil.ReadFile(mainFile)
	if err != nil {
		t.Fatal(err)
	}
	var spec parTestSpec
	err = yaml.Unmarshal(yamlData, &spec)
	if err != nil {
		t.Fatal(err)
	}

	for runListIdx, runList := range spec.Run {
		for len(t.clients) < len(runList) {
			t.addClient(false)
		}
		if testing.Verbose() || log.V(1) {
			var descr []string
			for _, re := range runList {
				descr = append(descr, fmt.Sprintf("%d:%s", re.Node, re.File))
			}
			fmt.Printf("%s: run list %d: %s\n", mainFile, runListIdx, strings.Join(descr, ", "))
		}
		ch := make(chan bool)
		for i, re := range runList {
			go t.processTestFile(filepath.Join(dir, re.File), t.clients[i].db, ch)
		}
		// Wait for all clients to complete.
		for range runList {
			<-ch
		}
	}
}

func (t *parallelTest) setup() {
	params := base.TestServerArgs{
		MaxOffset: logicMaxOffset,
		Knobs: base.TestingKnobs{
			SQLExecutor: &sql.ExecutorTestingKnobs{
				WaitForGossipUpdate:   true,
				CheckStmtStringChange: true,
			},
		},
	}
	t.srv, _, _ = serverutils.StartServer(t, params)
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
