// Copyright 2019 The Cockroach Authors.
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
	"context"
	"flag"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

var (
	flagExec     = flag.Bool("ex", false, "execute (instead of just parse) generated statements")
	flagNum      = flag.Int("num", 100, "number of statements to generate")
	flagSetup    = flag.String("setup", "", "setup for TestGenerateParse, empty for random")
	flagSetting  = flag.String("setting", "", "setting for TestGenerateParse, empty for random")
	flagCheckVec = flag.Bool("check-vec", false, "fail if a generated statement cannot be vectorized")
)

// TestSetups verifies that all setups generate executable SQL.
func TestSetups(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for name, setup := range Setups {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
			defer s.Stopper().Stop(ctx)

			rnd, _ := randutil.NewPseudoRand()

			sql := setup(rnd)
			if _, err := sqlDB.Exec(sql); err != nil {
				t.Log(sql)
				t.Fatal(err)
			}
		})
	}
}

// TestRandTableInserts tests that valid INSERTS can be generated for the
// rand-tables setup.
//
// There is little visibility into the validity of statements created by
// sqlsmith. This makes it easy to unknowingly make a change to sqlsmith that
// results in many or all generated statements being invalid, reducing the
// efficacy of sqlsmith. sqlsmith is greatly hampered if all INSERTs fail,
// because queries will only run on empty tables. See #63190 for additional
// context.
//
// This test is meant to catch these types of regressions in sqlsmith or
// rand-tables. It creates 10 random tables, and performs up to 1000 inserts.
// The test fails if not a single row was inserted.
//
// If this test fails, there is likely a bug in:
//
//   1. sqlsmith that makes valid INSERTs impossible or very unlikely
//   2. Or rand-tables that makes it impossible or very unlikely to ever
//      generate a successful INSERT
//
// Note that there is a small but non-zero chance that this test produces a
// false-negative.
func TestRandTableInserts(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	rnd, _ := randutil.NewPseudoRand()

	setup := randTablesN(rnd, 10)
	if _, err := sqlDB.Exec(setup); err != nil {
		t.Log(setup)
		t.Fatal(err)
	}

	insertOnly := simpleOption("insert only", func(s *Smither) {
		s.stmtWeights = []statementWeight{
			{1, makeInsert},
		}
	})

	smither, err := NewSmither(sqlDB, rnd, insertOnly())
	if err != nil {
		t.Fatal(err)
	}
	defer smither.Close()

	// Run up to numInserts INSERT statements.
	success := false
	numErrors := 0
	numZeroRowInserts := 0
	numInserts := 1000
	for i := 0; i < numInserts; i++ {
		stmt := smither.Generate()
		res, err := sqlDB.Exec(stmt)
		if err != nil {
			numErrors++
			continue
		}
		rows, _ := res.RowsAffected()
		if rows == 0 {
			numZeroRowInserts++
			continue
		}
		success = true
		break
	}

	if !success {
		t.Errorf(
			"expected 1 success out of %v inserts, got %v errors and %v zero-row inserts",
			numInserts, numErrors, numZeroRowInserts,
		)
		t.Log(setup)
	}
}

// TestGenerateParse verifies that statements produced by Generate can be
// parsed. This is useful because since we make AST nodes directly we can
// sometimes put them into bad states that the parser would never do.
func TestGenerateParse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	rnd, seed := randutil.NewPseudoRand()
	t.Log("seed:", seed)

	db := sqlutils.MakeSQLRunner(sqlDB)

	setupName := *flagSetup
	if setupName == "" {
		setupName = RandSetup(rnd)
	}
	setup, ok := Setups[setupName]
	if !ok {
		t.Fatalf("unknown setup %s", setupName)
	}
	t.Log("setup:", setupName)
	settingName := *flagSetting
	if settingName == "" {
		settingName = RandSetting(rnd)
	}
	setting, ok := Settings[settingName]
	if !ok {
		t.Fatalf("unknown setting %s", settingName)
	}
	settings := setting(rnd)
	t.Log("setting:", settingName, settings.Options)
	setupSQL := setup(rnd)
	t.Log(setupSQL)
	db.Exec(t, setupSQL)

	smither, err := NewSmither(sqlDB, rnd, settings.Options...)
	if err != nil {
		t.Fatal(err)
	}
	defer smither.Close()

	seen := map[string]bool{}
	for i := 0; i < *flagNum; i++ {
		stmt := smither.Generate()
		if err != nil {
			t.Fatalf("%v: %v", stmt, err)
		}
		parsed, err := parser.ParseOne(stmt)
		if err != nil {
			t.Fatalf("%v: %v", stmt, err)
		}
		stmt = prettyCfg.Pretty(parsed.AST)
		fmt.Print("STMT: ", i, "\n", stmt, ";\n\n")
		if *flagCheckVec {
			if _, err := sqlDB.Exec(fmt.Sprintf("EXPLAIN (vec) %s", stmt)); err != nil {
				es := err.Error()
				ok := false
				// It is hard to make queries that can always
				// be vectorized. Hard code a list of error
				// messages we are ok with.
				for _, s := range []string{
					// If the optimizer removes stuff due
					// to something like a `WHERE false`,
					// vec will fail with an error message
					// like this. This is hard to fix
					// because things like `WHERE true AND
					// false` similarly remove rows but are
					// harder to detect.
					"num_rows:0",
					"unsorted distinct",
				} {
					if strings.Contains(es, s) {
						ok = true
						break
					}
				}
				if !ok {
					t.Fatal(err)
				}
			}
		}
		if *flagExec {
			db.Exec(t, `SET statement_timeout = '9s'`)
			if _, err := sqlDB.Exec(stmt); err != nil {
				es := err.Error()
				if !seen[es] {
					seen[es] = true
					fmt.Printf("ERR (%d): %v\n", i, err)
				}
			}
		}
	}
}
