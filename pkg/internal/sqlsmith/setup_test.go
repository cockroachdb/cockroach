// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlsmith_test

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

var (
	flagExec    = flag.Bool("ex", false, "execute (instead of just parse) generated statements")
	flagNum     = flag.Int("num", 100, "number of statements to generate")
	flagSetup   = flag.String("setup", "", "setup for TestGenerateParse, empty for random")
	flagSetting = flag.String("setting", "", "setting for TestGenerateParse, empty for random")
)

// TestSetups verifies that all setups generate executable SQL.
func TestSetups(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for name, setup := range sqlsmith.Setups {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
			defer srv.Stopper().Stop(ctx)

			rnd, _ := randutil.NewTestRand()

			stmts := setup(rnd)
			for _, stmt := range stmts {
				if _, err := sqlDB.Exec(stmt); err != nil {
					t.Log(stmt)
					t.Fatal(err)
				}
			}
		})
	}
}

// TestGenerateParse verifies that statements produced by Generate can be
// parsed. This is useful because since we make AST nodes directly we can
// sometimes put them into bad states that the parser would never do.
func TestGenerateParse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	rnd, seed := randutil.NewTestRand()
	t.Log("seed:", seed)

	db := sqlutils.MakeSQLRunner(sqlDB)

	setupName := *flagSetup
	if setupName == "" {
		setupName = sqlsmith.RandSetup(rnd)
	}
	setup, ok := sqlsmith.Setups[setupName]
	if !ok {
		t.Fatalf("unknown setup %s", setupName)
	}
	t.Log("setup:", setupName)
	settingName := *flagSetting
	if settingName == "" {
		settingName = sqlsmith.RandSetting(rnd)
	}
	setting, ok := sqlsmith.Settings[settingName]
	if !ok {
		t.Fatalf("unknown setting %s", settingName)
	}
	settings := setting(rnd)
	t.Log("setting:", settingName, settings.Options)
	setupSQL := setup(rnd)
	t.Log(strings.Join(setupSQL, "\n"))
	for _, stmt := range setupSQL {
		db.Exec(t, stmt)
	}

	smither, err := sqlsmith.NewSmither(sqlDB, rnd, settings.Options...)
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
		stmt, err = sqlsmith.TestingPrettyCfg.Pretty(parsed.AST)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Print("STMT: ", i, "\n", stmt, ";\n\n")
		if *flagExec {
			_, err = conn.ExecContext(ctx, `SET statement_timeout = '9s'`)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = conn.ExecContext(ctx, stmt); err != nil {
				es := err.Error()
				if !seen[es] {
					seen[es] = true
					fmt.Printf("ERR (%d): %v\n", i, err)
				}
			}
		}
	}
}
