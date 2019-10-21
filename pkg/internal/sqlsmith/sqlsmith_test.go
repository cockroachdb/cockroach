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
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

var (
	flagExec        = flag.Bool("ex", false, "execute (instead of just parse) generated statements")
	flagNum         = flag.Int("num", 100, "number of statements to generate")
	flagNoMutations = flag.Bool("no-mutations", false, "disables mutations during testing")
	flagNoWiths     = flag.Bool("no-withs", false, "disables WITHs during testing")
	flagVec         = flag.Bool("vec", false, "attempt to generate vectorized-friendly queries")
	flagCheckVec    = flag.Bool("check-vec", false, "fail if a generated statement cannot be vectorized")
)

// TestGenerateParse verifies that statements produced by Generate can be
// parsed. This is useful because since we make AST nodes directly we can
// sometimes put them into bad states that the parser would never do.
func TestGenerateParse(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	rnd, _ := randutil.NewPseudoRand()

	db := sqlutils.MakeSQLRunner(sqlDB)
	var opts []SmitherOption
	if *flagNoMutations {
		opts = append(opts, DisableMutations())
	}
	if *flagNoWiths {
		opts = append(opts, DisableWith())
	}
	if *flagVec {
		opts = append(opts, Vectorizable())
		db.Exec(t, VecSeedTable)
	} else {
		db.Exec(t, SeedTable)
	}

	smither, err := NewSmither(sqlDB, rnd, opts...)
	if err != nil {
		t.Fatal(err)
	}

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

func TestWeightedSampler(t *testing.T) {
	defer leaktest.AfterTest(t)()

	expected := []int{1, 1, 1, 1, 1, 0, 2, 2, 0, 0, 0, 1, 1, 2, 0, 2}

	s := NewWeightedSampler([]int{1, 3, 4}, 0)
	var got []int
	for i := 0; i < 16; i++ {
		got = append(got, s.Next())
	}
	if !reflect.DeepEqual(expected, got) {
		t.Fatalf("got %v, expected %v", got, expected)
	}
}
