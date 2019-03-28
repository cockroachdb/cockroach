// Copyright 2019 The Cockroach Authors.
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

package sqlsmith

import (
	"context"
	"flag"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

var (
	flagExec = flag.Bool("ex", false, "execute (instead of just parse) generated statements")
	flagNum  = flag.Int("num", 100, "number of statements to generate")
)

func init() {
	flag.Parse()
}

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
	db.Exec(t, `
		CREATE TABLE t (
			_bool bool,
			_bytes bytes,
			_date date,
			_decimal decimal,
			_float4 float4,
			_float8 float8,
			_inet inet,
			_int4 int4,
			_int8 int8,
			_interval interval,
			_jsonb jsonb,
			_string string,
			_time time,
			_timestamp timestamp,
			_timestamptz timestamptz,
			_uuid uuid
		);
		INSERT INTO t DEFAULT VALUES;
	`)

	smither, err := NewSmither(sqlDB, rnd)
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
		if *flagExec {
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
