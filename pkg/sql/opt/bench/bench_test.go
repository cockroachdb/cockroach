// Copyright 2018 The Cockroach Authors.
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

package bench

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

type benchmarkType int

const (
	// Create the AST.
	parse benchmarkType = iota

	// parse + build the memo with no normalizations enabled.
	optbuild

	// optbuild + normalizations enabled, but with no exploration patterns,
	// enforcers, or costing.
	prepare

	// prepare + exploration patterns, enforcers, and costing.
	search

	// search + build the execution nodes.
	execbuild

	// execbuild + execute using minimal test execution harness.
	execute

	// Executes the query end-to-end using the v2.0 planner (but with empty
	// tables).
	v20

	// Executes the query end-to-end using the new optimizer (but with empty
	// tables).
	v21
)

var benchmarkTypeStrings = [...]string{
	parse:     "Parse",
	optbuild:  "OptBuild",
	prepare:   "Prepare",
	search:    "Search",
	execbuild: "ExecBuild",
	execute:   "Execute",
	v20:       "V20",
	v21:       "V21",
}

type benchQuery struct {
	name  string
	query string
}

var queries = [...]benchQuery{
	// Taken from BenchmarkSelectXXX in pkg/sql/bench/bench_test.go.
	{"Select1", `SELECT 1`},
	{"Select2", `SELECT a, b, c, a+b, a+1, (a+2)*(b+3)*(c+4) FROM bench.select WHERE (a = 1) OR ((a = 2) and (b = c)) OR (a + b = 3) OR (2*a + 4*b = 4*c)`},
	{"Select3", `SELECT a/b, b/c, c != 3.3 + 1.0, a = 2.0, c * 9.0 FROM bench.select WHERE a > 1 AND b < 4.5`},

	// Taken from BenchmarkCount in pkg/sql/bench/bench_test.go.
	{"Count", `SELECT COUNT(*) FROM bench.count`},

	// Taken from BenchmarkScan in pkg/sql/bench/bench_test.go.
	{"Scan", `SELECT * FROM scan LIMIT 10`},

	// Taken from BenchmarkPlanning in pkg/sql/bench/bench_test.go.
	{"Planning1", `SELECT * FROM abc`},
	{"Planning2", `SELECT * FROM abc WHERE a > 5 ORDER BY a`},
	{"Planning3", `SELECT * FROM abc WHERE b = 5`},
	{"Planning4", `SELECT * FROM abc WHERE b = 5 ORDER BY a`},
	{"Planning5", `SELECT * FROM abc WHERE c = 5`},
	{"Planning6", `SELECT * FROM abc JOIN abc AS abc2 ON abc.a = abc2.a`},

	// Taken from BenchmarkScanFilter in pkg/sql/bench/bench_test.go.
	{"ScanFilter", `SELECT * FROM scan2 WHERE a IN (1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 20, 21, 23) AND b < 10*a`},

	// Taken from BenchmarkWideTableIgnoreColumns in pkg/sql/bench/bench_test.go.
	{"WideTableIgnoreColumns", `SELECT COUNT(*) FROM widetable WHERE f4 < 10`},

	// Taken from BenchmarkIndexJoin in pkg/sql/bench_test.go.
	{"IndexJoin", `SELECT * from tidx WHERE v < 1000`},
}

func init() {
	security.SetAssetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
}

// BenchmarkPhases measures the time that each of the optimization phases takes
// to run, *inclusive* of the previous phases. For example, the "Prepare" phase
// benchmark will measure the time it takes to parse the SQL, build the opt
// expression tree, and run normalization rules over it. The "Search" phase
// will measure all of that *plus* the time to run exploration rules, cost, and
// add enforcers.
// NOTE: The v20 and v21 phases are mostly there to easily see the performance
//       of queries over empty tables, which is usually not that interesting.
//       To see performance over non-empty tables, run the main SQL
//       bench_test.go benchmarks (and set enableCockroachOpt = true at the top
//       of foreachdb.go).
func BenchmarkPhases(b *testing.B) {
	bm := newBenchmark(b)
	defer bm.close()

	for _, query := range queries {
		bm.run(b, parse, query)
		bm.run(b, optbuild, query)
		bm.run(b, prepare, query)
		bm.run(b, search, query)
		bm.run(b, execbuild, query)
		bm.run(b, execute, query)

		// Uncomment to see performance of v2.0 planner vs. new v2.1 optimizer
		// (but executes the queries over empty tables).
		// bm.run(b, v20, query)
		// bm.run(b, v21, query)
	}
}

type benchmark struct {
	s  serverutils.TestServerInterface
	db *gosql.DB
	sr *sqlutils.SQLRunner
}

func newBenchmark(b *testing.B) *benchmark {
	bm := &benchmark{}
	bm.s, bm.db, _ = serverutils.StartServer(b, base.TestServerArgs{UseDatabase: "bench"})

	bm.sr = sqlutils.MakeSQLRunner(bm.db)
	bm.sr.Exec(b, `CREATE DATABASE bench`)
	bm.sr.Exec(b, `CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT)`)
	bm.sr.Exec(b, `CREATE TABLE bench.scan (k INT PRIMARY KEY)`)
	bm.sr.Exec(b, `CREATE TABLE bench.select (k INT PRIMARY KEY, a INT, b INT, c INT, d INT)`)
	bm.sr.Exec(b, `CREATE TABLE bench.count (k INT PRIMARY KEY, v TEXT)`)
	bm.sr.Exec(b, `CREATE TABLE scan2 (a INT, b INT, PRIMARY KEY (a, b))`)
	bm.sr.Exec(b, `CREATE TABLE widetable (
		f1 INT, f2 INT, f3 INT, f4 INT, f5 INT, f6 INT, f7 INT, f8 INT, f9 INT, f10 INT,
		f11 TEXT, f12 TEXT, f13 TEXT, f14 TEXT, f15 TEXT, f16 TEXT, f17 TEXT, f18 TEXT, f19 TEXT,
		f20 TEXT,
		PRIMARY KEY (f1, f2, f3))`)
	bm.sr.Exec(b, `CREATE TABLE tidx (
		k INT NOT NULL,
		v INT NULL,
		extra STRING NULL,
		CONSTRAINT "primary" PRIMARY KEY (k ASC),
		INDEX idx (v ASC),
		FAMILY "primary" (k, v, extra))`)

	return bm
}

func (bm *benchmark) close() {
	bm.s.Stopper().Stop(context.TODO())
}

func (bm *benchmark) run(b *testing.B, bmType benchmarkType, query benchQuery) {
	b.Run(fmt.Sprintf("%s/%s", query.name, benchmarkTypeStrings[bmType]), func(b *testing.B) {
		switch bmType {
		case v20, v21:
			bm.runUsingSQLRunner(b, bmType, query.query)

		default:
			bm.runUsingAPI(b, bmType, query.query)
		}
	})
}

func (bm *benchmark) runUsingAPI(b *testing.B, bmType benchmarkType, query string) {
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext(false /* privileged */)
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	eng := bm.s.Executor().(exec.TestEngineFactory).NewTestEngine("bench")
	defer eng.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt, err := parser.ParseOne(query)
		if err != nil {
			b.Fatalf("%v", err)
		}

		if bmType == parse {
			continue
		}

		opt := xform.NewOptimizer(&evalCtx)
		if bmType == optbuild {
			opt.DisableOptimizations()
		}
		bld := optbuilder.New(ctx, &semaCtx, &evalCtx, eng.Catalog(), opt.Factory(), stmt)
		root, props, err := bld.Build()
		if err != nil {
			b.Fatalf("%v", err)
		}

		if bmType == optbuild || bmType == prepare {
			continue
		}

		ev := opt.Optimize(root, props)

		if bmType == search {
			continue
		}

		node, err := execbuilder.New(eng.Factory(), ev).Build()
		if err != nil {
			b.Fatalf("%v", err)
		}

		if bmType == execbuild {
			continue
		}

		// execute the node tree.
		_, err = eng.Execute(node)
		if err != nil {
			b.Fatalf("%v", err)
		}
	}
}

func (bm *benchmark) runUsingSQLRunner(b *testing.B, bmType benchmarkType, query string) {
	if bmType == v20 {
		// TODO(radu): remove this once the optimizer plays nice with distsql.
		bm.sr.Exec(b, `SET DISTSQL=OFF`)
		bm.sr.Exec(b, `SET EXPERIMENTAL_OPT=OFF`)
	} else {
		bm.sr.Exec(b, `SET EXPERIMENTAL_OPT=ON`)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bm.sr.Exec(b, query)
	}
}
