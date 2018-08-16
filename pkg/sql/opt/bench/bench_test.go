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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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
	normalize

	// normalize + exploration patterns, enforcers, and costing.
	explore
)

var benchmarkTypeStrings = [...]string{
	parse:     "Parse",
	optbuild:  "OptBuild",
	normalize: "Normalize",
	explore:   "Explore",
}

type benchQuery struct {
	name  string
	query string
}

var queries = [...]benchQuery{
	{"kv-read", `SELECT k, v FROM kv WHERE k IN ($1)`},
	{"planning1", `SELECT * FROM abc`},
	{"planning2", `SELECT * FROM abc WHERE a > 5 ORDER BY a`},
	{"planning3", `SELECT * FROM abc WHERE b = 5`},
	{"planning4", `SELECT * FROM abc WHERE b = 5 ORDER BY a`},
	{"planning5", `SELECT * FROM abc WHERE c = 5`},
	{"planning6", `SELECT * FROM abc JOIN abc AS abc2 ON abc.a = abc2.a`},
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
		bm.run(b, normalize, query)
		bm.run(b, explore, query)
	}
}

type benchmark struct {
}

func newBenchmark(_ *testing.B) *benchmark {
	return &benchmark{}
}

func (bm *benchmark) close() {
}

func (bm *benchmark) run(b *testing.B, bmType benchmarkType, query benchQuery) {
	b.Run(fmt.Sprintf("%s/%s", query.name, benchmarkTypeStrings[bmType]), func(b *testing.B) {
		bm.runUsingAPI(b, bmType, query.query)
	})
}

func (bm *benchmark) runUsingAPI(b *testing.B, bmType benchmarkType, query string) {
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext(false /* privileged */)
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	cat := testcat.New()
	bm.executeDDL(b, cat, `CREATE TABLE kv (k BIGINT NOT NULL PRIMARY KEY, v BYTES NOT NULL)`)
	bm.executeDDL(b, cat, `CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT, INDEX(b), UNIQUE INDEX(c))`)

	stmt, err := parser.ParseOne(query)
	if err != nil {
		b.Fatalf("%v", err)
	}

	var opt xform.Optimizer

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if bmType == parse {
			stmt, _ = parser.ParseOne(query)
			continue
		}

		opt.Init(&evalCtx)
		if bmType == optbuild {
			opt.DisableOptimizations()
		}
		bld := optbuilder.New(ctx, &semaCtx, &evalCtx, cat, opt.Factory(), stmt)
		root, props, err := bld.Build()
		if err != nil {
			b.Fatalf("%v", err)
		}

		if bmType == optbuild || bmType == normalize {
			continue
		}

		opt.Optimize(root, props)

		if bmType == explore {
			continue
		}
	}
}

func (bm *benchmark) executeDDL(b *testing.B, cat *testcat.Catalog, sql string) {
	_, err := cat.ExecuteDDL(sql)
	if err != nil {
		b.Fatalf("%v", err)
	}
}
