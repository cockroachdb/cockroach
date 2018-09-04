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
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
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
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type BenchmarkType int

const (
	// Parse creates the AST.
	Parse BenchmarkType = iota

	// OptBuild constructs the Memo from the AST. It runs no normalization or
	// exploration rules. OptBuild does not include the time to Parse.
	OptBuild

	// Normalize constructs the Memo from the AST, but enables all normalization
	// rules, unlike OptBuild. No Explore rules are enabled. Normalize includes
	// the time to OptBuild.
	Normalize

	// Explore constructs the Memo from the AST and enables all normalization
	// and exploration rules. The Memo is fully optimized. Explore includes the
	// time to OptBuild and Normalize.
	Explore

	// ExecPlan executes the query end-to-end using the heuristic planner.
	ExecPlan

	// ExecOpt executes the query end-to-end using the cost-based optimizer.
	ExecOpt
)

var benchmarkTypeStrings = [...]string{
	Parse:     "Parse",
	OptBuild:  "OptBuild",
	Normalize: "Normalize",
	Explore:   "Explore",
	ExecPlan:  "ExecPlan",
	ExecOpt:   "ExecOpt",
}

type benchQuery struct {
	name  string
	query string
}

var schemas = [...]string{
	`CREATE TABLE kv (k BIGINT NOT NULL PRIMARY KEY, v BYTES NOT NULL)`,
	`CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT, INDEX(b), UNIQUE INDEX(c))`,
}

var queries = [...]benchQuery{
	{"kv-read", `SELECT k, v FROM kv WHERE k IN (5)`},
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

var profileTime = flag.Duration("profile-time", 10*time.Second, "duration of profiling run")
var profileType = flag.String("profile-type", "Explore", "Parse, OptBuild, Normalize, Explore, ExecPlan, ExecOpt")
var profileQuery = flag.String("profile-query", "kv-read", "name of query to run")

// TestCPUProfile executes the configured profileQuery in a loop in order to
// profile its CPU usage. Rather than allow the Go testing infrastructure to
// start profiling, TestCPUProfile triggers startup, so that it has control over
// when profiling starts. In particular, profiling is only started once the
// server or API has been initialized, so that the profiles don't include
// startup activities.
//
// TestCPUProfile writes the output profile to a cpu.out file in the current
// directory. See the profile flags for ways to configure what is profiled.
func TestCPUProfile(t *testing.T) {
	t.Skip("Remove this when profiling.")

	h := newHarness()
	defer h.close()

	var query benchQuery
	for _, query = range queries {
		if query.name == *profileQuery {
			break
		}
	}

	var bmType BenchmarkType
	for i, s := range benchmarkTypeStrings {
		if s == *profileType {
			bmType = BenchmarkType(i)
		}
	}

	h.runForProfiling(t, bmType, query, *profileTime)
}

// BenchmarkPhases measures the time that each of the optimization phases takes
// to run. See the comments for the BenchmarkType enumeration for more details
// on what each phase includes.
func BenchmarkPhases(b *testing.B) {
	bm := newHarness()
	defer bm.close()

	for _, query := range queries {
		bm.runForBenchmark(b, Parse, query)
		bm.runForBenchmark(b, OptBuild, query)
		bm.runForBenchmark(b, Normalize, query)
		bm.runForBenchmark(b, Explore, query)
	}
}

// BenchmarkExec measures the time to execute a query end-to-end using both the
// heuristic planner and the cost-based optimizer.
func BenchmarkExec(b *testing.B) {
	h := newHarness()
	defer h.close()

	for _, query := range queries {
		h.runForBenchmark(b, ExecPlan, query)
		h.runForBenchmark(b, ExecOpt, query)
	}
}

type harness struct {
	ctx       context.Context
	semaCtx   tree.SemaContext
	evalCtx   tree.EvalContext
	stmt      tree.Statement
	cat       *testcat.Catalog
	optimizer xform.Optimizer

	s  serverutils.TestServerInterface
	db *gosql.DB
	sr *sqlutils.SQLRunner

	ready bool
}

func newHarness() *harness {
	return &harness{}
}

func (h *harness) close() {
	if h.s != nil {
		h.s.Stopper().Stop(context.TODO())
	}
}

func (h *harness) runForProfiling(
	t *testing.T, bmType BenchmarkType, query benchQuery, duration time.Duration,
) {
	sql := query.query
	h.prepare(t, bmType, sql)

	f, err := os.Create("cpu.out")
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer f.Close()

	err = pprof.StartCPUProfile(f)
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer pprof.StopCPUProfile()

	start := timeutil.Now()
	for {
		now := timeutil.Now()
		if now.Sub(start) > duration {
			break
		}

		// Minimize overhead of getting timings by iterating 1000 times before
		// checking if done.
		for i := 0; i < 1000; i++ {
			switch bmType {
			case ExecPlan, ExecOpt:
				h.runUsingServer(t, sql)

			default:
				h.runUsingAPI(t, bmType, sql)
			}
		}
	}
}

func (h *harness) runForBenchmark(b *testing.B, bmType BenchmarkType, query benchQuery) {
	sql := query.query
	h.prepare(b, bmType, sql)

	b.Run(fmt.Sprintf("%s/%s", query.name, benchmarkTypeStrings[bmType]), func(b *testing.B) {
		switch bmType {
		case ExecPlan, ExecOpt:
			for i := 0; i < b.N; i++ {
				h.runUsingServer(b, sql)
			}

		default:
			for i := 0; i < b.N; i++ {
				h.runUsingAPI(b, bmType, sql)
			}
		}
	})
}

func (h *harness) prepare(tb testing.TB, bmType BenchmarkType, query string) {
	switch bmType {
	case ExecPlan, ExecOpt:
		h.prepareUsingServer(tb, bmType)

	default:
		h.prepareUsingAPI(tb, query)
	}
}

func (h *harness) prepareUsingServer(tb testing.TB, bmType BenchmarkType) {
	if !h.ready {
		// Set up database.
		h.s, h.db, _ = serverutils.StartServer(tb, base.TestServerArgs{UseDatabase: "bench"})
		h.sr = sqlutils.MakeSQLRunner(h.db)
		h.sr.Exec(tb, `CREATE DATABASE bench`)
		for _, schema := range schemas {
			h.sr.Exec(tb, schema)
		}
		h.ready = true
	}

	// Set session state.
	if bmType == ExecPlan {
		h.sr.Exec(tb, `SET OPTIMIZER=OFF`)
	} else {
		h.sr.Exec(tb, `SET OPTIMIZER=ON`)
	}
}

func (h *harness) runUsingServer(tb testing.TB, query string) {
	h.sr.Exec(tb, query)
}

func (h *harness) prepareUsingAPI(tb testing.TB, query string) {
	h.ctx = context.Background()
	h.semaCtx = tree.MakeSemaContext(false /* privileged */)
	h.evalCtx = tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	h.cat = testcat.New()
	for _, schema := range schemas {
		_, err := h.cat.ExecuteDDL(schema)
		if err != nil {
			tb.Fatalf("%v", err)
		}
	}

	stmt, err := parser.ParseOne(query)
	if err != nil {
		tb.Fatalf("%v", err)
	}
	h.stmt = stmt
}

func (h *harness) runUsingAPI(tb testing.TB, bmType BenchmarkType, query string) {
	if bmType == Parse {
		_, err := parser.ParseOne(query)
		if err != nil {
			tb.Fatalf("%v", err)
		}
		return
	}

	h.optimizer.Init(&h.evalCtx)
	if bmType == OptBuild {
		h.optimizer.DisableOptimizations()
	}
	bld := optbuilder.New(h.ctx, &h.semaCtx, &h.evalCtx, h.cat, h.optimizer.Factory(), h.stmt)
	root, props, err := bld.Build()
	if err != nil {
		tb.Fatalf("%v", err)
	}

	if bmType == OptBuild || bmType == Normalize {
		return
	}

	h.optimizer.Optimize(root, props)
}
