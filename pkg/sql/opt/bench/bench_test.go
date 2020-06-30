// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bench

import (
	"bytes"
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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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

	// ExecBuild calls a stub factory to construct a dummy plan from the optimized
	// Memo. Since the factory is not creating a real plan, only a part of the
	// execbuild time is captured.
	ExecBuild

	// EndToEnd executes the query end-to-end using the cost-based optimizer.
	EndToEnd
)

var benchmarkTypeStrings = [...]string{
	Parse:     "Parse",
	OptBuild:  "OptBuild",
	Normalize: "Normalize",
	Explore:   "Explore",
	ExecBuild: "ExecBuild",
	EndToEnd:  "EndToEnd",
}

type benchQuery struct {
	name    string
	query   string
	args    []interface{}
	prepare bool
}

var schemas = [...]string{
	`CREATE TABLE kv (k BIGINT NOT NULL PRIMARY KEY, v BYTES NOT NULL)`,
	`
	CREATE TABLE customer
	(
		c_id           integer        not null,
		c_d_id         integer        not null,
		c_w_id         integer        not null,
		c_first        varchar(16),
		c_middle       char(2),
		c_last         varchar(16),
		c_street_1     varchar(20),
		c_street_2     varchar(20),
		c_city         varchar(20),
		c_state        char(2),
		c_zip          char(9),
		c_phone        char(16),
		c_since        timestamp,
		c_credit       char(2),
		c_credit_lim   decimal(12,2),
		c_discount     decimal(4,4),
		c_balance      decimal(12,2),
		c_ytd_payment  decimal(12,2),
		c_payment_cnt  integer,
		c_delivery_cnt integer,
		c_data         varchar(500),
		primary key (c_w_id, c_d_id, c_id),
		index customer_idx (c_w_id, c_d_id, c_last, c_first)
	)
	`,
	`
	CREATE TABLE new_order
	(
		no_o_id  integer   not null,
		no_d_id  integer   not null,
		no_w_id  integer   not null,
		primary key (no_w_id, no_d_id, no_o_id DESC)
	)
	`,
	`
	CREATE TABLE stock
	(
		s_i_id       integer       not null,
		s_w_id       integer       not null,
		s_quantity   integer,
		s_dist_01    char(24),
		s_dist_02    char(24),
		s_dist_03    char(24),
		s_dist_04    char(24),
		s_dist_05    char(24),
		s_dist_06    char(24),
		s_dist_07    char(24),
		s_dist_08    char(24),
		s_dist_09    char(24),
		s_dist_10    char(24),
		s_ytd        integer,
		s_order_cnt  integer,
		s_remote_cnt integer,
		s_data       varchar(50),
		primary key (s_w_id, s_i_id),
		index stock_item_fk_idx (s_i_id)
	)
	`,
	`
	CREATE TABLE order_line
	(
		ol_o_id         integer   not null,
		ol_d_id         integer   not null,
		ol_w_id         integer   not null,
		ol_number       integer   not null,
		ol_i_id         integer   not null,
		ol_supply_w_id  integer,
		ol_delivery_d   timestamp,
		ol_quantity     integer,
		ol_amount       decimal(6,2),
		ol_dist_info    char(24),
		primary key (ol_w_id, ol_d_id, ol_o_id DESC, ol_number),
		index order_line_fk (ol_supply_w_id, ol_i_id),
		foreign key (ol_supply_w_id, ol_i_id) references stock (s_w_id, s_i_id)
	)
	`,
	`
	CREATE TABLE j
	(
	  a INT PRIMARY KEY,
	  b INT,
	  INDEX (b)
	)
  `,
}

var queries = [...]benchQuery{
	// 1. Table with small number of columns.
	// 2. Table with no indexes.
	// 3. Very simple query that returns single row based on key filter.
	{
		name:    "kv-read",
		query:   `SELECT k, v FROM kv WHERE k IN ($1)`,
		args:    []interface{}{1},
		prepare: true,
	},

	// 1. No PREPARE phase, only EXECUTE.
	{
		name:    "kv-read-no-prep",
		query:   `SELECT k, v FROM kv WHERE k IN ($1)`,
		args:    []interface{}{1},
		prepare: false,
	},

	// 1. PREPARE with constant filter value (no placeholders).
	{
		name:    "kv-read-const",
		query:   `SELECT k, v FROM kv WHERE k IN (1)`,
		args:    []interface{}{},
		prepare: true,
	},

	// 1. Table with many columns.
	// 2. Multi-column primary key.
	// 3. Mutiple indexes to consider.
	// 4. Multiple placeholder values.
	{
		name: "tpcc-new-order",
		query: `
			SELECT c_discount, c_last, c_credit
			FROM customer
			WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3
		`,
		args:    []interface{}{10, 100, 50},
		prepare: true,
	},

	// 1. ORDER BY clause.
	// 2. LIMIT clause.
	// 3. Best plan requires reverse scan.
	{
		name: "tpcc-delivery",
		query: `
			SELECT no_o_id
			FROM new_order
			WHERE no_w_id = $1 AND no_d_id = $2
			ORDER BY no_o_id ASC
			LIMIT 1
		`,
		args:    []interface{}{10, 100},
		prepare: true,
	},

	// 1. Count and Distinct aggregate functions.
	// 2. Simple join.
	// 3. Best plan requires lookup join.
	// 4. Placeholders used in larger constant expressions.
	{
		name: "tpcc-stock-level",
		query: `
			SELECT count(DISTINCT s_i_id)
			FROM order_line
			JOIN stock
			ON s_i_id=ol_i_id AND s_w_id=ol_w_id
			WHERE ol_w_id = $1
				AND ol_d_id = $2
				AND ol_o_id BETWEEN $3 - 20 AND $3 - 1
				AND s_quantity < $4
		`,
		args:    []interface{}{10, 100, 1000, 15},
		prepare: true,
	},
}

func init() {
	security.SetAssetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
}

var profileTime = flag.Duration("profile-time", 10*time.Second, "duration of profiling run")
var profileType = flag.String("profile-type", "ExecBuild", "Parse, OptBuild, Normalize, Explore, ExecBuild, EndToEnd")
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
	t.Skip(
		"Remove this when profiling. Use profile flags above to configure. Sample command line: \n" +
			"GOMAXPROCS=1 go test -run TestCPUProfile --logtostderr NONE && go tool pprof bench.test cpu.out",
	)

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
		bm.runForBenchmark(b, ExecBuild, query)
	}
}

// BenchmarkEndToEnd measures the time to execute a query end-to-end.
func BenchmarkEndToEnd(b *testing.B) {
	h := newHarness()
	defer h.close()

	for _, query := range queries {
		h.runForBenchmark(b, EndToEnd, query)
	}
}

type harness struct {
	ctx       context.Context
	semaCtx   tree.SemaContext
	evalCtx   tree.EvalContext
	prepMemo  *memo.Memo
	cat       *testcat.Catalog
	optimizer xform.Optimizer

	s  serverutils.TestServerInterface
	db *gosql.DB
	sr *sqlutils.SQLRunner

	bmType   BenchmarkType
	query    benchQuery
	prepared *gosql.Stmt
	ready    bool
}

func newHarness() *harness {
	return &harness{}
}

func (h *harness) close() {
	if h.s != nil {
		h.s.Stopper().Stop(context.Background())
	}
}

func (h *harness) runForProfiling(
	t *testing.T, bmType BenchmarkType, query benchQuery, duration time.Duration,
) {
	h.bmType = bmType
	h.query = query
	h.prepare(t)

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
			case EndToEnd:
				h.runUsingServer(t)

			default:
				h.runUsingAPI(t, bmType, query.prepare)
			}
		}
	}
}

func (h *harness) runForBenchmark(b *testing.B, bmType BenchmarkType, query benchQuery) {
	h.bmType = bmType
	h.query = query
	h.prepare(b)

	b.Run(fmt.Sprintf("%s/%s", query.name, benchmarkTypeStrings[bmType]), func(b *testing.B) {
		switch bmType {
		case EndToEnd:
			for i := 0; i < b.N; i++ {
				h.runUsingServer(b)
			}

		default:
			for i := 0; i < b.N; i++ {
				h.runUsingAPI(b, bmType, query.prepare)
			}
		}
	})
}

func (h *harness) prepare(tb testing.TB) {
	switch h.bmType {
	case EndToEnd:
		h.prepareUsingServer(tb)

	default:
		h.prepareUsingAPI(tb)
	}
}

func (h *harness) prepareUsingServer(tb testing.TB) {
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

	if h.query.prepare {
		var err error
		h.prepared, err = h.db.Prepare(h.query.query)
		if err != nil {
			tb.Fatalf("%v", err)
		}
	} else {
		h.prepared = nil
	}
}

func (h *harness) runUsingServer(tb testing.TB) {
	var err error
	if h.prepared != nil {
		_, err = h.prepared.Exec(h.query.args...)
		if err != nil {
			tb.Fatalf("%v", err)
		}
	} else {
		h.sr.Exec(tb, h.query.query, h.query.args...)
	}
}

func (h *harness) prepareUsingAPI(tb testing.TB) {
	// Clear any state from previous usage of this harness instance.
	h.ctx = context.Background()
	h.semaCtx = tree.MakeSemaContext()
	h.evalCtx = tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	h.prepMemo = nil
	h.cat = nil
	h.optimizer = xform.Optimizer{}

	// Set up the catalog.
	h.cat = testcat.New()
	for _, schema := range schemas {
		_, err := h.cat.ExecuteDDL(schema)
		if err != nil {
			tb.Fatalf("%v", err)
		}
	}

	if err := h.semaCtx.Placeholders.Init(len(h.query.args), nil /* typeHints */); err != nil {
		tb.Fatal(err)
	}
	if h.query.prepare {
		// Prepare the query by normalizing it (if it has placeholders) or exploring
		// it (if it doesn't have placeholders), and cache the resulting memo so that
		// it can be used during execution.
		if len(h.query.args) > 0 {
			h.runUsingAPI(tb, Normalize, false /* usePrepared */)
		} else {
			h.runUsingAPI(tb, Explore, false /* usePrepared */)
		}
		h.prepMemo = h.optimizer.DetachMemo()
	} else {
		// Run optbuilder to infer any placeholder types.
		h.runUsingAPI(tb, OptBuild, false /* usePrepared */)
	}

	// Construct placeholder values.
	h.semaCtx.Placeholders.Values = make(tree.QueryArguments, len(h.query.args))
	for i, arg := range h.query.args {
		var parg tree.Expr
		parg, err := parser.ParseExpr(fmt.Sprintf("%v", arg))
		if err != nil {
			tb.Fatalf("%v", err)
		}

		id := tree.PlaceholderIdx(i)
		typ, _ := h.semaCtx.Placeholders.ValueType(id)
		texpr, err := sqlbase.SanitizeVarFreeExpr(
			context.Background(),
			parg,
			typ,
			"", /* context */
			&h.semaCtx,
			tree.VolatilityVolatile,
		)
		if err != nil {
			tb.Fatalf("%v", err)
		}

		h.semaCtx.Placeholders.Values[i] = texpr
	}
	h.evalCtx.Placeholders = &h.semaCtx.Placeholders
	h.evalCtx.Annotations = &h.semaCtx.Annotations
}

func (h *harness) runUsingAPI(tb testing.TB, bmType BenchmarkType, usePrepared bool) {
	var stmt parser.Statement
	var err error
	if !usePrepared {
		stmt, err = parser.ParseOne(h.query.query)
		if err != nil {
			tb.Fatalf("%v", err)
		}
	}

	if bmType == Parse {
		return
	}

	h.optimizer.Init(&h.evalCtx, h.cat)
	if bmType == OptBuild {
		h.optimizer.DisableOptimizations()
	}

	if !usePrepared {
		bld := optbuilder.New(h.ctx, &h.semaCtx, &h.evalCtx, h.cat, h.optimizer.Factory(), stmt.AST)
		if err = bld.Build(); err != nil {
			tb.Fatalf("%v", err)
		}
	} else if h.prepMemo.HasPlaceholders() {
		_ = h.optimizer.Factory().AssignPlaceholders(h.prepMemo)
	}

	if bmType == OptBuild || bmType == Normalize {
		return
	}

	var execMemo *memo.Memo
	if usePrepared && !h.prepMemo.HasPlaceholders() {
		execMemo = h.prepMemo
	} else {
		if _, err := h.optimizer.Optimize(); err != nil {
			panic(err)
		}
		execMemo = h.optimizer.Memo()
	}

	if bmType == Explore {
		return
	}

	root := execMemo.RootExpr()
	execFactory := stubFactory{}
	eb := execbuilder.New(&execFactory, execMemo, nil /* catalog */, root, &h.evalCtx)
	if _, err = eb.Build(); err != nil {
		tb.Fatalf("%v", err)
	}
}

func makeChain(size int) benchQuery {
	var buf bytes.Buffer
	buf.WriteString(`SELECT * FROM `)
	comma := ""
	for i := 0; i < size; i++ {
		buf.WriteString(comma)
		fmt.Fprintf(&buf, "j AS tab%d", i+1)
		comma = ", "
	}

	if size > 1 {
		buf.WriteString(" WHERE ")
	}

	comma = ""
	for i := 0; i < size-1; i++ {
		buf.WriteString(comma)
		fmt.Fprintf(&buf, "tab%d.a = tab%d.b", i+1, i+2)
		comma = " AND "
	}

	return benchQuery{
		name:  fmt.Sprintf("chain-%d", size),
		query: buf.String(),
	}
}

// BenchmarkChain benchmarks the planning of a "chain" query, where
// some number of tables are joined together, with there being a
// predicate joining the first and second, second and third, third
// and fourth, etc.
//
// For example, a 5-chain looks like:
//
//   SELECT * FROM a, b, c, d, e
//   WHERE a.x = b.y
//     AND b.x = c.y
//     AND c.x = d.y
//     AND d.x = e.y
//
func BenchmarkChain(b *testing.B) {
	h := newHarness()
	defer h.close()

	for i := 1; i < 20; i++ {
		q := makeChain(i)
		for i := 0; i < b.N; i++ {
			h.runForBenchmark(b, Explore, q)
		}
	}
}
