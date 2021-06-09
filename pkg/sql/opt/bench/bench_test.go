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
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/execbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// A query can be issued using the "simple protocol" or the "prepare protocol".
//
// With the simple protocol, all arguments are inlined in the SQL string; the
// query goes through all phases of planning on each execution. Only these
// phases are valid with the simple protocol:
//   - Parse
//   - OptBuildNoNorm
//   - OptBuildNorm
//   - Explore
//   - ExecBuild
//
// With the prepare protocol, the query is built at prepare time (with
// normalization rules turned on) and the resulting memo is saved and reused. On
// each execution, placeholders are assigned before exploration. Only these
// phases are valid with the prepare protocol:
//  - AssignPlaceholdersNoNorm
//  - AssignPlaceholdersNorm
//  - Explore
//  - ExecBuild
type Phase int

const (
	// Parse creates the AST from the SQL string.
	Parse Phase = iota

	// OptBuildNoNorm constructs the Memo from the AST, with normalization rules
	// disabled. OptBuildNoNorm includes the time to Parse.
	OptBuildNoNorm

	// OptBuildNorm constructs the Memo from the AST, with normalization rules
	// enabled. OptBuildNorm includes the time to Parse.
	OptBuildNorm

	// AssignPlaceholdersNoNorm uses a prepared Memo and assigns placeholders,
	// with normalization rules disabled.
	AssignPlaceholdersNoNorm

	// AssignPlaceholdersNorm uses a prepared Memo and assigns placeholders, with
	// normalization rules enabled.
	AssignPlaceholdersNorm

	// Explore constructs the Memo (either by building it from the statement or by
	// assigning placeholders to a prepared Memo) and enables all normalization
	// and exploration rules. The Memo is fully optimized. Explore includes the
	// time to OptBuildNorm or AssignPlaceholdersNorm.
	Explore

	// ExecBuild calls a stub factory to construct a dummy plan from the optimized
	// Memo. Since the factory is not creating a real plan, only a part of the
	// execbuild time is captured. ExecBuild includes the time to Explore.
	ExecBuild
)

// SimplePhases are the legal phases when running a query that was not prepared.
var SimplePhases = []Phase{Parse, OptBuildNoNorm, OptBuildNorm, Explore, ExecBuild}

// PreparedPhases are the legal phases when running a query that was prepared.
var PreparedPhases = []Phase{AssignPlaceholdersNoNorm, AssignPlaceholdersNorm, Explore, ExecBuild}

func (bt Phase) String() string {
	var strTab = [...]string{
		Parse:                    "Parse",
		OptBuildNoNorm:           "OptBuildNoNorm",
		OptBuildNorm:             "OptBuildNorm",
		AssignPlaceholdersNoNorm: "AssignPlaceholdersNoNorm",
		AssignPlaceholdersNorm:   "AssignPlaceholdersNorm",
		Explore:                  "Explore",
		ExecBuild:                "ExecBuild",
	}
	return strTab[bt]
}

type benchQuery struct {
	name  string
	query string
	args  []interface{}
}

var schemas = []string{
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
		primary key (s_w_id, s_i_id)
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
		name:  "kv-read",
		query: `SELECT k, v FROM kv WHERE k IN ($1)`,
		args:  []interface{}{1},
	},

	// 1. PREPARE with constant filter value (no placeholders).
	{
		name:  "kv-read-const",
		query: `SELECT k, v FROM kv WHERE k IN (1)`,
		args:  []interface{}{},
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
		args: []interface{}{10, 100, 50},
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
		args: []interface{}{10, 100},
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
		args: []interface{}{10, 100, 1000, 15},
	},

	// 1. Table with more than 15 columns (triggers slow path for FastIntMap).
	// 2. Table with many indexes.
	// 3. Query with a single fixed column that can't use any of the indexes.
	{
		name: "many-columns-and-indexes-a",
		query: `
			SELECT id FROM k
			WHERE x = $1
		`,
		args: []interface{}{1},
	},

	// 1. Table with more than 15 columns (triggers slow path for FastIntMap).
	// 2. Table with many indexes.
	// 3. Query that can't use any of the indexes with a more complex filter.
	{
		name: "many-columns-and-indexes-b",
		query: `
			SELECT id FROM k
			WHERE x = $1 AND y = $2 AND z = $3
		`,
		args: []interface{}{1, 2, 3},
	},
}

func init() {
	security.SetAssetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)

	// Add a table with many columns and many indexes.
	var indexes strings.Builder
	for i := 0; i < 250; i++ {
		indexes.WriteString(",\nINDEX (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w)")
	}
	tableK := fmt.Sprintf(`CREATE TABLE k (
		id INT PRIMARY KEY,
		a INT, b INT, c INT, d INT, e INT, f INT, g INT, h INT, i INT, j INT,
		k INT, l INT, m INT, n INT, o INT, p INT, q INT, r INT, s INT, t INT,
		u INT, v INT, w INT, x INT, y INT, z INT
		%s
	)`, indexes.String())
	schemas = append(schemas, tableK)
}

// BenchmarkPhases measures the time that each of the optimization phases takes
// to run. See the comments for the Phase enumeration for more details
// on what each phase includes.
func BenchmarkPhases(b *testing.B) {
	for _, query := range queries {
		h := newHarness(b, query)
		b.Run(query.name, func(b *testing.B) {
			b.Run("Simple", func(b *testing.B) {
				for _, phase := range SimplePhases {
					b.Run(phase.String(), func(b *testing.B) {
						for i := 0; i < b.N; i++ {
							h.runSimple(b, query, phase)
						}
					})
				}
			})
			b.Run("Prepared", func(b *testing.B) {
				phases := PreparedPhases
				if h.prepMemo.IsOptimized() {
					// If the query has no placeholders or the placeholder fast path
					// succeeded, the only phase which does something is ExecBuild.
					phases = []Phase{ExecBuild}
				}
				for _, phase := range phases {
					b.Run(phase.String(), func(b *testing.B) {
						for i := 0; i < b.N; i++ {
							h.runPrepared(b, phase)
						}
					})
				}
			})
		})
	}
}

type harness struct {
	ctx       context.Context
	semaCtx   tree.SemaContext
	evalCtx   tree.EvalContext
	prepMemo  *memo.Memo
	testCat   *testcat.Catalog
	optimizer xform.Optimizer
}

func newHarness(tb testing.TB, query benchQuery) *harness {
	h := &harness{
		ctx:     context.Background(),
		semaCtx: tree.MakeSemaContext(),
		evalCtx: tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings()),
	}

	// Set up the test catalog.
	h.testCat = testcat.New()
	for _, schema := range schemas {
		_, err := h.testCat.ExecuteDDL(schema)
		if err != nil {
			tb.Fatalf("%v", err)
		}
	}

	if err := h.semaCtx.Placeholders.Init(len(query.args), nil /* typeHints */); err != nil {
		tb.Fatal(err)
	}
	// Run optbuilder to build the memo for Prepare. Even if we will not be using
	// the Prepare method, we still want to run the optbuilder to infer any
	// placeholder types.
	stmt, err := parser.ParseOne(query.query)
	if err != nil {
		tb.Fatalf("%v", err)
	}
	h.optimizer.Init(&h.evalCtx, h.testCat)
	bld := optbuilder.New(h.ctx, &h.semaCtx, &h.evalCtx, h.testCat, h.optimizer.Factory(), stmt.AST)
	bld.KeepPlaceholders = true
	if err := bld.Build(); err != nil {
		tb.Fatalf("%v", err)
	}

	// If there are no placeholders, we explore during PREPARE.
	if len(query.args) == 0 {
		if _, err := h.optimizer.Optimize(); err != nil {
			tb.Fatalf("%v", err)
		}
	} else {
		if _, _, err := h.optimizer.TryPlaceholderFastPath(); err != nil {
			tb.Fatalf("%v", err)
		}
	}
	h.prepMemo = h.optimizer.DetachMemo()
	h.optimizer = xform.Optimizer{}

	// Construct placeholder values.
	h.semaCtx.Placeholders.Values = make(tree.QueryArguments, len(query.args))
	for i, arg := range query.args {
		var parg tree.Expr
		parg, err := parser.ParseExpr(fmt.Sprintf("%v", arg))
		if err != nil {
			tb.Fatalf("%v", err)
		}

		id := tree.PlaceholderIdx(i)
		typ, _ := h.semaCtx.Placeholders.ValueType(id)
		texpr, err := schemaexpr.SanitizeVarFreeExpr(
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
	return h
}

// runSimple simulates running a query through the "simple protocol" (no prepare
// step). The placeholders are replaced with their values automatically when we
// build the memo.
func (h *harness) runSimple(tb testing.TB, query benchQuery, phase Phase) {
	stmt, err := parser.ParseOne(query.query)
	if err != nil {
		tb.Fatalf("%v", err)
	}

	if phase == Parse {
		return
	}

	h.optimizer.Init(&h.evalCtx, h.testCat)
	if phase == OptBuildNoNorm {
		h.optimizer.DisableOptimizations()
	}

	bld := optbuilder.New(h.ctx, &h.semaCtx, &h.evalCtx, h.testCat, h.optimizer.Factory(), stmt.AST)
	// Note that KeepPlaceholders is false and we have placeholder values in the
	// evalCtx, so the optbuilder will replace all placeholders with their values.
	if err = bld.Build(); err != nil {
		tb.Fatalf("%v", err)
	}

	if phase == OptBuildNoNorm || phase == OptBuildNorm {
		return
	}

	if _, err := h.optimizer.Optimize(); err != nil {
		panic(err)
	}
	execMemo := h.optimizer.Memo()

	if phase == Explore {
		return
	}

	if phase != ExecBuild {
		tb.Fatalf("invalid phase %s for Simple", phase)
	}

	root := execMemo.RootExpr()
	eb := execbuilder.New(
		exec.StubFactory{}, execMemo, nil /* catalog */, root, &h.evalCtx, true, /* allowAutoCommit */
	)
	if _, err = eb.Build(); err != nil {
		tb.Fatalf("%v", err)
	}
}

// runPrepared simulates running the query after it was prepared.
func (h *harness) runPrepared(tb testing.TB, phase Phase) {
	h.optimizer.Init(&h.evalCtx, h.testCat)

	if !h.prepMemo.IsOptimized() {
		if phase == AssignPlaceholdersNoNorm {
			h.optimizer.DisableOptimizations()
		}
		err := h.optimizer.Factory().AssignPlaceholders(h.prepMemo)
		if err != nil {
			tb.Fatalf("%v", err)
		}
	}

	if phase == AssignPlaceholdersNoNorm || phase == AssignPlaceholdersNorm {
		return
	}

	var execMemo *memo.Memo
	if h.prepMemo.IsOptimized() {
		// No placeholders or the placeholder fast path succeeded; we already did
		// the exploration at prepare time.
		execMemo = h.prepMemo
	} else {
		if _, err := h.optimizer.Optimize(); err != nil {
			tb.Fatalf("%v", err)
		}
		execMemo = h.optimizer.Memo()
	}

	if phase == Explore {
		return
	}

	if phase != ExecBuild {
		tb.Fatalf("invalid phase %s for Prepared", phase)
	}

	root := execMemo.RootExpr()
	eb := execbuilder.New(
		exec.StubFactory{}, execMemo, nil /* catalog */, root, &h.evalCtx, true, /* allowAutoCommit */
	)
	if _, err := eb.Build(); err != nil {
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
	for i := 1; i < 20; i++ {
		q := makeChain(i)
		h := newHarness(b, q)
		b.Run(q.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				h.runSimple(b, q, Explore)
			}
		})
	}
}

// BenchmarkEndToEnd measures the time to execute a query end-to-end (against a
// test server).
func BenchmarkEndToEnd(b *testing.B) {
	defer log.Scope(b).Close(b)

	// Set up database.
	srv, db, _ := serverutils.StartServer(b, base.TestServerArgs{UseDatabase: "bench"})
	defer srv.Stopper().Stop(context.Background())
	sr := sqlutils.MakeSQLRunner(db)
	sr.Exec(b, `CREATE DATABASE bench`)
	for _, schema := range schemas {
		sr.Exec(b, schema)
	}

	for _, query := range queries {
		b.Run(query.name, func(b *testing.B) {
			b.Run("Simple", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					sr.Exec(b, query.query, query.args...)
				}
			})
			b.Run("Prepared", func(b *testing.B) {
				prepared, err := db.Prepare(query.query)
				if err != nil {
					b.Fatalf("%v", err)
				}
				for i := 0; i < b.N; i++ {
					if _, err = prepared.Exec(query.args...); err != nil {
						b.Fatalf("%v", err)
					}
				}
			})
		})
	}
}
