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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
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
	for _, query := range queriesToTest(b) {
		h := newHarness(b, query, schemas)
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

func newHarness(tb testing.TB, query benchQuery, schemas []string) *harness {
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

	if err := h.semaCtx.Placeholders.Init(len(query.args), nil /* typeHints */, false /* fromSQL */); err != nil {
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
		explain.NewPlanGistFactory(exec.StubFactory{}),
		&h.optimizer,
		execMemo,
		nil, /* catalog */
		root,
		&h.evalCtx,
		true, /* allowAutoCommit */
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
		explain.NewPlanGistFactory(exec.StubFactory{}),
		&h.optimizer,
		execMemo,
		nil, /* catalog */
		root,
		&h.evalCtx,
		true, /* allowAutoCommit */
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

func makeQueryWithORs(size int) benchQuery {
	var buf bytes.Buffer
	buf.WriteString(`SELECT * FROM stock WHERE `)
	sep := ""
	for i := 0; i < size; i++ {
		buf.WriteString(sep)
		fmt.Fprintf(&buf, "s_w_id = %d AND s_order_cnt = %d", i, i)
		sep = " OR "
	}
	return benchQuery{
		name:  fmt.Sprintf("ored-preds-%d", size),
		query: buf.String(),
	}
}

func makeParameterizedQueryWithORs(size int) benchQuery {
	var buf bytes.Buffer
	buf.WriteString(`SELECT * FROM stock WHERE `)
	sep := ""
	numParams := size
	parameterValues := make([]interface{}, numParams)
	for i := 1; i <= numParams; i++ {
		parameterValues[i-1] = i
		buf.WriteString(sep)
		fmt.Fprintf(&buf, "s_w_id = $%d AND s_order_cnt = $%d", i, i)
		sep = " OR "
	}
	return benchQuery{
		name:  fmt.Sprintf("ored-preds-using-params-%d", size),
		query: buf.String(),
		args:  parameterValues,
	}
}

// makeOredPredsTests constructs a set of non-parameterized queries and
// parameterized queries with a certain number of ORed predicates as indicated
// in the testSizes array and test name suffix. The test names produced are:
// ored-preds-100
// ored-preds-using-params-100
func makeOredPredsTests(b *testing.B) []benchQuery {
	// Add more entries to this array to test with different numbers of ORed
	// predicates.
	testSizes := [...]int{100}
	benchQueries := make([]benchQuery, len(testSizes)*2)
	for i := 0; i < len(testSizes); i++ {
		benchQueries[i] = makeQueryWithORs(testSizes[i])
	}
	for i := len(testSizes); i < len(testSizes)*2; i++ {
		benchQueries[i] = makeParameterizedQueryWithORs(testSizes[i-len(testSizes)])
	}
	return benchQueries
}

func queriesToTest(b *testing.B) []benchQuery {
	allQueries := append(queries[:], makeOredPredsTests(b)...)
	return allQueries
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
		h := newHarness(b, q, schemas)
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

	for _, query := range queriesToTest(b) {
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

var slowSchemas = []string{
	`
    CREATE TABLE tab1 (
        col1 INT8 NOT NULL,
        col2 INT8 NOT NULL,
        col3 INT8 NOT NULL,
        col4 INT8 NULL,
        col5 INT8 NULL,
        col6 INT8 NOT NULL,
        col7 INT8 NOT NULL,
        col8 INT8 NOT NULL,
        col9 INT8 NOT NULL,
        col10 INT8 NOT NULL,
        col11 INT8 NULL,
        col12 INT8 NULL,
        col13 INT8 NULL,
        col14 INT8 NULL,
        col15 INT8 NULL,
        col16 INT8 NOT NULL,
        col17 INT8 NOT NULL,
        col18 INT8 NULL,
        col19 INT8 NOT NULL,
        col20 INT8 NOT NULL,
        col21 INT8 NOT NULL,
        col22 INT8 NULL,
        col23 INT8 NULL,
        col24 INT8 NOT NULL,
        col25 INT8 NOT NULL,
        col26 INT8 NULL,
        col27 INT8 NULL,
        col28 INT8 NOT NULL,
        col29 INT8 NULL,
        col30 INT8 NOT NULL,
        col31 INT8 NOT NULL,
        col32 INT8 NULL,
        col33 INT8 NULL,
        col34 INT8 NULL,
        col35 INT8 NULL,
        col36 INT8 NULL,
        col37 INT8 NOT NULL,
        col38 INT8 NULL,
        col39 INT8 NULL,
        col40 INT8 NOT NULL,
        col41 INT8 NULL,
        col42 INT8 NULL,
        col43 INT8 NULL,
        col44 INT8 NULL,
        col45 INT8 NULL,
        col46 INT8 NULL,
        col47 INT8 NULL,
        col48 INT8 NULL,
        col49 INT8 NULL,
        col50 INT8 NULL,
        col51 INT8 NULL,
        col52 INT8 NULL,
        col53 INT8 NULL,
        col54 INT8 NULL,
        col55 INT8 NULL,
        col56 INT8 NULL,
        col57 INT8 NULL,
        col58 INT8 NULL,
        col59 INT8 NOT NULL,
        col60 INT8 NOT NULL,
        col61 INT8 NULL,
        col62 INT8 NOT NULL,
        col63 INT8 NULL,
        col64 INT8 NULL,
        col65 INT8 NULL,
        col66 INT8 NULL,
        col67 INT8 NOT NULL,
        col68 INT8 NULL,
        col69 INT8 NULL,
        col70 INT8 NULL,
        col71 INT8 NULL,
        col72 INT8 NULL,
        col73 INT8 NULL,
        col74 INT8 NULL,
        col75 INT8 NULL,
        col76 INT8 NULL,
        col77 INT8 NULL,
        col78 INT8 NULL,
        col79 INT8 NULL,
        col80 INT8 NULL,
        col81 INT8 NULL,
        col82 INT8 NULL,
        col83 INT8 NULL,
        col84 INT8 NULL,
        col85 INT8 NOT NULL,
        col86 INT8 NULL,
        col87 INT8 NULL,
        col88 INT8 NULL,
        col89 INT8 NULL,
        col90 INT8 NULL,
        col91 INT8 NULL,
        col92 INT8 NULL,
        col93 INT8 NULL,
        col94 INT8 NULL,
        col95 INT8 NULL,
        col96 INT8 NULL,
        col97 INT8 NULL,
        col98 INT8 NULL,
        col99 INT8 NULL,
        col100 INT8 NULL,
        col101 INT8 NULL,
        col102 INT8 NULL,
        col103 INT8 NULL,
        col104 INT8 NULL,
        col105 INT8 NULL,
        col106 INT8 NULL,
        col107 INT8 NULL,
        col108 INT8 NULL,
        col109 INT8 NULL,
        col110 INT8 NULL,
        col111 INT8 NULL,
        col112 INT8 NOT NULL,
        col113 INT8 NOT NULL,
        col114 INT8 NULL,
        col115 INT8 NULL,
        col116 INT8 NULL,
        col117 INT8 NULL,
        col118 INT8 NULL,
        col119 INT8 NOT NULL,
        col120 INT8 NOT NULL,
        col121 INT8 NULL,
        col122 INT8 NULL,
        col123 INT8 NULL,
        col124 INT8 NULL,
        col125 INT8 NULL,
        col126 INT8 NULL,
        col127 INT8 NULL,
        col128 INT8 NULL,
        col129 INT8 NULL,
        col130 INT8 NULL,
        col131 INT8 NULL,
        col132 INT8 NULL,
        col133 INT8 NULL,
        col134 INT8 NULL,
        col135 INT8 NULL,
        col136 INT8 NOT NULL,
        col137 INT8 NOT NULL,
        col138 INT8 NULL,
        col139 INT8 NULL,
        col140 INT8 NULL,
        col141 INT8 NULL,
        col142 INT8 NULL,
        col143 INT8 NULL,
        col144 INT8 NULL,
        col145 INT8 NULL,
        col146 INT8 NULL,
        col147 INT8 NULL,
        col148 INT8 NULL,
        col149 INT8 NULL,
        col150 INT8 NULL,
        CONSTRAINT "primary" PRIMARY KEY (col2 ASC, col1 ASC),
        INDEX index1 (col2 ASC, col20 ASC, col40 ASC) PARTITION BY LIST (col2) (
            PARTITION p1 VALUES IN ((1)),
            PARTITION p2 VALUES IN ((2)),
            PARTITION p3 VALUES IN ((3)),
            PARTITION p4 VALUES IN ((4)),
            PARTITION p5 VALUES IN ((5)),
            PARTITION p6 VALUES IN ((6)),
            PARTITION p7 VALUES IN ((7)),
            PARTITION p8 VALUES IN ((8)),
            PARTITION p9 VALUES IN ((9)),
            PARTITION p10 VALUES IN ((10)),
            PARTITION p11 VALUES IN ((11)),
            PARTITION p12 VALUES IN ((12)),
            PARTITION p13 VALUES IN ((13)),
            PARTITION p14 VALUES IN ((14)),
            PARTITION p15 VALUES IN ((15)),
            PARTITION p16 VALUES IN ((16)),
            PARTITION p17 VALUES IN ((17)),
            PARTITION p18 VALUES IN ((18)),
            PARTITION p19 VALUES IN ((19)),
            PARTITION p20 VALUES IN ((20)),
            PARTITION p21 VALUES IN ((21)),
            PARTITION p22 VALUES IN ((22)),
            PARTITION p23 VALUES IN ((23)),
            PARTITION p24 VALUES IN ((24))
        ),
        INDEX index2 (col2 ASC, col18 ASC, col6 ASC) PARTITION BY LIST (col2) (
            PARTITION p1 VALUES IN ((1)),
            PARTITION p2 VALUES IN ((2)),
            PARTITION p3 VALUES IN ((3)),
            PARTITION p4 VALUES IN ((4)),
            PARTITION p5 VALUES IN ((5)),
            PARTITION p6 VALUES IN ((6)),
            PARTITION p7 VALUES IN ((7)),
            PARTITION p8 VALUES IN ((8)),
            PARTITION p9 VALUES IN ((9)),
            PARTITION p10 VALUES IN ((10)),
            PARTITION p11 VALUES IN ((11)),
            PARTITION p12 VALUES IN ((12)),
            PARTITION p13 VALUES IN ((13)),
            PARTITION p14 VALUES IN ((14)),
            PARTITION p15 VALUES IN ((15)),
            PARTITION p16 VALUES IN ((16)),
            PARTITION p17 VALUES IN ((17)),
            PARTITION p18 VALUES IN ((18)),
            PARTITION p19 VALUES IN ((19)),
            PARTITION p20 VALUES IN ((20)),
            PARTITION p21 VALUES IN ((21)),
            PARTITION p22 VALUES IN ((22)),
            PARTITION p23 VALUES IN ((23)),
            PARTITION p24 VALUES IN ((24))
        ),
        INDEX index3 (col2 ASC, col87 ASC) PARTITION BY LIST (col2) (
            PARTITION p1 VALUES IN ((1)),
            PARTITION p2 VALUES IN ((2)),
            PARTITION p3 VALUES IN ((3)),
            PARTITION p4 VALUES IN ((4)),
            PARTITION p5 VALUES IN ((5)),
            PARTITION p6 VALUES IN ((6)),
            PARTITION p7 VALUES IN ((7)),
            PARTITION p8 VALUES IN ((8)),
            PARTITION p9 VALUES IN ((9)),
            PARTITION p10 VALUES IN ((10)),
            PARTITION p11 VALUES IN ((11)),
            PARTITION p12 VALUES IN ((12)),
            PARTITION p13 VALUES IN ((13)),
            PARTITION p14 VALUES IN ((14)),
            PARTITION p15 VALUES IN ((15)),
            PARTITION p16 VALUES IN ((16)),
            PARTITION p17 VALUES IN ((17)),
            PARTITION p18 VALUES IN ((18)),
            PARTITION p19 VALUES IN ((19)),
            PARTITION p20 VALUES IN ((20)),
            PARTITION p21 VALUES IN ((21)),
            PARTITION p22 VALUES IN ((22)),
            PARTITION p23 VALUES IN ((23)),
            PARTITION p24 VALUES IN ((24))
        ),
        INDEX index4 (col2 ASC, col8 ASC) PARTITION BY LIST (col2) (
            PARTITION p1 VALUES IN ((1)),
            PARTITION p2 VALUES IN ((2)),
            PARTITION p3 VALUES IN ((3)),
            PARTITION p4 VALUES IN ((4)),
            PARTITION p5 VALUES IN ((5)),
            PARTITION p6 VALUES IN ((6)),
            PARTITION p7 VALUES IN ((7)),
            PARTITION p8 VALUES IN ((8)),
            PARTITION p9 VALUES IN ((9)),
            PARTITION p10 VALUES IN ((10)),
            PARTITION p11 VALUES IN ((11)),
            PARTITION p12 VALUES IN ((12)),
            PARTITION p13 VALUES IN ((13)),
            PARTITION p14 VALUES IN ((14)),
            PARTITION p15 VALUES IN ((15)),
            PARTITION p16 VALUES IN ((16)),
            PARTITION p17 VALUES IN ((17)),
            PARTITION p18 VALUES IN ((18)),
            PARTITION p19 VALUES IN ((19)),
            PARTITION p20 VALUES IN ((20)),
            PARTITION p21 VALUES IN ((21)),
            PARTITION p22 VALUES IN ((22)),
            PARTITION p23 VALUES IN ((23)),
            PARTITION p24 VALUES IN ((24))
        ),
        INDEX index5 (col2 ASC, col4 ASC, col7 ASC) PARTITION BY LIST (col2) (
            PARTITION p1 VALUES IN ((1)),
            PARTITION p2 VALUES IN ((2)),
            PARTITION p3 VALUES IN ((3)),
            PARTITION p4 VALUES IN ((4)),
            PARTITION p5 VALUES IN ((5)),
            PARTITION p6 VALUES IN ((6)),
            PARTITION p7 VALUES IN ((7)),
            PARTITION p8 VALUES IN ((8)),
            PARTITION p9 VALUES IN ((9)),
            PARTITION p10 VALUES IN ((10)),
            PARTITION p11 VALUES IN ((11)),
            PARTITION p12 VALUES IN ((12)),
            PARTITION p13 VALUES IN ((13)),
            PARTITION p14 VALUES IN ((14)),
            PARTITION p15 VALUES IN ((15)),
            PARTITION p16 VALUES IN ((16)),
            PARTITION p17 VALUES IN ((17)),
            PARTITION p18 VALUES IN ((18)),
            PARTITION p19 VALUES IN ((19)),
            PARTITION p20 VALUES IN ((20)),
            PARTITION p21 VALUES IN ((21)),
            PARTITION p22 VALUES IN ((22)),
            PARTITION p23 VALUES IN ((23)),
            PARTITION p24 VALUES IN ((24))
        ),
        INDEX index6 (col2 ASC, col20 ASC, col40 ASC, col30 ASC, col8 DESC) STORING (col18) PARTITION BY LIST (col2) (
            PARTITION p1 VALUES IN ((1)),
            PARTITION p2 VALUES IN ((2)),
            PARTITION p3 VALUES IN ((3)),
            PARTITION p4 VALUES IN ((4)),
            PARTITION p5 VALUES IN ((5)),
            PARTITION p6 VALUES IN ((6)),
            PARTITION p7 VALUES IN ((7)),
            PARTITION p8 VALUES IN ((8)),
            PARTITION p9 VALUES IN ((9)),
            PARTITION p10 VALUES IN ((10)),
            PARTITION p11 VALUES IN ((11)),
            PARTITION p12 VALUES IN ((12)),
            PARTITION p13 VALUES IN ((13)),
            PARTITION p14 VALUES IN ((14)),
            PARTITION p15 VALUES IN ((15)),
            PARTITION p16 VALUES IN ((16)),
            PARTITION p17 VALUES IN ((17)),
            PARTITION p18 VALUES IN ((18)),
            PARTITION p19 VALUES IN ((19)),
            PARTITION p20 VALUES IN ((20)),
            PARTITION p21 VALUES IN ((21)),
            PARTITION p22 VALUES IN ((22)),
            PARTITION p23 VALUES IN ((23)),
            PARTITION p24 VALUES IN ((24))
        ),
        INDEX index7 (col2 ASC, col30 ASC, col8 ASC, col1 ASC) PARTITION BY LIST (col2) (
            PARTITION p1 VALUES IN ((1)),
            PARTITION p2 VALUES IN ((2)),
            PARTITION p3 VALUES IN ((3)),
            PARTITION p4 VALUES IN ((4)),
            PARTITION p5 VALUES IN ((5)),
            PARTITION p6 VALUES IN ((6)),
            PARTITION p7 VALUES IN ((7)),
            PARTITION p8 VALUES IN ((8)),
            PARTITION p9 VALUES IN ((9)),
            PARTITION p10 VALUES IN ((10)),
            PARTITION p11 VALUES IN ((11)),
            PARTITION p12 VALUES IN ((12)),
            PARTITION p13 VALUES IN ((13)),
            PARTITION p14 VALUES IN ((14)),
            PARTITION p15 VALUES IN ((15)),
            PARTITION p16 VALUES IN ((16)),
            PARTITION p17 VALUES IN ((17)),
            PARTITION p18 VALUES IN ((18)),
            PARTITION p19 VALUES IN ((19)),
            PARTITION p20 VALUES IN ((20)),
            PARTITION p21 VALUES IN ((21)),
            PARTITION p22 VALUES IN ((22)),
            PARTITION p23 VALUES IN ((23)),
            PARTITION p24 VALUES IN ((24))
        ),
        INDEX index8 (col2 ASC, col56 ASC, col1 ASC) PARTITION BY LIST (col2) (
            PARTITION p1 VALUES IN ((1)),
            PARTITION p2 VALUES IN ((2)),
            PARTITION p3 VALUES IN ((3)),
            PARTITION p4 VALUES IN ((4)),
            PARTITION p5 VALUES IN ((5)),
            PARTITION p6 VALUES IN ((6)),
            PARTITION p7 VALUES IN ((7)),
            PARTITION p8 VALUES IN ((8)),
            PARTITION p9 VALUES IN ((9)),
            PARTITION p10 VALUES IN ((10)),
            PARTITION p11 VALUES IN ((11)),
            PARTITION p12 VALUES IN ((12)),
            PARTITION p13 VALUES IN ((13)),
            PARTITION p14 VALUES IN ((14)),
            PARTITION p15 VALUES IN ((15)),
            PARTITION p16 VALUES IN ((16)),
            PARTITION p17 VALUES IN ((17)),
            PARTITION p18 VALUES IN ((18)),
            PARTITION p19 VALUES IN ((19)),
            PARTITION p20 VALUES IN ((20)),
            PARTITION p21 VALUES IN ((21)),
            PARTITION p22 VALUES IN ((22)),
            PARTITION p23 VALUES IN ((23)),
            PARTITION p24 VALUES IN ((24))
        ),
        INDEX index9 (col2 ASC, col8 ASC) PARTITION BY LIST (col2) (
            PARTITION p1 VALUES IN ((1)),
            PARTITION p2 VALUES IN ((2)),
            PARTITION p3 VALUES IN ((3)),
            PARTITION p4 VALUES IN ((4)),
            PARTITION p5 VALUES IN ((5)),
            PARTITION p6 VALUES IN ((6)),
            PARTITION p7 VALUES IN ((7)),
            PARTITION p8 VALUES IN ((8)),
            PARTITION p9 VALUES IN ((9)),
            PARTITION p10 VALUES IN ((10)),
            PARTITION p11 VALUES IN ((11)),
            PARTITION p12 VALUES IN ((12)),
            PARTITION p13 VALUES IN ((13)),
            PARTITION p14 VALUES IN ((14)),
            PARTITION p15 VALUES IN ((15)),
            PARTITION p16 VALUES IN ((16)),
            PARTITION p17 VALUES IN ((17)),
            PARTITION p18 VALUES IN ((18)),
            PARTITION p19 VALUES IN ((19)),
            PARTITION p20 VALUES IN ((20)),
            PARTITION p21 VALUES IN ((21)),
            PARTITION p22 VALUES IN ((22)),
            PARTITION p23 VALUES IN ((23)),
            PARTITION p24 VALUES IN ((24))
        ),
        INDEX index10 (col2 ASC, col20 ASC, col40 ASC, col1 ASC, col8 ASC) PARTITION BY LIST (col2) (
            PARTITION p1 VALUES IN ((1)),
            PARTITION p2 VALUES IN ((2)),
            PARTITION p3 VALUES IN ((3)),
            PARTITION p4 VALUES IN ((4)),
            PARTITION p5 VALUES IN ((5)),
            PARTITION p6 VALUES IN ((6)),
            PARTITION p7 VALUES IN ((7)),
            PARTITION p8 VALUES IN ((8)),
            PARTITION p9 VALUES IN ((9)),
            PARTITION p10 VALUES IN ((10)),
            PARTITION p11 VALUES IN ((11)),
            PARTITION p12 VALUES IN ((12)),
            PARTITION p13 VALUES IN ((13)),
            PARTITION p14 VALUES IN ((14)),
            PARTITION p15 VALUES IN ((15)),
            PARTITION p16 VALUES IN ((16)),
            PARTITION p17 VALUES IN ((17)),
            PARTITION p18 VALUES IN ((18)),
            PARTITION p19 VALUES IN ((19)),
            PARTITION p20 VALUES IN ((20)),
            PARTITION p21 VALUES IN ((21)),
            PARTITION p22 VALUES IN ((22)),
            PARTITION p23 VALUES IN ((23)),
            PARTITION p24 VALUES IN ((24))
        ),
        CONSTRAINT check1 CHECK (col2 IN (1:::INT8, 2:::INT8, 3:::INT8, 4:::INT8, 5:::INT8, 6:::INT8, 7:::INT8, 8:::INT8, 9:::INT8, 10:::INT8, 11:::INT8, 12:::INT8, 13:::INT8, 14:::INT8, 15:::INT8, 16:::INT8, 17:::INT8, 18:::INT8, 19:::INT8, 20:::INT8, 21:::INT8, 22:::INT8, 23:::INT8, 24:::INT8))
    ) PARTITION BY LIST (col2) (
        PARTITION p1 VALUES IN ((1)),
        PARTITION p2 VALUES IN ((2)),
        PARTITION p3 VALUES IN ((3)),
        PARTITION p4 VALUES IN ((4)),
        PARTITION p5 VALUES IN ((5)),
        PARTITION p6 VALUES IN ((6)),
        PARTITION p7 VALUES IN ((7)),
        PARTITION p8 VALUES IN ((8)),
        PARTITION p9 VALUES IN ((9)),
        PARTITION p10 VALUES IN ((10)),
        PARTITION p11 VALUES IN ((11)),
        PARTITION p12 VALUES IN ((12)),
        PARTITION p13 VALUES IN ((13)),
        PARTITION p14 VALUES IN ((14)),
        PARTITION p15 VALUES IN ((15)),
        PARTITION p16 VALUES IN ((16)),
        PARTITION p17 VALUES IN ((17)),
        PARTITION p18 VALUES IN ((18)),
        PARTITION p19 VALUES IN ((19)),
        PARTITION p20 VALUES IN ((20)),
        PARTITION p21 VALUES IN ((21)),
        PARTITION p22 VALUES IN ((22)),
        PARTITION p23 VALUES IN ((23)),
        PARTITION p24 VALUES IN ((24))
    )
  `,
	`
    CREATE TABLE tab2 (
        col1 INT8 NOT NULL,
        col2 INT8 NOT NULL,
        col3 INT8 NOT NULL,
        col4 INT8 NOT NULL,
        col5 INT8 NOT NULL,
        col6 INT8 NULL,
        col7 INT8 NULL,
        col8 INT8 NOT NULL,
        col9 INT8 NULL,
        col10 INT8 NULL,
        col11 INT8 NOT NULL,
        col12 INT8 NOT NULL,
        col13 INT8 NULL,
        col14 INT8 NOT NULL,
        col15 INT8 NOT NULL,
        col16 INT8 NULL,
        col17 INT8 NULL,
        col18 INT8 NULL,
        col19 INT8 NULL,
        col20 INT8 NULL,
        col21 INT8 NULL,
        col22 INT8 NULL,
        col23 INT8 NULL,
        col24 INT8 NULL,
        col25 INT8 NULL,
        col26 INT8 NULL,
        col27 INT8 NULL,
        col28 INT8 NULL,
        col29 INT8 NOT NULL,
        col30 INT8 NULL,
        col31 INT8 NOT NULL,
        col32 INT8 NULL,
        col33 INT8 NOT NULL,
        col34 INT8 NULL,
        col35 INT8 NULL,
        col36 INT8 NULL,
        col37 INT8 NOT NULL,
        col38 INT8 NULL,
        col39 INT8 NULL,
        col40 INT8 NULL,
        col41 INT8 NULL,
        col42 INT8 NULL,
        col43 INT8 NULL,
        col44 INT8 NULL,
        col45 INT8 NULL,
        col46 INT8 NULL,
        col47 INT8 NULL,
        col48 INT8 NULL,
        col49 INT8 NULL,
        col50 INT8 NULL,
        col51 INT8 NULL,
        col52 INT8 NULL,
        col53 INT8 NULL,
        col54 INT8 NULL,
        col55 INT8 NULL,
        col56 INT8 NULL,
        col57 INT8 NULL,
        col58 INT8 NULL,
        col59 INT8 NULL,
        col60 INT8 NULL,
        col61 INT8 NULL,
        col62 INT8 NULL,
        col63 INT8 NULL,
        col64 INT8 NULL,
        col65 INT8 NULL,
        col66 INT8 NULL,
        col67 INT8 NULL,
        col68 INT8 NULL,
        col69 INT8 NULL,
        col70 INT8 NULL,
        col71 INT8 NULL,
        col72 INT8 NULL,
        col73 INT8 NULL,
        col74 INT8 NULL,
        col75 INT8 NULL,
        col76 INT8 NULL,
        col77 INT8 NULL,
        col78 INT8 NOT NULL,
        col79 INT8 NULL,
        col80 INT8 NULL,
        col81 INT8 NULL,
        col82 INT8 NULL,
        col83 INT8 NULL,
        col84 INT8 NULL,
        col85 INT8 NULL,
        col86 INT8 NULL,
        col87 INT8 NULL,
        col88 INT8 NULL,
        col89 INT8 NOT NULL,
        col90 INT8 NOT NULL,
        col91 INT8 NOT NULL,
        col92 INT8 NOT NULL,
        col93 INT8 NOT NULL,
        col94 INT8 NOT NULL,
        col95 INT8 NOT NULL,
        col96 INT8 NOT NULL,
        col97 INT8 NOT NULL,
        col98 INT8 NOT NULL,
        col99 INT8 NOT NULL,
        col100 INT8 NOT NULL,
        CONSTRAINT "primary" PRIMARY KEY (col2 ASC, col1 ASC, col1 ASC),
        CONSTRAINT fk1 FOREIGN KEY (col2, col1) REFERENCES tab1(col2, col1),
        UNIQUE INDEX index1 (col2 ASC, col1 ASC) PARTITION BY LIST (col2) (
            PARTITION p1 VALUES IN ((1)),
            PARTITION p2 VALUES IN ((2)),
            PARTITION p3 VALUES IN ((3)),
            PARTITION p4 VALUES IN ((4)),
            PARTITION p5 VALUES IN ((5)),
            PARTITION p6 VALUES IN ((6)),
            PARTITION p7 VALUES IN ((7)),
            PARTITION p8 VALUES IN ((8)),
            PARTITION p9 VALUES IN ((9)),
            PARTITION p10 VALUES IN ((10)),
            PARTITION p11 VALUES IN ((11)),
            PARTITION p12 VALUES IN ((12)),
            PARTITION p13 VALUES IN ((13)),
            PARTITION p14 VALUES IN ((14)),
            PARTITION p15 VALUES IN ((15)),
            PARTITION p16 VALUES IN ((16)),
            PARTITION p17 VALUES IN ((17)),
            PARTITION p18 VALUES IN ((18)),
            PARTITION p19 VALUES IN ((19)),
            PARTITION p20 VALUES IN ((20)),
            PARTITION p21 VALUES IN ((21)),
            PARTITION p22 VALUES IN ((22)),
            PARTITION p23 VALUES IN ((23)),
            PARTITION p24 VALUES IN ((24))
        ),
        INDEX index2 (col2 ASC, col1 ASC, col38 ASC, col33 ASC) PARTITION BY LIST (col2) (
            PARTITION p1 VALUES IN ((1)),
            PARTITION p2 VALUES IN ((2)),
            PARTITION p3 VALUES IN ((3)),
            PARTITION p4 VALUES IN ((4)),
            PARTITION p5 VALUES IN ((5)),
            PARTITION p6 VALUES IN ((6)),
            PARTITION p7 VALUES IN ((7)),
            PARTITION p8 VALUES IN ((8)),
            PARTITION p9 VALUES IN ((9)),
            PARTITION p10 VALUES IN ((10)),
            PARTITION p11 VALUES IN ((11)),
            PARTITION p12 VALUES IN ((12)),
            PARTITION p13 VALUES IN ((13)),
            PARTITION p14 VALUES IN ((14)),
            PARTITION p15 VALUES IN ((15)),
            PARTITION p16 VALUES IN ((16)),
            PARTITION p17 VALUES IN ((17)),
            PARTITION p18 VALUES IN ((18)),
            PARTITION p19 VALUES IN ((19)),
            PARTITION p20 VALUES IN ((20)),
            PARTITION p21 VALUES IN ((21)),
            PARTITION p22 VALUES IN ((22)),
            PARTITION p23 VALUES IN ((23)),
            PARTITION p24 VALUES IN ((24))
        ),
        INDEX index3 (col2 ASC, col11 ASC) PARTITION BY LIST (col2) (
            PARTITION p1 VALUES IN ((1)),
            PARTITION p2 VALUES IN ((2)),
            PARTITION p3 VALUES IN ((3)),
            PARTITION p4 VALUES IN ((4)),
            PARTITION p5 VALUES IN ((5)),
            PARTITION p6 VALUES IN ((6)),
            PARTITION p7 VALUES IN ((7)),
            PARTITION p8 VALUES IN ((8)),
            PARTITION p9 VALUES IN ((9)),
            PARTITION p10 VALUES IN ((10)),
            PARTITION p11 VALUES IN ((11)),
            PARTITION p12 VALUES IN ((12)),
            PARTITION p13 VALUES IN ((13)),
            PARTITION p14 VALUES IN ((14)),
            PARTITION p15 VALUES IN ((15)),
            PARTITION p16 VALUES IN ((16)),
            PARTITION p17 VALUES IN ((17)),
            PARTITION p18 VALUES IN ((18)),
            PARTITION p19 VALUES IN ((19)),
            PARTITION p20 VALUES IN ((20)),
            PARTITION p21 VALUES IN ((21)),
            PARTITION p22 VALUES IN ((22)),
            PARTITION p23 VALUES IN ((23)),
            PARTITION p24 VALUES IN ((24))
        ),
        INDEX index4 (col2 ASC, col17 ASC) PARTITION BY LIST (col2) (
            PARTITION p1 VALUES IN ((1)),
            PARTITION p2 VALUES IN ((2)),
            PARTITION p3 VALUES IN ((3)),
            PARTITION p4 VALUES IN ((4)),
            PARTITION p5 VALUES IN ((5)),
            PARTITION p6 VALUES IN ((6)),
            PARTITION p7 VALUES IN ((7)),
            PARTITION p8 VALUES IN ((8)),
            PARTITION p9 VALUES IN ((9)),
            PARTITION p10 VALUES IN ((10)),
            PARTITION p11 VALUES IN ((11)),
            PARTITION p12 VALUES IN ((12)),
            PARTITION p13 VALUES IN ((13)),
            PARTITION p14 VALUES IN ((14)),
            PARTITION p15 VALUES IN ((15)),
            PARTITION p16 VALUES IN ((16)),
            PARTITION p17 VALUES IN ((17)),
            PARTITION p18 VALUES IN ((18)),
            PARTITION p19 VALUES IN ((19)),
            PARTITION p20 VALUES IN ((20)),
            PARTITION p21 VALUES IN ((21)),
            PARTITION p22 VALUES IN ((22)),
            PARTITION p23 VALUES IN ((23)),
            PARTITION p24 VALUES IN ((24))
        ),
        INDEX index5 (col2 ASC, col18 ASC) PARTITION BY LIST (col2) (
            PARTITION p1 VALUES IN ((1)),
            PARTITION p2 VALUES IN ((2)),
            PARTITION p3 VALUES IN ((3)),
            PARTITION p4 VALUES IN ((4)),
            PARTITION p5 VALUES IN ((5)),
            PARTITION p6 VALUES IN ((6)),
            PARTITION p7 VALUES IN ((7)),
            PARTITION p8 VALUES IN ((8)),
            PARTITION p9 VALUES IN ((9)),
            PARTITION p10 VALUES IN ((10)),
            PARTITION p11 VALUES IN ((11)),
            PARTITION p12 VALUES IN ((12)),
            PARTITION p13 VALUES IN ((13)),
            PARTITION p14 VALUES IN ((14)),
            PARTITION p15 VALUES IN ((15)),
            PARTITION p16 VALUES IN ((16)),
            PARTITION p17 VALUES IN ((17)),
            PARTITION p18 VALUES IN ((18)),
            PARTITION p19 VALUES IN ((19)),
            PARTITION p20 VALUES IN ((20)),
            PARTITION p21 VALUES IN ((21)),
            PARTITION p22 VALUES IN ((22)),
            PARTITION p23 VALUES IN ((23)),
            PARTITION p24 VALUES IN ((24))
        ),
        INDEX index6 (col2 ASC, col1 ASC) STORING (col38, col9, col10, col42, col40) PARTITION BY LIST (col2) (
            PARTITION p1 VALUES IN ((1)),
            PARTITION p2 VALUES IN ((2)),
            PARTITION p3 VALUES IN ((3)),
            PARTITION p4 VALUES IN ((4)),
            PARTITION p5 VALUES IN ((5)),
            PARTITION p6 VALUES IN ((6)),
            PARTITION p7 VALUES IN ((7)),
            PARTITION p8 VALUES IN ((8)),
            PARTITION p9 VALUES IN ((9)),
            PARTITION p10 VALUES IN ((10)),
            PARTITION p11 VALUES IN ((11)),
            PARTITION p12 VALUES IN ((12)),
            PARTITION p13 VALUES IN ((13)),
            PARTITION p14 VALUES IN ((14)),
            PARTITION p15 VALUES IN ((15)),
            PARTITION p16 VALUES IN ((16)),
            PARTITION p17 VALUES IN ((17)),
            PARTITION p18 VALUES IN ((18)),
            PARTITION p19 VALUES IN ((19)),
            PARTITION p20 VALUES IN ((20)),
            PARTITION p21 VALUES IN ((21)),
            PARTITION p22 VALUES IN ((22)),
            PARTITION p23 VALUES IN ((23)),
            PARTITION p24 VALUES IN ((24))
        ),
        INDEX index7 (col2 ASC, col11 ASC, col17 ASC, col38 ASC, col8 ASC) STORING (col3, col35) PARTITION BY LIST (col2) (
            PARTITION p1 VALUES IN ((1)),
            PARTITION p2 VALUES IN ((2)),
            PARTITION p3 VALUES IN ((3)),
            PARTITION p4 VALUES IN ((4)),
            PARTITION p5 VALUES IN ((5)),
            PARTITION p6 VALUES IN ((6)),
            PARTITION p7 VALUES IN ((7)),
            PARTITION p8 VALUES IN ((8)),
            PARTITION p9 VALUES IN ((9)),
            PARTITION p10 VALUES IN ((10)),
            PARTITION p11 VALUES IN ((11)),
            PARTITION p12 VALUES IN ((12)),
            PARTITION p13 VALUES IN ((13)),
            PARTITION p14 VALUES IN ((14)),
            PARTITION p15 VALUES IN ((15)),
            PARTITION p16 VALUES IN ((16)),
            PARTITION p17 VALUES IN ((17)),
            PARTITION p18 VALUES IN ((18)),
            PARTITION p19 VALUES IN ((19)),
            PARTITION p20 VALUES IN ((20)),
            PARTITION p21 VALUES IN ((21)),
            PARTITION p22 VALUES IN ((22)),
            PARTITION p23 VALUES IN ((23)),
            PARTITION p24 VALUES IN ((24))
        ),
        INDEX index8 (col2 ASC, col51 ASC, col17 ASC, col33 ASC) PARTITION BY LIST (col2) (
            PARTITION p1 VALUES IN ((1)),
            PARTITION p2 VALUES IN ((2)),
            PARTITION p3 VALUES IN ((3)),
            PARTITION p4 VALUES IN ((4)),
            PARTITION p5 VALUES IN ((5)),
            PARTITION p6 VALUES IN ((6)),
            PARTITION p7 VALUES IN ((7)),
            PARTITION p8 VALUES IN ((8)),
            PARTITION p9 VALUES IN ((9)),
            PARTITION p10 VALUES IN ((10)),
            PARTITION p11 VALUES IN ((11)),
            PARTITION p12 VALUES IN ((12)),
            PARTITION p13 VALUES IN ((13)),
            PARTITION p14 VALUES IN ((14)),
            PARTITION p15 VALUES IN ((15)),
            PARTITION p16 VALUES IN ((16)),
            PARTITION p17 VALUES IN ((17)),
            PARTITION p18 VALUES IN ((18)),
            PARTITION p19 VALUES IN ((19)),
            PARTITION p20 VALUES IN ((20)),
            PARTITION p21 VALUES IN ((21)),
            PARTITION p22 VALUES IN ((22)),
            PARTITION p23 VALUES IN ((23)),
            PARTITION p24 VALUES IN ((24))
        ),
        INDEX index9 (col2 ASC, col1 ASC, col33 ASC) PARTITION BY LIST (col2) (
            PARTITION p1 VALUES IN ((1)),
            PARTITION p2 VALUES IN ((2)),
            PARTITION p3 VALUES IN ((3)),
            PARTITION p4 VALUES IN ((4)),
            PARTITION p5 VALUES IN ((5)),
            PARTITION p6 VALUES IN ((6)),
            PARTITION p7 VALUES IN ((7)),
            PARTITION p8 VALUES IN ((8)),
            PARTITION p9 VALUES IN ((9)),
            PARTITION p10 VALUES IN ((10)),
            PARTITION p11 VALUES IN ((11)),
            PARTITION p12 VALUES IN ((12)),
            PARTITION p13 VALUES IN ((13)),
            PARTITION p14 VALUES IN ((14)),
            PARTITION p15 VALUES IN ((15)),
            PARTITION p16 VALUES IN ((16)),
            PARTITION p17 VALUES IN ((17)),
            PARTITION p18 VALUES IN ((18)),
            PARTITION p19 VALUES IN ((19)),
            PARTITION p20 VALUES IN ((20)),
            PARTITION p21 VALUES IN ((21)),
            PARTITION p22 VALUES IN ((22)),
            PARTITION p23 VALUES IN ((23)),
            PARTITION p24 VALUES IN ((24))
        ),
        INDEX index10 (col2 ASC, col18 ASC, col1 ASC, col33 ASC) PARTITION BY LIST (col2) (
            PARTITION p1 VALUES IN ((1)),
            PARTITION p2 VALUES IN ((2)),
            PARTITION p3 VALUES IN ((3)),
            PARTITION p4 VALUES IN ((4)),
            PARTITION p5 VALUES IN ((5)),
            PARTITION p6 VALUES IN ((6)),
            PARTITION p7 VALUES IN ((7)),
            PARTITION p8 VALUES IN ((8)),
            PARTITION p9 VALUES IN ((9)),
            PARTITION p10 VALUES IN ((10)),
            PARTITION p11 VALUES IN ((11)),
            PARTITION p12 VALUES IN ((12)),
            PARTITION p13 VALUES IN ((13)),
            PARTITION p14 VALUES IN ((14)),
            PARTITION p15 VALUES IN ((15)),
            PARTITION p16 VALUES IN ((16)),
            PARTITION p17 VALUES IN ((17)),
            PARTITION p18 VALUES IN ((18)),
            PARTITION p19 VALUES IN ((19)),
            PARTITION p20 VALUES IN ((20)),
            PARTITION p21 VALUES IN ((21)),
            PARTITION p22 VALUES IN ((22)),
            PARTITION p23 VALUES IN ((23)),
            PARTITION p24 VALUES IN ((24))
        ),
        CONSTRAINT check1 CHECK (col2 IN (1:::INT8, 2:::INT8, 3:::INT8, 4:::INT8, 5:::INT8, 6:::INT8, 7:::INT8, 8:::INT8, 9:::INT8, 10:::INT8, 11:::INT8, 12:::INT8, 13:::INT8, 14:::INT8, 15:::INT8, 16:::INT8, 17:::INT8, 18:::INT8, 19:::INT8, 20:::INT8, 21:::INT8, 22:::INT8, 23:::INT8, 24:::INT8))
    ) PARTITION BY LIST (col2) (
        PARTITION p1 VALUES IN ((1)),
        PARTITION p2 VALUES IN ((2)),
        PARTITION p3 VALUES IN ((3)),
        PARTITION p4 VALUES IN ((4)),
        PARTITION p5 VALUES IN ((5)),
        PARTITION p6 VALUES IN ((6)),
        PARTITION p7 VALUES IN ((7)),
        PARTITION p8 VALUES IN ((8)),
        PARTITION p9 VALUES IN ((9)),
        PARTITION p10 VALUES IN ((10)),
        PARTITION p11 VALUES IN ((11)),
        PARTITION p12 VALUES IN ((12)),
        PARTITION p13 VALUES IN ((13)),
        PARTITION p14 VALUES IN ((14)),
        PARTITION p15 VALUES IN ((15)),
        PARTITION p16 VALUES IN ((16)),
        PARTITION p17 VALUES IN ((17)),
        PARTITION p18 VALUES IN ((18)),
        PARTITION p19 VALUES IN ((19)),
        PARTITION p20 VALUES IN ((20)),
        PARTITION p21 VALUES IN ((21)),
        PARTITION p22 VALUES IN ((22)),
        PARTITION p23 VALUES IN ((23)),
        PARTITION p24 VALUES IN ((24))
    )
  `,
	`
		CREATE TABLE table64793_1 (
			col1_0 CHAR NOT NULL, col1_1 BOOL NOT NULL, col1_2 REGPROC NOT NULL,
			col1_3 REGPROCEDURE NOT NULL, col1_4 TIMETZ NOT NULL, col1_5 FLOAT8 NULL,
			col1_6 INT2 NOT NULL, col1_7 BOOL, col1_8 BOX2D NOT NULL,
			col1_9 REGNAMESPACE NOT NULL,
			PRIMARY KEY (
				col1_8 DESC, col1_9 DESC, col1_4 DESC, col1_1, col1_2 ASC, col1_3 DESC,
				col1_0 DESC, col1_6
			),
			col1_10 INT2 NOT NULL AS (col1_6 + 22798:::INT8) VIRTUAL,
			FAMILY (col1_4), FAMILY (col1_0, col1_5), FAMILY (col1_1),
			FAMILY (col1_8, col1_3, col1_9, col1_7), FAMILY (col1_2), FAMILY (col1_6))
	`,
	`
		CREATE TYPE greeting64793 AS ENUM ('hello', 'howdy', 'hi', 'good day', 'morning');
	`,
	`
		CREATE TABLE seed64793 (
			_int2 INT2,
			_int4 INT4,
			_int8 INT8,
			_float4 FLOAT4,
			_float8 FLOAT8,
			_date DATE,
			_timestamp TIMESTAMP,
			_timestamptz TIMESTAMPTZ,
			_interval INTERVAL,
			_bool BOOL,
			_decimal DECIMAL,
			_string STRING,
			_bytes BYTES,
			_uuid UUID,
			_inet INET,
			_jsonb JSONB,
			_enum greeting64793
		);
	`,
	`
		CREATE INDEX on seed64793 (_int8, _float8, _date);
	`,
	`
		CREATE INVERTED INDEX on seed64793 (_jsonb);
	`,
	`
		CREATE TABLE table64793_2 (
			col1_0 "char" NOT NULL, col1_1 OID NOT NULL, col1_2 BIT(38) NOT NULL,
			col1_3 BIT(18) NOT NULL, col1_4 BYTES NOT NULL, col1_5 INT8 NOT NULL,
			col1_6 INTERVAL NOT NULL, col1_7 BIT(33) NOT NULL, col1_8 INTERVAL NULL,
			col1_9 GEOMETRY NOT NULL, col1_10 BOOL NOT NULL, col1_11 INT2,
			PRIMARY KEY (
				col1_4 ASC, col1_7 DESC, col1_1 ASC, col1_2 ASC, col1_10 ASC, col1_5,
				col1_0 ASC, col1_3, col1_6
			),
			UNIQUE (
				col1_8 DESC, col1_11, col1_3 DESC, col1_7, col1_6 DESC, col1_4 ASC,
				col1_1 DESC
			)
		);
	`,
	`
		CREATE TABLE table64793_3 (
			col2_0 NAME NOT NULL, col2_1 TIMETZ NOT NULL,
			PRIMARY KEY (col2_0 ASC, col2_1),
			col2_2 STRING NOT NULL AS (lower(col2_0)) VIRTUAL,
			UNIQUE (col2_0 DESC, col2_2 DESC, col2_1)
			WHERE (table64793_3.col2_2 > e'\U00002603':::STRING)
			OR (table64793_3.col2_0 != '"':::STRING),
			UNIQUE (col2_1 ASC, col2_2, col2_0),
			UNIQUE (col2_0 DESC,col2_1, col2_2),
			INDEX (col2_1 DESC),
			UNIQUE (col2_2 DESC, col2_0 ASC)
			WHERE table64793_3.col2_2 = '"':::STRING
		);
	`,
	`
		CREATE TABLE table64793_4 (
			col2_0 NAME NOT NULL, col2_1 TIMETZ NOT NULL, col3_2 REGPROC NOT NULL,
			col3_3 "char", col3_4 BOX2D, col3_5 INT8 NULL, col3_6 TIMESTAMP NOT NULL,
			col3_7 FLOAT8, col3_8 INT4 NULL, col3_9 INET NULL, col3_10 UUID NOT NULL,
			col3_11 UUID NULL, col3_12 INT2 NOT NULL, col3_13 BIT(34),
			col3_14 REGPROCEDURE NULL, col3_15 FLOAT8 NULL,
			PRIMARY KEY (
				col2_0 ASC, col2_1, col3_11 DESC, col3_13, col3_6, col3_3 DESC,
				col3_15 ASC, col3_2 ASC, col3_4 ASC, col3_9 DESC, col3_12 ASC,
				col3_8 ASC, col3_5, col3_14 ASC
			),
			UNIQUE (col3_2, col3_8 ASC)
			WHERE ((((table64793_4.col3_5 < 0:::INT8)
			AND (table64793_4.col3_3 != '':::STRING))
			AND (table64793_4.col2_1 < '00:00:00+15:59:00':::TIMETZ))
			AND (table64793_4.col3_12 > 0:::INT8))
			AND (table64793_4.col3_15 <= 1.7976931348623157e+308:::FLOAT8),
			UNIQUE (col3_10 DESC, col3_3 ASC, col2_1 DESC, col3_9 ASC)
		);
	`,
}

var slowQueries = [...]benchQuery{
	// 1. The first long-running query taken from #64793.
	// 2. The most recent long-running query from #64793 (as of July 2022).
	// 3. A long-running query inspired by support issue #1710.
	{
		name: "slow-query-1",
		query: `
			WITH with_186941 (col_1103773, col_1103774) AS (
				SELECT
					*
				FROM
					(
						VALUES
							('clvl', 3 :: INT2),
							(
								'n',
								(
									SELECT
										tab_455284.col1_6 AS col_1103772
									FROM
										table64793_1@[0] AS tab_455284
									ORDER BY
										tab_455284.col1_2 DESC,
										tab_455284.col1_1 DESC
									LIMIT
										1 ::: INT8
								)
							),
							(NULL, 6736 ::: INT8)
					) AS tab_455285 (col_1103773, col_1103774)
			),
			with_186942 (col_1103775) AS (
				SELECT
					*
				FROM
					(
						VALUES
							('yk'),
							(NULL)
					) AS tab_455286 (col_1103775)
			)
			SELECT
				0 ::: OID AS col_1103776,
				(
					(-32244820164.24410487)::: DECIMAL :: DECIMAL + tab_455291.col1_10 :: INT8
				):: DECIMAL AS col_1103777,
				tab_455287._bool AS col_1103778
			FROM
				with_186942 AS cte_ref_54113,
				seed64793@[0] AS tab_455287
				JOIN seed64793 AS tab_455288
				JOIN seed64793 AS tab_455289 ON (tab_455288._int8) = (tab_455289._int8)
				AND (tab_455288._date) = (tab_455289._date)
				AND (tab_455288._float8) = (tab_455289._float8)
				JOIN table64793_1@[0] AS tab_455290
				JOIN table64793_1@primary AS tab_455291
				JOIN table64793_1@[0] AS tab_455295
				JOIN seed64793 AS tab_455296
				JOIN seed64793 AS tab_455297 ON (tab_455296._int8) = (tab_455297._int8)
				AND (tab_455296._date) = (tab_455297._date) ON (tab_455295.col1_5) = (tab_455297._float8)
				AND (tab_455295.col1_5) = (tab_455296._float8)
				AND (tab_455295.col1_5) = (tab_455297._float8)
				AND (tab_455295.col1_5) = (tab_455297._float8) ON (tab_455291.col1_2) = (tab_455295.tableoid)
				AND (tab_455291.col1_7) = (tab_455295.col1_1) ON (tab_455290.col1_2) = (tab_455291.col1_9)
				AND (tab_455290.col1_7) = (tab_455291.col1_7) ON (tab_455289._float8) = (tab_455296._float8) ON (tab_455287._float4) = (tab_455290.col1_5)
				AND (tab_455287.tableoid) = (tab_455295.col1_9)
				AND (tab_455287._bool) = (tab_455295.col1_7);
		`,
		args: []interface{}{},
	},
	{
		name: "slow-query-2",
		query: `
			WITH with_121707 (col_692430) AS (
				SELECT
					*
				FROM
					(
						VALUES
							(
								(-0.19099748134613037)::: FLOAT8
							),
							(0.9743397235870361 ::: FLOAT8),
							(
								(-1.6944892406463623)::: FLOAT8
							)
					) AS tab_297691 (col_692430)
			)
			SELECT
				'-35 years -11 mons -571 days -08:18:57.001029' ::: INTERVAL AS col_692441
			FROM
				table64793_2@table64793_2_col1_8_col1_11_col1_3_col1_7_col1_6_col1_4_col1_1_key AS tab_297692
				JOIN table64793_3@table64793_3_col2_0_col2_1_col2_2_key AS tab_297693
				JOIN table64793_2@[0] AS tab_297694
				JOIN seed64793@seed64793__int8__float8__date_idx AS tab_297695
				RIGHT JOIN table64793_3@[0] AS tab_297696
				JOIN table64793_4@table64793_4_col3_10_col3_3_col2_1_col3_9_key AS tab_297697 ON (tab_297696.col2_0) = (tab_297697.col3_3) CROSS
				JOIN table64793_4@[0] AS tab_297698
				JOIN table64793_3 AS tab_297699 ON (tab_297698.col2_0) = (tab_297699.col2_0) ON TRUE
				JOIN table64793_4@[0] AS tab_297700 ON (tab_297697.col3_12) = (tab_297700.col3_8) ON (tab_297694.tableoid) = (tab_297695.tableoid)
				AND (tab_297694.col1_5) = (tab_297698.col3_8)
				AND (tab_297694.tableoid) = (tab_297698.col3_2)
				AND (tab_297694.col1_5) = (tab_297697.col3_12) ON (tab_297693.col2_2) = (tab_297700.col3_3)
				AND (tab_297693.col2_1) = (tab_297698.col2_1)
				AND (tab_297693.tableoid) = (tab_297699.tableoid)
				AND (tab_297693.col2_1) = (tab_297697.col2_1)
				AND (tab_297693.tableoid) = (tab_297694.col1_1)
				AND (tab_297693.col2_2) = (tab_297695._string)
				AND (tab_297693.col2_2) = (tab_297696.col2_0)
				AND (tab_297693.col2_2) = (tab_297698.col3_3) ON (tab_297692.col1_11) = (tab_297694.col1_11)
			ORDER BY
				tab_297695._enum DESC
			LIMIT
				57 ::: INT8;
		`,
		args: []interface{}{},
	},
	{
		name: "slow-query-3",
		query: `
      SELECT
        *
      FROM
        tab1
        INNER JOIN tab2 ON
            tab1.col1 = tab2.col1 AND tab1.col2 = tab2.col2
      WHERE
        tab1.col1
        IN (
            10:::INT8,
            20:::INT8,
            30:::INT8,
            40:::INT8,
            50:::INT8,
            60:::INT8,
            70:::INT8,
            80:::INT8,
            90:::INT8,
            100:::INT8,
            110:::INT8,
            120:::INT8,
            130:::INT8,
            140:::INT8,
            150:::INT8,
            160:::INT8,
            170:::INT8,
            180:::INT8,
            190:::INT8,
            200:::INT8,
            210:::INT8,
            220:::INT8,
            230:::INT8,
            240:::INT8,
            250:::INT8,
            260:::INT8,
            270:::INT8,
            280:::INT8,
            290:::INT8,
            300:::INT8,
            310:::INT8,
            320:::INT8,
            330:::INT8,
            340:::INT8,
            350:::INT8,
            360:::INT8,
            370:::INT8,
            380:::INT8,
            390:::INT8,
            400:::INT8,
            410:::INT8,
            420:::INT8,
            430:::INT8,
            440:::INT8,
            450:::INT8,
            460:::INT8,
            470:::INT8,
            480:::INT8,
            490:::INT8,
            500:::INT8
          )
        AND tab1.col8 > 0
        AND tab1.col8 < 10
        AND tab1.col20 = 4500
        AND tab1.col40 = 10
      ORDER BY
        tab1.col8 DESC;
    `,
		args: []interface{}{},
	},
}

func BenchmarkSlowQueries(b *testing.B) {
	for _, query := range slowQueries {
		h := newHarness(b, query, slowSchemas)
		h.evalCtx.SessionData().ReorderJoinsLimit = 8
		b.Run(query.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				h.runSimple(b, query, Explore)
			}
		})
	}
}
