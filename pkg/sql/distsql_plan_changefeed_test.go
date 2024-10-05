// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
)

// Tests in this file test CDC specific logic when planning/executing
// expressions. CDC expressions are a restricted form of a select statement. The
// tests here do not test full CDC semantics, and instead just use expressions
// that satisfy CDC requirement. The tests here also do not test any CDC
// specific functions; this is done elsewhere.

func TestChangefeedLogicalPlan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	defer tree.TestingEnableFamilyIndexHint()()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `
CREATE TABLE foo (
  a INT,
  b int,
  c STRING,
  double_c STRING AS (concat(c, c)) VIRTUAL,  -- Virtual columns ignored for now, but adding to make sure.
  extra STRING NOT NULL,
  CONSTRAINT "pk" PRIMARY KEY (a, b),
  UNIQUE (c),
  UNIQUE (extra),
  FAMILY main (a,b,c),
  FAMILY extra (extra)
)`)

	sqlDB.Exec(t, `CREATE TABLE bar (a INT)`)
	fooDesc := desctestutils.TestingGetTableDescriptor(
		kvDB, keys.SystemSQLCodec, "defaultdb", "public", "foo")

	ctx := context.Background()
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	sd := NewInternalSessionData(ctx, execCfg.Settings, "test")
	sd.Database = "defaultdb"

	p, cleanup := NewInternalPlanner("test", kv.NewTxn(ctx, kvDB, s.NodeID()),
		username.NodeUserName(), &MemoryMetrics{}, &execCfg, sd,
	)
	defer cleanup()

	primarySpan := fooDesc.PrimaryIndexSpan(keys.SystemSQLCodec)
	pkStart := primarySpan.Key
	pkEnd := primarySpan.EndKey
	fooID := fooDesc.GetID()

	rc := func(n string, typ *types.T) colinfo.ResultColumn {
		return colinfo.ResultColumn{Name: n, Typ: typ}
	}
	mainColumns := colinfo.ResultColumns{
		rc("a", types.Int), rc("b", types.Int), rc("c", types.String),
	}
	// mainColumns as tuple.
	mainColumnsTuple := colinfo.ResultColumns{
		rc("foo", types.MakeLabeledTuple(
			[]*types.T{types.Int, types.Int, types.String},
			[]string{"a", "b", "c"}),
		)}

	extraColumns := colinfo.ResultColumns{
		rc("a", types.Int), rc("b", types.Int), rc("extra", types.String),
	}

	// extraColumnsAs tuple.
	extraColumnsTuple := colinfo.ResultColumns{
		rc("foo", types.MakeLabeledTuple(
			[]*types.T{types.Int, types.Int, types.String},
			[]string{"a", "b", "extra"}),
		)}

	checkPresentation := func(t *testing.T, expected, found colinfo.ResultColumns) {
		t.Helper()
		require.Equal(t, len(expected), len(found), "e=%v f=%v", expected, found)
		for i := 0; i < len(found); i++ {
			require.Equal(t, expected[i].Name, found[i].Name, "e=%v f=%v", expected[i], found[i])
			require.True(t, expected[i].Typ.Equal(found[i].Typ), "e=%v f=%v", expected[i], found[i])
		}
	}

	for _, tc := range []struct {
		stmt        string
		opts        []CDCOption
		expectErr   string
		expectSpans roachpb.Spans
		present     colinfo.ResultColumns
	}{
		{
			stmt:        "SELECT * FROM foo WHERE 5 > 1",
			expectSpans: roachpb.Spans{primarySpan},
			present:     mainColumns,
		},
		{
			stmt: "SELECT * FROM foo",
			opts: []CDCOption{
				// Add extra hidden column -- should not show up in the presentation.
				WithExtraColumn(copyColumnAs(t, fooDesc, colinfo.MVCCTimestampColumnName, "mvcc")),
			},
			expectSpans: roachpb.Spans{primarySpan},
			present:     mainColumns,
		},
		{
			stmt: "SELECT *, mvcc FROM foo",
			opts: []CDCOption{
				WithExtraColumn(copyColumnAs(t, fooDesc, colinfo.MVCCTimestampColumnName, "mvcc")),
			},
			expectSpans: roachpb.Spans{primarySpan},
			present:     append(mainColumns, rc("mvcc", colinfo.MVCCTimestampColumnType)),
		},
		{
			// Star scoped to column family.
			stmt:        "SELECT * FROM foo@{FAMILY=[1]} WHERE 5 > 1",
			expectSpans: roachpb.Spans{primarySpan},
			present:     extraColumns,
		},
		{
			stmt:      "SELECT * FROM foo WHERE 0 != 0",
			expectErr: "does not match any rows",
		},
		{
			stmt:      "SELECT * FROM foo WHERE a IS NULL",
			expectErr: "does not match any rows",
		},
		{
			stmt:      "SELECT * FROM foo@{FAMILY=[1]} WHERE extra IS NULL",
			expectErr: "does not match any rows",
		},
		{
			// Cannot reference extra column when targeting main column family.
			stmt:      "SELECT * FROM foo WHERE extra IS NULL",
			expectErr: `column "extra" does not exist`,
		},
		{
			// Can access system columns.
			stmt:        "SELECT *, tableoid FROM foo",
			expectSpans: roachpb.Spans{primarySpan},
			present:     append(mainColumns, rc("tableoid", colinfo.TableOIDColumnDesc.Type)),
		},
		{
			// Can access system columns in extra family.
			stmt:        "SELECT *, crdb_internal_mvcc_timestamp AS mvcc FROM foo@{FAMILY=[1]}",
			expectSpans: roachpb.Spans{primarySpan},
			present:     append(extraColumns, rc("mvcc", colinfo.MVCCTimestampColumnDesc.Type)),
		},
		{
			stmt:      "SELECT * FROM foo, bar WHERE foo.a = bar.a",
			expectErr: "unexpected multiple primary index scan operations",
		},
		{
			stmt:      "SELECT (SELECT a FROM foo) AS a FROM foo",
			expectErr: "unexpected query structure",
		},
		{
			stmt:      "SELECT * FROM foo WHERE a > 3 AND a < 3",
			expectErr: "does not match any rows",
		},
		{
			stmt:      "SELECT * FROM foo WHERE 5 > 1 UNION SELECT * FROM foo WHERE a < 1",
			expectErr: "unexpected expression type",
		},
		{
			stmt:        "SELECT * FROM foo WHERE a >=3 or a < 3",
			expectSpans: roachpb.Spans{primarySpan},
			present:     mainColumns,
		},
		{
			stmt:      "SELECT * FROM foo WHERE 5",
			expectErr: "argument of WHERE must be type bool, not type int",
		},
		{
			stmt:      "SELECT * FROM foo WHERE no_such_column = 'something'",
			expectErr: `column "no_such_column" does not exist`,
		},
		{
			stmt:        "SELECT * FROM foo WHERE true",
			expectSpans: roachpb.Spans{primarySpan},
			present:     mainColumns,
		},
		{
			stmt:      "SELECT * FROM foo WHERE false",
			expectErr: "does not match any rows",
		},
		{
			stmt:        "SELECT * FROM foo WHERE a > 100",
			expectSpans: roachpb.Spans{{Key: mkPkKey(t, fooID, 101), EndKey: pkEnd}},
			present:     mainColumns,
		},
		{
			stmt:        "SELECT * FROM foo WHERE a < 100",
			expectSpans: roachpb.Spans{{Key: pkStart, EndKey: mkPkKey(t, fooID, 100)}},
			present:     mainColumns,
		},
		{
			stmt:        "SELECT * FROM foo WHERE a > 10 AND a > 5",
			expectSpans: roachpb.Spans{{Key: mkPkKey(t, fooID, 11), EndKey: pkEnd}},
			present:     mainColumns,
		},
		{
			stmt:        "SELECT * FROM foo WHERE a > 10 OR a > 5",
			expectSpans: roachpb.Spans{{Key: mkPkKey(t, fooID, 6), EndKey: pkEnd}},
			present:     mainColumns,
		},
		{
			stmt:        "SELECT * FROM foo WHERE a > 100 AND a <= 101",
			expectSpans: roachpb.Spans{{Key: mkPkKey(t, fooID, 101), EndKey: mkPkKey(t, fooID, 102)}},
			present:     mainColumns,
		},
		{
			stmt:        "SELECT * FROM foo WHERE a > 100 and a < 200",
			expectSpans: roachpb.Spans{{Key: mkPkKey(t, fooID, 101), EndKey: mkPkKey(t, fooID, 200)}},
			present:     mainColumns,
		},
		{
			stmt: "SELECT * FROM foo WHERE a > 100 or a <= 99",
			expectSpans: roachpb.Spans{
				{Key: pkStart, EndKey: mkPkKey(t, fooID, 100)},
				{Key: mkPkKey(t, fooID, 101), EndKey: pkEnd},
			},
			present: mainColumns,
		},
		{
			stmt:        "SELECT * FROM foo WHERE a > 100 AND b > 11",
			expectSpans: roachpb.Spans{{Key: mkPkKey(t, fooID, 101, 12), EndKey: pkEnd}},
			present:     mainColumns,
		},
		{
			// Same as above, but with table alias -- we expect remaining expression to
			// preserve the alias.
			stmt:        "SELECT * FROM foo AS buz WHERE buz.a > 100 AND b > 11",
			expectSpans: roachpb.Spans{{Key: mkPkKey(t, fooID, 101, 12), EndKey: pkEnd}},
			present:     mainColumns,
		},
		{
			// Same as above, but w/ silly tautology, which should be removed.
			stmt:        "SELECT * FROM defaultdb.public.foo WHERE (a > 3 OR a <= 3) AND a > 100 AND b > 11",
			expectSpans: roachpb.Spans{{Key: mkPkKey(t, fooID, 101, 12), EndKey: pkEnd}},
			present:     mainColumns,
		},
		{
			stmt: "SELECT * FROM foo WHERE a < 42 OR (a > 100 AND b > 11)",
			expectSpans: roachpb.Spans{
				{Key: pkStart, EndKey: mkPkKey(t, fooID, 42)},
				{Key: mkPkKey(t, fooID, 101, 12), EndKey: pkEnd},
			},
			present: mainColumns,
		},
		{
			// Same as above, but now with tuples.
			stmt: "SELECT * FROM foo WHERE a < 42 OR ((a, b) > (100, 11))",
			expectSpans: roachpb.Spans{
				{Key: pkStart, EndKey: mkPkKey(t, fooID, 42)},
				// Remember: tuples use lexicographical ordering so the start key is
				// /Table/104/1/100/12 (i.e. a="100" and b="12" (because 100/12 lexicographically follows 100).
				{Key: mkPkKey(t, fooID, 100, 12), EndKey: pkEnd},
			},
			present: mainColumns,
		},
		{
			stmt:        "SELECT * FROM foo WHERE (a, b) > (2, 5)",
			expectSpans: roachpb.Spans{{Key: mkPkKey(t, fooID, 2, 6), EndKey: pkEnd}},
			present:     mainColumns,
		},
		{
			// Test that aliased table names work.
			stmt:        "SELECT * FROM foo as buz WHERE (buz.a, buz.b) > (2, 5)",
			expectSpans: roachpb.Spans{{Key: mkPkKey(t, fooID, 2, 6), EndKey: pkEnd}},
			present:     mainColumns,
		},
		{
			// This test also uses qualified names for some fields.
			stmt: "SELECT * FROM foo WHERE foo.a IN (5, 10, 20) AND b < 25",
			expectSpans: roachpb.Spans{
				{Key: mkPkKey(t, fooID, 5), EndKey: mkPkKey(t, fooID, 5, 25)},
				{Key: mkPkKey(t, fooID, 10), EndKey: mkPkKey(t, fooID, 10, 25)},
				{Key: mkPkKey(t, fooID, 20), EndKey: mkPkKey(t, fooID, 20, 25)},
			},
			present: mainColumns,
		},
		{
			// Currently, only primary index supported; so even when doing lookup
			// on a non-primary index, we expect primary index to be scanned.
			stmt:        "SELECT * FROM foo WHERE c = 'unique'",
			expectSpans: roachpb.Spans{primarySpan},
			present:     mainColumns,
		},
		{
			// Point lookup.
			stmt:        `SELECT a as apple, b as boy, pi()/2 as "halfPie" FROM foo WHERE (a = 5 AND b = 10)`,
			expectSpans: roachpb.Spans{{Key: mkPkKey(t, fooID, 5, 10, 0)}},
			present: colinfo.ResultColumns{
				rc("apple", types.Int), rc("boy", types.Int), rc("halfPie", types.Float),
			},
		},
		{
			// Scope -- restrict columns
			stmt:        `SELECT * FROM (SELECT a, c FROM foo) AS foo`,
			expectSpans: roachpb.Spans{primarySpan},
			present:     colinfo.ResultColumns{rc("a", types.Int), rc("c", types.String)},
		},
		{
			// Expression targets column C with unique index.
			// However, we currently do not support any indexes other than primary.
			// Verify that's the case.
			stmt:        `SELECT a, c FROM foo WHERE c IN ('a', 'b', 'c')`,
			expectSpans: roachpb.Spans{primarySpan},
			present:     colinfo.ResultColumns{rc("a", types.Int), rc("c", types.String)},
		},
		{
			// foo as a table-typed tuple; main column family.
			stmt:        `SELECT foo FROM foo`,
			expectSpans: roachpb.Spans{primarySpan},
			present:     mainColumnsTuple,
		},
		{
			// foo as a table-typed tuple; extra column family.
			stmt:        `SELECT foo FROM foo@{FAMILY=[1]}`,
			expectSpans: roachpb.Spans{primarySpan},
			present:     extraColumnsTuple,
		},
		{
			// foo as a table typed tuple (extra column family), but using table reference.
			stmt:        fmt.Sprintf(`SELECT foo FROM [%d AS foo]@{FAMILY=[1]}`, fooDesc.GetID()),
			expectSpans: roachpb.Spans{primarySpan},
			present:     extraColumnsTuple,
		},
		{
			// System columns
			stmt:        `SELECT crdb_internal_mvcc_timestamp AS mvcc FROM foo`,
			expectSpans: roachpb.Spans{primarySpan},
			present:     colinfo.ResultColumns{rc("mvcc", colinfo.MVCCTimestampColumnType)},
		},
	} {
		t.Run(tc.stmt, func(t *testing.T) {
			stmt, err := parser.ParseOne(tc.stmt)
			require.NoError(t, err)
			expr := stmt.AST.(*tree.Select)

			plan, err := PlanCDCExpression(ctx, p, expr, tc.opts...)
			if tc.expectErr != "" {
				require.Regexp(t, tc.expectErr, err, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectSpans, plan.Spans)
			checkPresentation(t, tc.present, plan.Presentation)
		})
	}

	t.Run("collector", func(t *testing.T) {
		for _, tc := range []struct {
			stmt       string
			opts       []CDCOption
			expectCols []string
		}{
			{
				stmt:       "SELECT * FROM foo",
				expectCols: []string{"a", "b", "c"},
			},
			{
				stmt: "SELECT * FROM foo",
				opts: []CDCOption{
					WithExtraColumn(copyColumnAs(t, fooDesc, colinfo.MVCCTimestampColumnName, "mvcc")),
				},
				expectCols: []string{"a", "b", "c"},
			},
			{
				stmt:       "SELECT * FROM foo WHERE crdb_internal_mvcc_timestamp > 0",
				expectCols: []string{"a", "b", "c", "crdb_internal_mvcc_timestamp"},
			},
			{
				stmt: "SELECT *, mvcc FROM foo WHERE crdb_internal_mvcc_timestamp > 0",
				opts: []CDCOption{
					WithExtraColumn(copyColumnAs(t, fooDesc, colinfo.MVCCTimestampColumnName, "mvcc")),
				},
				expectCols: []string{"a", "b", "c", "crdb_internal_mvcc_timestamp", "mvcc"},
			},
		} {
			stmt, err := parser.ParseOne(tc.stmt)
			require.NoError(t, err)
			expr := stmt.AST.(*tree.Select)

			plan, err := PlanCDCExpression(ctx, p, expr, tc.opts...)
			require.NoError(t, err)
			collected := make(map[uint32]string)
			plan.CollectPlanColumns(
				func(c colinfo.ResultColumn) bool {
					collected[c.PGAttributeNum] = c.Name
					return false
				})
			collectedNames := make([]string, 0, len(collected))
			for _, v := range collected {
				collectedNames = append(collectedNames, v)
			}
			sort.Strings(collectedNames)
			require.Equal(t, tc.expectCols, collectedNames)
		}
	})
}

// Ensure the physical plan does not buffer results.
func TestChangefeedStreamsResults(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b int)`)
	fooDesc := desctestutils.TestingGetTableDescriptor(
		kvDB, keys.SystemSQLCodec, "defaultdb", "public", "foo")

	ctx := context.Background()
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	sd := NewInternalSessionData(ctx, execCfg.Settings, "test")
	sd.Database = "defaultdb"
	p, cleanup := NewInternalPlanner("test", kv.NewTxn(ctx, kvDB, s.NodeID()),
		username.NodeUserName(), &MemoryMetrics{}, &execCfg, sd,
	)
	defer cleanup()
	stmt, err := parser.ParseOne("SELECT * FROM foo WHERE a < 10")
	require.NoError(t, err)
	expr := stmt.AST.(*tree.Select)
	cdcPlan, err := PlanCDCExpression(ctx, p, expr)
	require.NoError(t, err)

	cdcPlan.Plan.planNode, err = prepareCDCPlan(ctx, cdcPlan.Plan.planNode,
		nil, catalog.ColumnIDToOrdinalMap(fooDesc.PublicColumns()))
	require.NoError(t, err)

	planner := p.(*planner)
	physPlan, physPlanCleanup, err := planner.DistSQLPlanner().createPhysPlan(ctx, cdcPlan.PlanCtx, cdcPlan.Plan)
	defer physPlanCleanup()
	require.NoError(t, err)
	require.Equal(t, 1, len(physPlan.LocalProcessors))
	require.True(t, physPlan.LocalProcessors[0].MustBeStreaming())
}

func TestCdcExpressionExecution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	tt := s.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TABLE foo (
a INT PRIMARY KEY, b INT, c STRING, extra STRING,
FAMILY main(a,b,c),
-- We will target primary family in the foo table.
-- The semantics around * expansion should be adopted to
-- only reference columns in the main family.
FAMILY extra (extra)
)`)

	fooDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, tt.Codec(), "defaultdb", "foo")

	ctx := context.Background()
	execCfg := tt.ExecutorConfig().(ExecutorConfig)
	sd := NewInternalSessionData(ctx, execCfg.Settings, "test")
	sd.Database = "defaultdb"
	p, cleanup := NewInternalPlanner("test", kv.NewTxn(ctx, kvDB, s.NodeID()),
		username.NodeUserName(), &MemoryMetrics{}, &execCfg, sd,
	)
	defer cleanup()
	planner := p.(*planner)

	for _, tc := range []struct {
		name      string
		stmt      string
		hiddenCol bool
		expectRow func(row rowenc.EncDatumRow) (expected []string)
	}{
		{
			name: "star",
			stmt: "SELECT * FROM foo WHERE a % 2 = 0",
			expectRow: func(row rowenc.EncDatumRow) (expected []string) {
				if tree.MustBeDInt(row[0].Datum)%2 == 0 {
					a := tree.AsStringWithFlags(row[0].Datum, tree.FmtExport)
					b := tree.AsStringWithFlags(row[1].Datum, tree.FmtExport)
					c := tree.AsStringWithFlags(row[2].Datum, tree.FmtExport)
					expected = append(expected, a, b, c)
				}
				return expected
			},
		},
		{
			name:      "hidden column",
			stmt:      "SELECT a, hidden FROM foo",
			hiddenCol: true,
			expectRow: func(row rowenc.EncDatumRow) (expected []string) {
				a := tree.AsStringWithFlags(row[0].Datum, tree.FmtExport)
				expected = append(expected, a, fmt.Sprintf("%d", fooDesc.GetID()))
				return expected
			},
		},
		{
			name: "same columns",
			stmt: "SELECT a, c, c, a FROM foo WHERE a % 2 = 0",
			expectRow: func(row rowenc.EncDatumRow) (expected []string) {
				if tree.MustBeDInt(row[0].Datum)%2 == 0 {
					a := tree.AsStringWithFlags(row[0].Datum, tree.FmtExport)
					c := tree.AsStringWithFlags(row[2].Datum, tree.FmtExport)
					expected = append(expected, a, c, c, a)
				}
				return expected
			},
		},
		{
			name: "double c",
			stmt: "SELECT concat(c, c) AS doubleC FROM foo WHERE c IS NOT NULL",
			expectRow: func(row rowenc.EncDatumRow) (expected []string) {
				if row[2].Datum != tree.DNull {
					c := tree.AsStringWithFlags(row[2].Datum, tree.FmtExport)
					expected = append(expected, c+c)
				}
				return expected
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parser.ParseOne(tc.stmt)
			require.NoError(t, err)
			expr := stmt.AST.(*tree.Select)

			var inputCols catalog.TableColMap
			var inputTypes []*types.T
			// We target main family; so only 3 columns should be used.
			for _, id := range fooDesc.GetFamilies()[0].ColumnIDs {
				col, err := catalog.MustFindColumnByID(fooDesc, id)
				require.NoError(t, err)
				inputCols.Set(col.GetID(), len(inputTypes))
				inputTypes = append(inputTypes, col.GetType())
			}

			var opts []CDCOption
			var oidCol catalog.Column
			if tc.hiddenCol {
				oidCol = copyColumnAs(t, fooDesc, colinfo.TableOIDColumnName, "hidden")
				opts = append(opts, WithExtraColumn(oidCol))
				inputCols.Set(oidCol.GetID(), len(inputTypes))
				inputTypes = append(inputTypes, oidCol.GetType())
			}

			var input execinfra.RowChannel
			input.InitWithBufSizeAndNumSenders(inputTypes, 1024, 1)
			plan, err := PlanCDCExpression(ctx, p, expr, opts...)
			require.NoError(t, err)

			var rows [][]string
			g := ctxgroup.WithContext(context.Background())

			g.GoCtx(func(ctx context.Context) error {
				writer := NewCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
					strRow := make([]string, len(row))
					for i, d := range row {
						strRow[i] = tree.AsStringWithFlags(d, tree.FmtExport)
					}
					rows = append(rows, strRow)
					return nil
				})

				r := MakeDistSQLReceiver(
					ctx,
					writer,
					tree.Rows,
					planner.execCfg.RangeDescriptorCache,
					nil,
					nil, /* clockUpdater */
					planner.extendedEvalCtx.Tracing,
				)
				defer r.Release()

				if err := RunCDCEvaluation(ctx, plan, &input, inputCols, r); err != nil {
					return err
				}
				return writer.Err()
			})

			rng, _ := randutil.NewTestRand()

			var expectedRows [][]string
			func() {
				defer input.ProducerDone()
				for i := 0; i < 100; i++ {
					row := randEncDatumRow(rng, fooDesc, inputCols)
					if tc.hiddenCol {
						row = append(row, rowenc.EncDatum{Datum: tree.NewDOid(oid.Oid(fooDesc.GetID()))})
					}
					input.Push(row, nil)
					if expected := tc.expectRow(row); expected != nil {
						expectedRows = append(expectedRows, tc.expectRow(row))
					}
				}
			}()

			require.NoError(t, g.Wait())
			require.Equal(t, expectedRows, rows)
		})
	}
}

func randEncDatumRow(
	rng *rand.Rand, desc catalog.TableDescriptor, includeCols catalog.TableColMap,
) (row rowenc.EncDatumRow) {
	for _, col := range desc.PublicColumns() {
		if _, include := includeCols.Get(col.GetID()); include && !col.IsVirtual() {
			row = append(row, rowenc.EncDatum{Datum: randgen.RandDatum(rng, col.GetType(), col.IsNullable())})
		}
	}
	return row
}

func mkPkKey(t *testing.T, tableID descpb.ID, vals ...int) roachpb.Key {
	t.Helper()

	// Encode index id, then each value.
	key, err := keyside.Encode(
		keys.SystemSQLCodec.TablePrefix(uint32(tableID)),
		tree.NewDInt(tree.DInt(1)), encoding.Ascending)

	require.NoError(t, err)
	for _, v := range vals {
		d := tree.NewDInt(tree.DInt(v))
		key, err = keyside.Encode(key, d, encoding.Ascending)
		require.NoError(t, err)
	}

	return key
}

func copyColumnAs(t *testing.T, table catalog.TableDescriptor, from, to tree.Name) catalog.Column {
	t.Helper()
	src, err := catalog.MustFindColumnByTreeName(table, from)
	require.NoError(t, err)
	dst := src.DeepCopy()
	desc := dst.ColumnDesc()
	desc.Name = string(to)
	desc.ID = table.GetNextColumnID()
	desc.Nullable = true
	desc.SystemColumnKind = catpb.SystemColumnKind_NONE
	return dst
}
