// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

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

func TestSpanConstrainer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	s, db, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT, b int, c STRING, CONSTRAINT "pk" PRIMARY KEY (a, b), INDEX (c))`)
	fooDesc := desctestutils.TestingGetTableDescriptor(
		kvDB, keys.SystemSQLCodec, "defaultdb", "public", "foo")

	ctx := context.Background()
	execCfg := s.ExecutorConfig().(ExecutorConfig)
	p, cleanup := NewInternalPlanner("test", kv.NewTxn(ctx, kvDB, s.NodeID()),
		username.RootUserName(), &MemoryMetrics{}, &execCfg, sessiondatapb.SessionData{},
	)
	defer cleanup()

	primarySpan := fooDesc.PrimaryIndexSpan(keys.SystemSQLCodec)
	pkStart := primarySpan.Key
	pkEnd := primarySpan.EndKey
	fooID := fooDesc.GetID()

	sc := p.(SpanConstrainer)
	evalCtx := eval.MakeTestingEvalContext(s.ClusterSettings())
	semaCtx := tree.MakeSemaContext()
	for _, tc := range []struct {
		filter      string
		req         SpanConstraintRequirement
		expectErr   string
		remaining   string
		alias       *tree.TableName
		expectSpans []roachpb.Span
	}{
		{
			filter:      "5 > 1",
			expectSpans: []roachpb.Span{primarySpan},
		},
		{
			filter:    "0 != 0",
			expectErr: "is a contradiction",
		},
		{
			filter:    "a IS NULL",
			expectErr: "is a contradiction",
		},
		{
			filter:    "a > 3 AND a < 3",
			expectErr: "is a contradiction",
		},
		{
			filter:    "a >=3 or a < 3",
			expectErr: "is a tautology",
		},
		{
			filter:      "a >=3 or a < 3",
			req:         BestEffortConstrain,
			expectSpans: []roachpb.Span{primarySpan},
		},
		{
			filter:    "5",
			expectErr: "expected boolean expression",
		},
		{
			filter:    "no_such_column = 'something'",
			expectErr: `column "no_such_column" does not exist`,
		},
		{
			filter:      "true",
			expectSpans: []roachpb.Span{primarySpan},
		},
		{
			filter:    "false",
			expectErr: "is a contradiction",
		},
		{
			filter:      "a > 100",
			expectSpans: []roachpb.Span{{Key: mkPkKey(t, fooID, 101), EndKey: pkEnd}},
		},
		{
			filter:      "a < 100",
			expectSpans: []roachpb.Span{{Key: pkStart, EndKey: mkPkKey(t, fooID, 100)}},
		},
		{
			filter:      "a > 10 AND a > 5",
			expectSpans: []roachpb.Span{{Key: mkPkKey(t, fooID, 11), EndKey: pkEnd}},
		},
		{
			filter:      "a > 10 OR a > 5",
			expectSpans: []roachpb.Span{{Key: mkPkKey(t, fooID, 6), EndKey: pkEnd}},
		},
		{
			filter:      "a > 100 AND a <= 101",
			expectSpans: []roachpb.Span{{Key: mkPkKey(t, fooID, 101), EndKey: mkPkKey(t, fooID, 102)}},
		},
		{
			filter:      "a > 100 and a < 200",
			expectSpans: []roachpb.Span{{Key: mkPkKey(t, fooID, 101), EndKey: mkPkKey(t, fooID, 200)}},
		},
		{
			filter: "a > 100 or a <= 99",
			expectSpans: []roachpb.Span{
				{Key: pkStart, EndKey: mkPkKey(t, fooID, 100)},
				{Key: mkPkKey(t, fooID, 101), EndKey: pkEnd},
			},
		},
		{
			filter:    "b < 42",
			expectErr: "cannot be fully constrained",
		},
		{
			filter:    "c = 'ten'",
			expectErr: "cannot be fully constrained",
		},
		{
			filter:    "a < 42 OR (a > 100 AND b > 11)",
			expectErr: "cannot be fully constrained",
		},
		{
			filter:    "a > 100 AND b > 11",
			remaining: "b > 11",
			req:       BestEffortConstrain,
			expectSpans: []roachpb.Span{
				{Key: mkPkKey(t, fooID, 101, 12), EndKey: pkEnd},
			},
		},
		{
			// Same as above, but with table alias -- we expect remaining expression to
			// preserve the alias.
			filter:    "buz.a > 100 AND b > 11",
			remaining: "buz.b > 11",
			req:       BestEffortConstrain,
			alias:     tree.NewUnqualifiedTableName("buz"),
			expectSpans: []roachpb.Span{
				{Key: mkPkKey(t, fooID, 101, 12), EndKey: pkEnd},
			},
		},
		{
			// Same as above, but w/ silly tautology, which should be removed.
			filter:    "(a > 3 OR a <= 3) AND a > 100 AND b > 11",
			alias:     tree.NewTableNameWithSchema("defaultdb", "public", "foo"),
			remaining: "defaultdb.public.foo.b > 11", // When alias specified, it is preserved.
			req:       BestEffortConstrain,
			expectSpans: []roachpb.Span{
				{Key: mkPkKey(t, fooID, 101, 12), EndKey: pkEnd},
			},
		},
		{
			filter:    "a < 42 OR (a > 100 AND b > 11)",
			remaining: "(a < 42) OR ((a > 100) AND (b > 11))",
			req:       BestEffortConstrain,
			expectSpans: []roachpb.Span{
				{Key: pkStart, EndKey: mkPkKey(t, fooID, 42)},
				{Key: mkPkKey(t, fooID, 101, 12), EndKey: pkEnd},
			},
		},
		{
			filter:    "a > 2 AND b > 5 AND a > 2",
			expectErr: "cannot be fully constrained",
		},
		{
			// Same as above, but now with tuples.
			filter: "a < 42 OR ((a, b) > (100, 11))",
			expectSpans: []roachpb.Span{
				{Key: pkStart, EndKey: mkPkKey(t, fooID, 42)},
				// Remember: tuples use lexicographical ordering so the start key is
				// /Table/104/1/100/12 (i.e. a="100" and b="12" (because 100/12 lexicographically follows 100).
				{Key: mkPkKey(t, fooID, 100, 12), EndKey: pkEnd},
			},
		},
		{
			filter:      "(a, b) > (2, 5)",
			expectSpans: []roachpb.Span{{Key: mkPkKey(t, fooID, 2, 6), EndKey: pkEnd}},
		},
		{
			// Test that aliased table names work.
			filter:      "(buz.a, buz.b) > (2, 5)",
			expectSpans: []roachpb.Span{{Key: mkPkKey(t, fooID, 2, 6), EndKey: pkEnd}},
			alias:       tree.NewUnqualifiedTableName("buz"),
		},
		{
			// This test also uses qualified names for some fields.
			filter: "foo.a IN (5, 10, 20) AND b < 25",
			alias:  tree.NewUnqualifiedTableName("foo"),
			expectSpans: []roachpb.Span{
				{Key: mkPkKey(t, fooID, 5), EndKey: mkPkKey(t, fooID, 5, 25)},
				{Key: mkPkKey(t, fooID, 10), EndKey: mkPkKey(t, fooID, 10, 25)},
				{Key: mkPkKey(t, fooID, 20), EndKey: mkPkKey(t, fooID, 20, 25)},
			},
		},
	} {
		t.Run(tc.filter, func(t *testing.T) {
			filterExpr, err := parser.ParseExpr(tc.filter)
			require.NoError(t, err)

			alias := tc.alias
			if alias == nil {
				var tn tree.TableName
				alias = &tn
			}

			spans, remaining, err := sc.ConstrainPrimaryIndexSpanByExpr(
				ctx, tc.req, alias, fooDesc, &evalCtx, &semaCtx, filterExpr)
			if tc.expectErr != "" {
				require.Regexp(t, tc.expectErr, err)
				require.Nil(t, spans)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectSpans, spans)

			if tc.remaining == "" {
				require.Equal(t, tree.DBoolTrue, remaining, tree.AsStringWithFlags(remaining, tree.FmtExport))
			} else {
				require.Equal(t, tc.remaining, tree.AsStringWithFlags(remaining, tree.FmtExport))
			}
		})
	}
}
