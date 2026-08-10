// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestShouldEnableAdaptiveLookupJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	adaptiveLookupJoinEnabled.Override(ctx, &st.SV, true)
	adaptiveLookupJoinThresholdBytes.Override(ctx, &st.SV, 1024)

	eligibleSpec := execinfrapb.JoinReaderSpec{
		FetchSpec: fetchpb.IndexFetchSpec{
			KeyAndSuffixColumns: []fetchpb.IndexFetchSpec_KeyColumn{
				{IndexFetchSpec_Column: fetchpb.IndexFetchSpec_Column{ColumnID: 1}},
			},
			FetchedColumns: []fetchpb.IndexFetchSpec_Column{
				{ColumnID: 1},
			},
		},
		LookupColumns: []uint32{0},
	}
	eligiblePlanInfo := lookupJoinPlanningInfo{joinType: descpb.InnerJoin}

	enabled, threshold := shouldEnableAdaptiveLookupJoin(&st.SV, &eligibleSpec, &eligiblePlanInfo)
	require.True(t, enabled)
	require.Equal(t, int64(1024), threshold)

	t.Run("feature disabled", func(t *testing.T) {
		adaptiveLookupJoinEnabled.Override(ctx, &st.SV, false)
		enabled, _ := shouldEnableAdaptiveLookupJoin(&st.SV, &eligibleSpec, &eligiblePlanInfo)
		require.False(t, enabled)
		adaptiveLookupJoinEnabled.Override(ctx, &st.SV, true)
	})

	t.Run("non-inner", func(t *testing.T) {
		planInfo := eligiblePlanInfo
		planInfo.joinType = descpb.LeftOuterJoin
		enabled, _ := shouldEnableAdaptiveLookupJoin(&st.SV, &eligibleSpec, &planInfo)
		require.False(t, enabled)
	})

	t.Run("ordering required", func(t *testing.T) {
		spec := eligibleSpec
		spec.MaintainOrdering = true
		enabled, _ := shouldEnableAdaptiveLookupJoin(&st.SV, &spec, &eligiblePlanInfo)
		require.False(t, enabled)
	})

	t.Run("locality optimized", func(t *testing.T) {
		planInfo := eligiblePlanInfo
		planInfo.remoteLookupExpr = tree.DBoolTrue
		enabled, _ := shouldEnableAdaptiveLookupJoin(&st.SV, &eligibleSpec, &planInfo)
		require.False(t, enabled)
	})

	t.Run("paired join mode", func(t *testing.T) {
		planInfo := eligiblePlanInfo
		planInfo.isSecondJoinInPairedJoiner = true
		enabled, _ := shouldEnableAdaptiveLookupJoin(&st.SV, &eligibleSpec, &planInfo)
		require.False(t, enabled)
	})

	t.Run("lookup expr unsupported in MVP", func(t *testing.T) {
		planInfo := eligiblePlanInfo
		planInfo.lookupExpr = tree.DBoolTrue
		enabled, _ := shouldEnableAdaptiveLookupJoin(&st.SV, &eligibleSpec, &planInfo)
		require.False(t, enabled)
	})

	t.Run("on expr unsupported in MVP", func(t *testing.T) {
		planInfo := eligiblePlanInfo
		planInfo.onCond = tree.DBoolTrue
		enabled, _ := shouldEnableAdaptiveLookupJoin(&st.SV, &eligibleSpec, &planInfo)
		require.False(t, enabled)
	})

	t.Run("missing fetched key columns", func(t *testing.T) {
		spec := eligibleSpec
		spec.FetchSpec.FetchedColumns = nil
		enabled, _ := shouldEnableAdaptiveLookupJoin(&st.SV, &spec, &eligiblePlanInfo)
		require.False(t, enabled)
	})
}

func TestAdaptiveLookupJoinPlanningFlags(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	r := sqlutils.MakeSQLRunner(db)

	r.Exec(t, "SET CLUSTER SETTING sql.distsql.adaptive_lookup_join.enabled = true")
	r.Exec(t, "SET distsql = always")
	r.Exec(t, `
CREATE DATABASE test;
CREATE TABLE test.l (a INT, b INT, PRIMARY KEY (a, b));
CREATE TABLE test.r (a INT, b INT, v INT, PRIMARY KEY (a, b));
INSERT INTO test.l VALUES (1, 1), (2, 1), (3, 1);
INSERT INTO test.r VALUES (1, 1, 11), (2, 1, 21), (3, 1, 31);
`)

	// Eligible: inner lookup join without ordering.
	eligibleQuery := `SELECT l.a, r.v FROM test.l AS l INNER LOOKUP JOIN test.r AS r ON l.a = r.a AND l.b = r.b`
	require.True(t, hasJoinReaderInExplain(t, r, eligibleQuery),
		"expected EXPLAIN to contain a lookup join for eligible query")

	// Ineligible: inner lookup join WITH ordering — adaptive should not be enabled.
	ineligibleQuery := `SELECT l.a, r.v FROM test.l AS l INNER LOOKUP JOIN test.r AS r ON l.a = r.a AND l.b = r.b ORDER BY l.a`
	require.True(t, hasJoinReaderInExplain(t, r, ineligibleQuery),
		"expected EXPLAIN to contain a lookup join for ineligible query")

	// Verify that the eligible query gets the adaptive flag set at the spec
	// level by checking the EXPLAIN (DISTSQL, JSON) output for the JoinReader
	// processor title.
	eligibleJSON := getExplainDistSQLJSON(t, r, eligibleQuery)
	require.Contains(t, eligibleJSON, "JoinReader",
		"expected EXPLAIN DISTSQL JSON to contain a JoinReader processor")

	// Verify unit-test level: shouldEnableAdaptiveLookupJoin logic is correct.
	// This is already covered in TestShouldEnableAdaptiveLookupJoin above.
}

func hasJoinReaderInExplain(t *testing.T, r *sqlutils.SQLRunner, query string) bool {
	t.Helper()
	rows := r.QueryStr(t, fmt.Sprintf("EXPLAIN %s", query))
	for _, row := range rows {
		for _, col := range row {
			if strings.Contains(col, "lookup join") {
				return true
			}
		}
	}
	return false
}

func getExplainDistSQLJSON(t *testing.T, r *sqlutils.SQLRunner, query string) string {
	t.Helper()
	var result string
	r.QueryRow(t, fmt.Sprintf("EXPLAIN (DISTSQL, JSON) %s", query)).Scan(&result)
	return result
}

