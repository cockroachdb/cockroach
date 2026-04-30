// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"encoding/json"
	"fmt"
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
				{Column: fetchpb.IndexFetchSpec_Column{ColumnID: 1}},
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

	eligibleQuery := `SELECT l.a, r.v FROM test.l AS l INNER LOOKUP JOIN test.r AS r ON l.a = r.a AND l.b = r.b`
	eligibleSpecs := getJoinReaderSpecsFromExplainDistSQLJSON(t, r, eligibleQuery)
	require.NotEmpty(t, eligibleSpecs)
	require.True(t, hasAdaptiveEnabledJoinReaderSpec(eligibleSpecs))

	ineligibleQuery := `SELECT l.a, r.v FROM test.l AS l INNER LOOKUP JOIN test.r AS r ON l.a = r.a AND l.b = r.b ORDER BY l.a`
	ineligibleSpecs := getJoinReaderSpecsFromExplainDistSQLJSON(t, r, ineligibleQuery)
	require.NotEmpty(t, ineligibleSpecs)
	require.False(t, hasAdaptiveEnabledJoinReaderSpec(ineligibleSpecs))
}

func getJoinReaderSpecsFromExplainDistSQLJSON(
	t *testing.T, r *sqlutils.SQLRunner, query string,
) []map[string]any {
	t.Helper()

	var explainJSON string
	r.QueryRow(t, fmt.Sprintf("EXPLAIN (DISTSQL, JSON) %s", query)).Scan(&explainJSON)

	var root any
	require.NoError(t, json.Unmarshal([]byte(explainJSON), &root))

	var specs []map[string]any
	var walk func(node any)
	walk = func(node any) {
		switch v := node.(type) {
		case map[string]any:
			if jr, ok := v["joinReader"]; ok {
				if spec, ok := jr.(map[string]any); ok {
					specs = append(specs, spec)
				}
			}
			for _, child := range v {
				walk(child)
			}
		case []any:
			for _, child := range v {
				walk(child)
			}
		}
	}
	walk(root)

	return specs
}

func hasAdaptiveEnabledJoinReaderSpec(specs []map[string]any) bool {
	for i := range specs {
		enabled, ok := specs[i]["adaptiveEnabled"].(bool)
		if ok && enabled {
			return true
		}
	}
	return false
}
