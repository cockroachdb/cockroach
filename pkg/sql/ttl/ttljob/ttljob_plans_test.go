// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ttljob

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catsessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestQueryPlans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	runner := sqlutils.MakeSQLRunner(sqlDB)
	runner.Exec(t, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false;")
	db := s.InternalDB().(*sql.InternalDB)
	cutoff, err := tree.MakeDTimestamp(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Second)
	require.NoError(t, err)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	sd := sql.NewInternalSessionData(ctx, s.ClusterSettings(), "test" /* opName */)
	sds := sessiondata.NewStack(sd)
	dsdp := catsessiondata.NewDescriptorSessionDataStackProvider(sds)
	descsCol := execCfg.CollectionFactory.NewCollection(ctx, descs.WithDescriptorSessionDataProvider(dsdp))

	var noOverrides string
	getExplainPlan := func(query string, overrides string) string {
		rows := runner.Query(t, fmt.Sprintf(`SELECT crdb_internal.execute_internally('EXPLAIN %s', '%s');`, query, overrides))
		var explainPlan string
		for rows.Next() {
			var explainRow string
			require.NoError(t, rows.Scan(&explainRow))
			explainPlan += "\n" + explainRow
		}
		require.NoError(t, rows.Err())
		return explainPlan
	}

	for _, tc := range []struct {
		name                           string
		setup                          []string
		targetTableName                string
		bounds                         QueryBounds
		expectedSelectPlan             string
		expectedDeletePlan             string
		expectedDeletePlanWithOverride [2]string
	}{
		{
			name: "cascade (regression test for #118129)",
			setup: []string{
				`CREATE TABLE p (id INT PRIMARY KEY, b BOOL) WITH (ttl = 'on', ttl_expire_after = '00:10:00':::INTERVAL);`,
				`CREATE TABLE c (id INT PRIMARY KEY, p_id INT NOT NULL REFERENCES p(id) ON DELETE CASCADE, INDEX (p_id));`,
			},
			targetTableName: "p",
			bounds: QueryBounds{
				Start: tree.Datums{tree.NewDInt(tree.DInt(0))},
				End:   tree.Datums{tree.NewDInt(tree.DInt(100))},
			},
			expectedSelectPlan: `
distribution: local
vectorized: true

• filter
│ filter: crdb_internal_expiration <= '2024-01-01 00:00:00+00'
│
└── • scan
      missing stats
      table: p@p_pkey
      spans: [/0 - /100]`,
			expectedDeletePlan: `
distribution: local
vectorized: true

• root
│
├── • delete
│   │ from: p
│   │
│   └── • buffer
│       │ label: buffer 1
│       │
│       └── • filter
│           │ filter: crdb_internal_expiration <= '2024-01-01 00:00:00+00'
│           │
│           └── • scan
│                 missing stats
│                 table: p@p_pkey
│                 spans: [/0 - /0] [/100 - /100]
│
└── • fk-cascade
    │ fk: c_p_id_fkey
    │
    └── • delete
        │ from: c
        │
        └── • lookup join
            │ table: c@c_p_id_idx
            │ equality: (id) = (p_id)
            │
            └── • distinct
                │ estimated row count: 10
                │ distinct on: id
                │
                └── • scan buffer
                      estimated row count: 100
                      label: buffer 1000000`,
			expectedDeletePlanWithOverride: [2]string{
				`ReorderJoinsLimit=0`,
				`
distribution: local
vectorized: true

• root
│
├── • delete
│   │ from: p
│   │
│   └── • buffer
│       │ label: buffer 1
│       │
│       └── • filter
│           │ filter: crdb_internal_expiration <= '2024-01-01 00:00:00+00'
│           │
│           └── • scan
│                 missing stats
│                 table: p@p_pkey
│                 spans: [/0 - /0] [/100 - /100]
│
└── • fk-cascade
    │ fk: c_p_id_fkey
    │
    └── • delete
        │ from: c
        │
        └── • merge join
            │ equality: (p_id) = (id)
            │ right cols are key
            │
            ├── • scan
            │     missing stats
            │     table: c@c_p_id_idx
            │     spans: FULL SCAN
            │
            └── • sort
                │ estimated row count: 10
                │ order: +id
                │
                └── • distinct
                    │ estimated row count: 10
                    │ distinct on: id
                    │
                    └── • scan buffer
                          estimated row count: 100
                          label: buffer 1000000`,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, q := range tc.setup {
				runner.Exec(t, q)
			}
			// TODO(yuzefovich): this function might not work if multiple bounds
			// are specified.
			replacePlaceholders := func(query string) string {
				query = strings.ReplaceAll(query, "'", "''")
				query = strings.ReplaceAll(query, "$1", "'"+cutoff.String()+"'")
				// Confusingly, all End bounds go before all Start bounds when
				// assigning placeholder indices in the SelectQueryBuilder.
				for i, d := range tc.bounds.End {
					query = strings.ReplaceAll(query, fmt.Sprintf("$%d", i+2), d.String())
				}
				for i, d := range tc.bounds.Start {
					query = strings.ReplaceAll(query, fmt.Sprintf("$%d", i+2+len(tc.bounds.End)), d.String())
				}
				return query
			}

			var tableID int64
			row := runner.QueryRow(t, fmt.Sprintf("SELECT '%s'::REGCLASS::OID;", tc.targetTableName))
			row.Scan(&tableID)
			relationName, _, pkColNames, _, pkColDirs, _, _, err := getTableInfo(
				ctx, db, descsCol, descpb.ID(tableID),
			)
			require.NoError(t, err)

			if tc.expectedSelectPlan != "" {
				selectBuilder := MakeSelectQueryBuilder(
					SelectQueryParams{
						RelationName:    relationName,
						PKColNames:      pkColNames,
						PKColDirs:       pkColDirs,
						Bounds:          tc.bounds,
						SelectBatchSize: ttlbase.DefaultSelectBatchSizeValue,
						TTLExpr:         catpb.DefaultTTLExpirationExpr,
					},
					cutoff.UTC(),
				)
				selectQuery := replacePlaceholders(selectBuilder.buildQuery())
				selectPlan := getExplainPlan(selectQuery, noOverrides)
				require.Equal(t, tc.expectedSelectPlan, selectPlan)
			}
			if tc.expectedDeletePlan != "" {
				deleteBuilder := MakeDeleteQueryBuilder(
					DeleteQueryParams{
						RelationName:    relationName,
						PKColNames:      pkColNames,
						DeleteBatchSize: 100,
						TTLExpr:         catpb.DefaultTTLExpirationExpr,
					},
					cutoff.UTC(),
				)
				deleteQuery := replacePlaceholders(deleteBuilder.buildQuery(2 /* numRows */))
				deletePlan := getExplainPlan(deleteQuery, noOverrides)
				require.Equal(t, tc.expectedDeletePlan, deletePlan)
				if tc.expectedDeletePlanWithOverride[0] != "" {
					deletePlan = getExplainPlan(deleteQuery, tc.expectedDeletePlanWithOverride[0])
					require.Equal(t, tc.expectedDeletePlanWithOverride[1], deletePlan)
				}
			}
		})
	}
}
