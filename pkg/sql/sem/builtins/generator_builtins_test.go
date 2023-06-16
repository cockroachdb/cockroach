// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestConcurrentProcessorsReadEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	params := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLEvalContext: &eval.TestingKnobs{
				CallbackGenerators: map[string]*eval.CallbackValueGenerator{
					"my_callback": eval.NewCallbackValueGenerator(
						func(ctx context.Context, prev int, _ *kv.Txn) (int, error) {
							if prev < 10 {
								return prev + 1, nil
							}
							return -1, nil
						}),
				},
			},
		},
	}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	rows, err := db.Query(` select * from crdb_internal.testing_callback('my_callback')`)
	require.NoError(t, err)
	exp := 1
	for rows.Next() {
		var got int
		require.NoError(t, rows.Scan(&got))
		require.Equal(t, exp, got)
		exp++
	}
}

func TestGetSSTableMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	ts, hostDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer ts.Stopper().Stop(ctx)

	nodeId := 1
	storeId := ts.GetFirstStoreID()

	r := sqlutils.MakeSQLRunner(hostDB)
	r.Exec(t, `CREATE TABLE t(k INT PRIMARY KEY, v INT)`)
	r.Exec(t, `INSERT INTO t SELECT i, i*10 FROM generate_series(1, 10000) AS g(i)`)

	r.Exec(t, fmt.Sprintf(`
	 SELECT crdb_internal.compact_engine_span(
		 %d, %d,
		 (SELECT raw_start_key FROM [SHOW RANGES FROM TABLE t WITH KEYS] LIMIT 1),
		 (SELECT raw_end_key FROM [SHOW RANGES FROM TABLE t WITH KEYS] LIMIT 1))`,
		nodeId, storeId))

	rows := r.Query(t, fmt.Sprintf(`
	 SELECT * FROM crdb_internal.sstable_metrics(
		 %d, %d,
		 (SELECT raw_start_key FROM [SHOW RANGES FROM TABLE t WITH KEYS] LIMIT 1),
		 (SELECT raw_end_key FROM [SHOW RANGES FROM TABLE t WITH KEYS] LIMIT 1))`,
		nodeId, storeId))

	count := 0
	for rows.Next() {
		var nodeID int
		var storeID int
		var level int
		var fileNum int
		var metrics []byte

		require.NoError(t, rows.Scan(&nodeID, &storeID, &level, &fileNum, &metrics))
		require.NoError(t, json.Unmarshal(metrics, &enginepb.SSTableMetricsInfo{}))
		count++
	}
	require.Equal(t, 1, count)
}
