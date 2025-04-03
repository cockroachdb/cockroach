// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
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

func TestGetSSTableMetricsMultiNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	sqlDB.Exec(t, `CREATE TABLE t(k INT PRIMARY KEY, v INT)`)
	sqlDB.Exec(t, `INSERT INTO t SELECT i, i*10 FROM generate_series(1, 100) AS g(i)`)

	sqlDB.Exec(t, `CREATE TABLE b(k STRING PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO b VALUES('abc')`)
	sqlDB.Exec(t, `INSERT INTO b VALUES('bcd')`)
	sqlDB.Exec(t, `INSERT INTO b VALUES('cle')`)

	require.NoError(t, tc.WaitForFullReplication())

	var nodeID int
	var storeID int
	var level int
	var fileNum int
	var approximateSpanBytes uint64
	var metrics []byte

	for idx, id := range tc.NodeIDs() {
		nodeIDArg := int(id)
		srv := tc.Server(idx)
		store, err := srv.GetStores().(*kvserver.Stores).GetStore(srv.GetFirstStoreID())
		require.NoError(t, err)
		storeIDArg := int(store.StoreID())

		sqlDB.Exec(t, fmt.Sprintf(`
			SELECT crdb_internal.compact_engine_span(
			%d, %d,
			(SELECT raw_start_key FROM [SHOW RANGES FROM TABLE t WITH KEYS] LIMIT 1),
			(SELECT raw_end_key FROM [SHOW RANGES FROM TABLE t WITH KEYS] LIMIT 1))`,
			nodeIDArg, storeIDArg))

		sqlDB.Exec(t, fmt.Sprintf(`
			SELECT crdb_internal.compact_engine_span(
			%d, %d,
			(SELECT raw_start_key FROM [SHOW RANGES FROM TABLE b WITH KEYS] LIMIT 1),
			(SELECT raw_end_key FROM [SHOW RANGES FROM TABLE b WITH KEYS] LIMIT 1))`,
			nodeIDArg, storeIDArg))

		rows := sqlDB.Query(t, fmt.Sprintf(`
			SELECT * FROM crdb_internal.sstable_metrics(
			%d, %d,
			(SELECT raw_start_key FROM [SHOW RANGES FROM TABLE t WITH KEYS] LIMIT 1),
			(SELECT raw_end_key FROM [SHOW RANGES FROM TABLE t WITH KEYS] LIMIT 1))`,
			nodeIDArg, storeIDArg))

		count := 0
		for rows.Next() {
			require.NoError(t, rows.Scan(&nodeID, &storeID, &level, &fileNum, &approximateSpanBytes, &metrics))
			t.Logf("n%d s%d, table: t, level: %d  fileNum: %d  approximateSpanBytes: %d  metrics: %s",
				nodeID, storeID, level, fileNum, approximateSpanBytes, string(metrics))
			require.NoError(t, json.Unmarshal(metrics, &enginepb.SSTableMetricsInfo{}))
			require.Equal(t, nodeID, nodeIDArg)
			require.Equal(t, storeID, storeIDArg)
			require.NotZero(t, fileNum)
			require.NotZero(t, approximateSpanBytes)
			count++
		}
		require.Equal(t, 1, count, "nodeID: %d", nodeIDArg)

		rows = sqlDB.Query(t, fmt.Sprintf(`
		SELECT * FROM crdb_internal.sstable_metrics(
		%d, %d,
		(SELECT raw_start_key FROM [SHOW RANGES FROM TABLE b WITH KEYS] LIMIT 1),
		(SELECT raw_end_key FROM [SHOW RANGES FROM TABLE b WITH KEYS] LIMIT 1))`,
			nodeIDArg, storeIDArg))

		count = 0
		for rows.Next() {
			require.NoError(t, rows.Scan(&nodeID, &storeID, &level, &fileNum, &approximateSpanBytes, &metrics))
			t.Logf("n%d s%d, table: b, level: %d  fileNum: %d  approximateSpanBytes: %d  metrics: %s",
				nodeID, storeID, level, fileNum, approximateSpanBytes, string(metrics))
			require.NoError(t, json.Unmarshal(metrics, &enginepb.SSTableMetricsInfo{}))
			require.Equal(t, nodeID, nodeIDArg)
			require.Equal(t, storeID, storeIDArg)
			require.NotZero(t, fileNum)
			require.NotZero(t, approximateSpanBytes)
			count++
		}
		require.Equal(t, 1, count, "nodeID: %d", nodeIDArg)
	}
}

func TestGetSSTableMetricsSingleNode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	ts, hostDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer ts.Stopper().Stop(ctx)

	nodeIDArg := 1
	storeIDArg := int(ts.GetFirstStoreID())

	r := sqlutils.MakeSQLRunner(hostDB)
	r.Exec(t, `CREATE TABLE t(k INT PRIMARY KEY, v INT)`)
	r.Exec(t, `INSERT INTO t SELECT i, i*10 FROM generate_series(1, 10000) AS g(i)`)

	r.Exec(t, fmt.Sprintf(`
	 SELECT crdb_internal.compact_engine_span(
		 %d, %d,
		 (SELECT raw_start_key FROM [SHOW RANGES FROM TABLE t WITH KEYS] LIMIT 1),
		 (SELECT raw_end_key FROM [SHOW RANGES FROM TABLE t WITH KEYS] LIMIT 1))`,
		nodeIDArg, storeIDArg))

	rows := r.Query(t, fmt.Sprintf(`
	 SELECT * FROM crdb_internal.sstable_metrics(
		 %d, %d,
		 (SELECT raw_start_key FROM [SHOW RANGES FROM TABLE t WITH KEYS] LIMIT 1),
		 (SELECT raw_end_key FROM [SHOW RANGES FROM TABLE t WITH KEYS] LIMIT 1))`,
		nodeIDArg, storeIDArg))

	count := 0
	var nodeID int
	var storeID int
	var level int
	var fileNum int
	var approximateSpanBytes uint64
	var metrics []byte

	for rows.Next() {
		require.NoError(t, rows.Scan(&nodeID, &storeID, &level, &fileNum, &approximateSpanBytes, &metrics))
		require.NoError(t, json.Unmarshal(metrics, &enginepb.SSTableMetricsInfo{}))
		require.Equal(t, nodeID, nodeIDArg)
		require.Equal(t, storeID, storeIDArg)
		require.NotEqual(t, fileNum, 0)
		require.NotEqual(t, approximateSpanBytes, 0)
		count++
	}
	require.GreaterOrEqual(t, count, 1)
}

func TestScanStorageInternalKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	const numNodes = 3
	tc := serverutils.StartCluster(t, numNodes, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	sqlDB.Exec(t, `CREATE TABLE t(k INT PRIMARY KEY, v INT)`)
	sqlDB.Exec(t, `INSERT INTO t SELECT i, i*10 FROM generate_series(1, 1000) AS g(i)`)
	require.NoError(t, tc.WaitForFullReplication())

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	randKey := func() []byte {
		k := make([]byte, 1+rng.Intn(5))
		for i := range k {
			k[i] = byte(rng.Intn(256))
		}
		return k
	}
	for i := 0; i < 10000; i++ {
		a := randKey()
		b := a
		for bytes.Equal(a, b) {
			b = randKey()
		}
		if bytes.Compare(a, b) < 0 {
			a, b = b, a
		}
		n := 1 + rng.Intn(numNodes)
		sqlDB.QueryStr(t, `SELECT crdb_internal.scan_storage_internal_keys($1, $2, $3, $4, 100)`, n, n, a, b)
	}
}
