// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"fmt"
	"runtime/pprof"
	"sync"
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

var lineitemSchema string = `CREATE DATABASE c; CREATE TABLE c.lineitem (
	l_orderkey      INT8 NOT NULL,
	l_partkey       INT8 NOT NULL,
	l_suppkey       INT8 NOT NULL,
	l_linenumber    INT8 NOT NULL,
	l_quantity      DECIMAL(15,2) NOT NULL,
	l_extendedprice DECIMAL(15,2) NOT NULL,
	l_discount      DECIMAL(15,2) NOT NULL,
	l_tax           DECIMAL(15,2) NOT NULL,
	l_returnflag    CHAR(1) NOT NULL,
	l_linestatus    CHAR(1) NOT NULL,
	l_shipdate      DATE NOT NULL,
	l_commitdate    DATE NOT NULL,
	l_receiptdate   DATE NOT NULL,
	l_shipinstruct  CHAR(25) NOT NULL,
	l_shipmode      CHAR(10) NOT NULL,
	l_comment       VARCHAR(44) NOT NULL,
	PRIMARY KEY     (l_orderkey, l_linenumber),
	INDEX l_ok      (l_orderkey ASC),
	INDEX l_pk      (l_partkey ASC),
	INDEX l_sk      (l_suppkey ASC),
	INDEX l_sd      (l_shipdate ASC),
	INDEX l_cd      (l_commitdate ASC),
	INDEX l_rd      (l_receiptdate ASC),
	INDEX l_pk_sk   (l_partkey ASC, l_suppkey ASC),
	INDEX l_sk_pk   (l_suppkey ASC, l_partkey ASC)
)`

const csvData = `%d|155190|7706|1|17|21168.23|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the
`

func doCopyEx(
	ctx context.Context,
	t require.TestingT,
	s serverutils.TestServerInterface,
	txn *kv.Txn,
	rows []string,
	batchSizeOverride int,
	atomic bool,
) {
	numrows, err := sql.RunCopyFrom(ctx, s, "c", nil /* txn */, "COPY lineitem FROM STDIN WITH CSV DELIMITER '|';", rows, batchSizeOverride, atomic)
	require.NoError(t, err)
	require.Equal(t, len(rows), numrows)
}

func doCopyImplicit(
	ctx context.Context, t require.TestingT, s serverutils.TestServerInterface, rows []string,
) {
	doCopyEx(ctx, t, s, nil, rows, 0, true)
}

func doCopyWithTxn(
	ctx context.Context,
	t require.TestingT,
	s serverutils.TestServerInterface,
	txn *kv.Txn,
	rows []string,
) {
	doCopyEx(ctx, t, s, txn, rows, 0, true)
}

func doCopyOneRowBatches(
	ctx context.Context,
	t require.TestingT,
	s serverutils.TestServerInterface,
	rows []string,
	atomic bool,
) {
	doCopyEx(ctx, t, s, nil, rows, 1, atomic)
}

// TestCopyFrom is a simple test to verify RunCopyFrom works for benchmarking
// purposes.
func TestCopyFrom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: cluster.MakeTestingClusterSettings(),
	})
	defer s.Stopper().Stop(ctx)

	r := sqlutils.MakeSQLRunner(conn)
	r.Exec(t, lineitemSchema)
	rows := []string{fmt.Sprintf(csvData, 1), fmt.Sprintf(csvData, 2)}
	doCopyImplicit(ctx, t, s, rows)

	partKey := 0
	r.QueryRow(t, "SELECT l_partkey FROM c.lineitem WHERE l_orderkey = 1").Scan(&partKey)
	require.Equal(t, 155190, partKey)
}

// TestCopyFromExplicitTransaction tests that copy from rows are written with
// same transaction timestamp.
func TestCopyFromExplicitTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s, conn, db := serverutils.StartServer(t, base.TestServerArgs{
		Settings: cluster.MakeTestingClusterSettings(),
	})
	defer s.Stopper().Stop(ctx)

	r := sqlutils.MakeSQLRunner(conn)
	r.Exec(t, lineitemSchema)
	rows := []string{fmt.Sprintf(csvData, 1), fmt.Sprintf(csvData, 2)}
	txn := db.NewTxn(ctx, "test")
	doCopyWithTxn(ctx, t, s, txn, rows)
	if err := txn.Commit(ctx); err != nil {
		require.NoError(t, err)
	}
	partKey := 0
	r.QueryRow(t, "SELECT l_partkey FROM c.lineitem WHERE l_orderkey = 1").Scan(&partKey)
	require.Equal(t, 155190, partKey)

	sqlRows := r.Query(t, "SELECT crdb_internal_mvcc_timestamp FROM c.lineitem")
	var lastts float64
	firstTime := true
	for sqlRows.Next() {
		var ts float64
		err := sqlRows.Scan(&ts)
		require.NoError(t, err)
		if !firstTime {
			require.EqualValues(t, lastts, ts)
		} else {
			firstTime = false
		}
		lastts = ts
	}
}

// TestCopyFromImplicitAtomicTransaction tests that copy from rows are
// not committed in batches (22.2 default behavior).
func TestCopyFromImplicitAtomicTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: cluster.MakeTestingClusterSettings(),
	})
	defer s.Stopper().Stop(ctx)

	r := sqlutils.MakeSQLRunner(conn)
	r.Exec(t, lineitemSchema)
	rows := []string{fmt.Sprintf(csvData, 1), fmt.Sprintf(csvData, 2)}
	doCopyOneRowBatches(ctx, t, s, rows, true /* atomic */)

	partKey := 0
	r.QueryRow(t, "SELECT l_partkey FROM c.lineitem WHERE l_orderkey = 1").Scan(&partKey)
	require.Equal(t, 155190, partKey)

	sqlRows := r.Query(t, "SELECT crdb_internal_mvcc_timestamp FROM c.lineitem")
	var lastts apd.Decimal
	firstTime := true
	for sqlRows.Next() {
		var ts apd.Decimal
		err := sqlRows.Scan(&ts)
		require.NoError(t, err)
		if !firstTime {
			require.EqualValues(t, lastts, ts)
		} else {
			firstTime = false
		}
		lastts = ts
	}
}

// TestCopyFromImplicitNonAtomicTransaction tests that copy from rows are
// committed in batches (pre-22.2 default behavior).
func TestCopyFromImplicitNonAtomicTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: cluster.MakeTestingClusterSettings(),
	})
	defer s.Stopper().Stop(ctx)

	r := sqlutils.MakeSQLRunner(conn)
	r.Exec(t, lineitemSchema)
	rows := []string{fmt.Sprintf(csvData, 1), fmt.Sprintf(csvData, 2)}
	doCopyOneRowBatches(ctx, t, s, rows, false /* atomic */)

	partKey := 0
	r.QueryRow(t, "SELECT l_partkey FROM c.lineitem WHERE l_orderkey = 1").Scan(&partKey)
	require.Equal(t, 155190, partKey)

	sqlRows := r.Query(t, "SELECT crdb_internal_mvcc_timestamp FROM c.lineitem")
	var lastts apd.Decimal
	firstTime := true
	for sqlRows.Next() {
		var ts apd.Decimal
		err := sqlRows.Scan(&ts)
		require.NoError(t, err)
		if !firstTime {
			require.NotEqualValues(t, lastts, ts)
		} else {
			firstTime = false
		}
		lastts = ts
	}
}

// BenchmarkCopyFrom measures copy performance against a TestServer.
func BenchmarkCopyFrom(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	s, conn, _ := serverutils.StartServer(b, base.TestServerArgs{
		Settings: cluster.MakeTestingClusterSettings(),
	})
	defer s.Stopper().Stop(ctx)

	r := sqlutils.MakeSQLRunner(conn)
	r.Exec(b, lineitemSchema)

	// send data in 5 batches of 10k rows
	const ROWS = sql.CopyBatchRowSizeDefault * 4
	datalen := 0
	var rows []string
	for i := 0; i < ROWS; i++ {
		row := fmt.Sprintf(csvData, i)
		rows = append(rows, row)
		datalen += len(row)
	}
	rowsize := datalen / ROWS
	for _, batchSizeFactor := range []float64{.5, 1, 2, 4} {
		batchSize := int(batchSizeFactor * sql.CopyBatchRowSizeDefault)
		b.Run(fmt.Sprintf("%d", batchSize), func(b *testing.B) {
			actualRows := rows[:batchSize]
			for i := 0; i < b.N; i++ {
				pprof.Do(ctx, pprof.Labels("run", "copy"), func(ctx context.Context) {
					doCopyImplicit(ctx, b, s, actualRows)
				})
				b.StopTimer()
				r.Exec(b, "TRUNCATE TABLE c.lineitem")
				b.StartTimer()
			}
			b.SetBytes(int64(len(actualRows) * rowsize))
		})
	}
}

// BenchmarkParallelCopyFrom benchmarks break copy up into separate chunks in separate goroutines.
func BenchmarkParallelCopyFrom(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	s, conn, _ := serverutils.StartServer(b, base.TestServerArgs{
		Settings: cluster.MakeTestingClusterSettings(),
	})
	defer s.Stopper().Stop(ctx)

	r := sqlutils.MakeSQLRunner(conn)
	r.Exec(b, lineitemSchema)
	const ROWS = 50000
	datalen := 0
	const THREADS = 10
	var allrows [][]string

	chunk := ROWS / THREADS
	for j := 0; j < THREADS; j++ {
		var rows []string
		for i := 0; i < chunk; i++ {
			row := fmt.Sprintf(csvData, i+j*chunk)
			rows = append(rows, row)
			datalen += len(row)
		}
		allrows = append(allrows, rows)
	}

	start := timeutil.Now()
	var wg sync.WaitGroup

	for j := 0; j < THREADS; j++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			doCopyImplicit(ctx, b, s, allrows[j])
		}(j)
	}
	wg.Wait()
	duration := timeutil.Since(start)
	b.ReportMetric(float64(datalen)/(1024*1024)/duration.Seconds(), "mb/s")
	b.ReportMetric(float64(ROWS)/duration.Seconds(), "rows/s")
}
