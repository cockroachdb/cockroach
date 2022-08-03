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

	"github.com/cockroachdb/cockroach/pkg/base"
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
	numrows, err := sql.RunCopyFrom(ctx, s, "c", nil, "COPY lineitem FROM STDIN WITH CSV DELIMITER '|';", rows)
	require.NoError(t, err)
	require.Equal(t, 2, numrows)

	partKey := 0
	r.QueryRow(t, "SELECT l_partkey FROM c.lineitem WHERE l_orderkey = 1").Scan(&partKey)
	require.Equal(t, 155190, partKey)
}

// BenchmarkCopy measures copy performance against a TestServer.
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
	ROWS := 50000
	datalen := 0
	var rows []string
	for i := 0; i < ROWS; i++ {
		row := fmt.Sprintf(csvData, i)
		rows = append(rows, row)
		datalen += len(row)
	}
	start := timeutil.Now()
	pprof.Do(ctx, pprof.Labels("run", "copy"), func(ctx context.Context) {
		rows, err := sql.RunCopyFrom(ctx, s, "c", nil, "COPY lineitem FROM STDIN WITH CSV DELIMITER '|';", rows)
		require.NoError(b, err)
		require.Equal(b, ROWS, rows)
	})
	duration := timeutil.Since(start)
	b.ReportMetric(float64(datalen)/(1024*1024)/duration.Seconds(), "mb/s")
	b.ReportMetric(float64(ROWS)/duration.Seconds(), "rows/s")
}

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
	ROWS := 50000
	datalen := 0
	THREADS := 10
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
			count, err := sql.RunCopyFrom(ctx, s, "c", nil, "COPY lineitem FROM STDIN WITH CSV DELIMITER '|';", allrows[j])
			require.NoError(b, err)
			require.Equal(b, chunk, count)
		}(j)
	}
	wg.Wait()
	duration := timeutil.Since(start)
	b.ReportMetric(float64(datalen)/(1024*1024)/duration.Seconds(), "mb/s")
	b.ReportMetric(float64(ROWS)/duration.Seconds(), "rows/s")
}
