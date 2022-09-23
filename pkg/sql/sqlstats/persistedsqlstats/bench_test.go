// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package persistedsqlstats_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func BenchmarkConcurrentSelect1(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	for _, numOfConcurrentConn := range []int{24, 48, 64} {
		b.Run(fmt.Sprintf("concurrentConn=%d", numOfConcurrentConn), func(b *testing.B) {
			s, db, _ := serverutils.StartServer(b, base.TestServerArgs{})
			sqlServer := s.SQLServer().(*sql.Server)
			defer s.Stopper().Stop(ctx)

			starter := make(chan struct{})
			latencyChan := make(chan float64, numOfConcurrentConn)
			defer close(latencyChan)

			var wg sync.WaitGroup
			for connIdx := 0; connIdx < numOfConcurrentConn; connIdx++ {
				sqlConn, err := db.Conn(ctx)
				if err != nil {
					b.Fatalf("unexpected error creating db conn: %s", err)
				}
				wg.Add(1)

				go func(conn *gosql.Conn, idx int) {
					defer wg.Done()
					runner := sqlutils.MakeSQLRunner(conn)
					<-starter

					start := timeutil.Now()
					for i := 0; i < b.N; i++ {
						runner.Exec(b, "SELECT 1")
					}
					duration := timeutil.Since(start)
					latencyChan <- float64(duration.Milliseconds()) / float64(b.N)
				}(sqlConn, connIdx)
			}

			close(starter)
			wg.Wait()

			var totalLat float64
			for i := 0; i < numOfConcurrentConn; i++ {
				totalLat += <-latencyChan
			}
			b.ReportMetric(
				sqlServer.ServerMetrics.
					StatsMetrics.
					SQLTxnStatsCollectionOverhead.
					Mean(),
				"overhead(ns/op)")
		})
	}
}
