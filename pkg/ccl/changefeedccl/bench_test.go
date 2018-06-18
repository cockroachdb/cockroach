// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
)

func BenchmarkChangefeed(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer utilccl.TestingEnableEnterprise()()
	defer log.Scope(b).Close(b)

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(b, base.TestServerArgs{UseDatabase: "d"})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	sqlDB.Exec(b, `CREATE DATABASE d`)
	sqlDB.Exec(b, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`)

	const numRows = 1000
	bankTable := bank.FromRows(numRows).Tables()[0]
	timestamps, benchBytes, err := loadWorkloadBatches(sqlDB.DB, bankTable)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(benchBytes)

	// TODO(dan): This advances the clock through the timestamps of the ingested
	// data every time it's called, but that's a little unsatisfying. Instead,
	// wait for each batch to come out of the feed before advancing the
	// timestamp.
	var feedTimeIdx int
	feedClock := hlc.NewClock(func() int64 {
		if feedTimeIdx < len(timestamps) {
			feedTimeIdx++
			return timestamps[feedTimeIdx-1].UnixNano()
		}
		return timeutil.Now().UnixNano()
	}, time.Nanosecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resultsCh := make(chan tree.Datums, 1)

		b.StartTimer()
		cancelFeed := createBenchmarkChangefeed(ctx, s, feedClock, `d`, `bank`, resultsCh)
		for rows := 0; rows < numRows; rows++ {
			<-resultsCh
		}
		b.StopTimer()

		if err := cancelFeed(); err != nil {
			b.Errorf(`%+v`, err)
		}
	}
}
