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
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
)

func BenchmarkChangefeedTicks(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(b, base.TestServerArgs{UseDatabase: "d"})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	sqlDB.Exec(b, `CREATE DATABASE d`)
	sqlDB.Exec(b, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`)

	numRows := 1000
	if testing.Short() {
		numRows = 100
	}
	bankTable := bank.FromRows(numRows).Tables()[0]
	timestamps, _, err := loadWorkloadBatches(sqlDBRaw, bankTable)
	if err != nil {
		b.Fatal(err)
	}

	runBench := func(b *testing.B, feedClock *hlc.Clock) {
		var sinkBytes int64
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StartTimer()
			sink, cancelFeed := createBenchmarkChangefeed(ctx, s, feedClock, `d`, `bank`)
			for rows := 0; rows < numRows; {
				r, sb := sink.WaitForEmit()
				rows += r
				sinkBytes += sb
			}
			b.StopTimer()
			if err := cancelFeed(); err != nil {
				b.Errorf(`%+v`, err)
			}
		}
		b.SetBytes(sinkBytes / int64(b.N))
	}

	b.Run(`InitialScan`, func(b *testing.B) {
		// Use a clock that's immediately larger than any timestamp the data was
		// loaded at to catch it all in the initial scan.
		runBench(b, s.Clock())
	})

	b.Run(`SteadyState`, func(b *testing.B) {
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
		runBench(b, feedClock)
	})
}
