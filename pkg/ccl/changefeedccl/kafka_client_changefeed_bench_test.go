// Copyright 2025 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	gosql "database/sql"
	"fmt"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/lib/pq"
)

func BenchmarkChangefeedKafkaLinger(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	// Setup test cluster with cleanup verification.
	cluster, _, cleanup := startTestCluster(b)
	defer cleanup()
	s := cluster.Server(1)

	pgURL, cleanup := pgurlutils.PGUrl(b, s.SQLAddr(), b.Name(), url.User(username.RootUser))
	defer cleanup()
	pgBase, err := pq.NewConnector(pgURL.String())
	if err != nil {
		b.Fatal(err)
	}
	connector := pq.ConnectorWithNoticeHandler(pgBase, func(n *pq.Error) {
	})

	dbWithHandler := gosql.OpenDB(connector)
	defer dbWithHandler.Close()

	sqlDB := sqlutils.MakeSQLRunner(dbWithHandler)

	sqlDB.Exec(b, `CREATE DATABASE a`)
	sqlDB.Exec(b, `CREATE TABLE a.bench_table (
		id INT PRIMARY KEY,
		val STRING
	)`)

	// Enable the v2 Kafka sink which supports lingering.
	sqlDB.Exec(b, `SET CLUSTER SETTING changefeed.new_kafka_sink.enabled = true`)

	testCases := []struct {
		name     string
		lingerMs int
		config   string
	}{
		{"NoLinger", 0, ``},
		{"Linger10ms", 10, `kafka_sink_config='{"ProducerLinger": "1ms"}'`},
		{"Linger50ms", 50, `kafka_sink_config='{"ProducerLinger": "5ms"}'`},
		{"Linger100ms", 100, `kafka_sink_config='{"ProducerLinger": "10ms"}'`},
		{"Linger150ms", 150, `kafka_sink_config='{"ProducerLinger": "50ms"}'`},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// Reset table before every sub-benchmark run.
			sqlDB.Exec(b, `DROP TABLE IF EXISTS a.bench_table CASCADE`)
			sqlDB.Exec(b, `CREATE TABLE a.bench_table (
				id INT PRIMARY KEY,
				val STRING
			)`)

			f := makeKafkaBenchFeedFactory(b, s, dbWithHandler)

			// Create the changefeed with specified linger setting.
			var createStmt string
			if tc.config == `` {
				createStmt = `CREATE CHANGEFEED FOR a.bench_table`
			} else {
				createStmt = fmt.Sprintf(`CREATE CHANGEFEED FOR a.bench_table WITH %s`, tc.config)
			}

			feed := feed(b, f, createStmt)
			defer closeFeed(b, feed)

			benchFeed := feed.(*kafkaBenchFeed)

			// Track the next ID to use across all iterations.
			nextID := 1

			b.ResetTimer()
			const batchSize = 2000
			for i := 0; i < b.N; i++ {
				start := timeutil.Now()
				// Use sequential IDs that never overlap
				startID := nextID
				endID := startID + batchSize - 1
				nextID = endID + 1 // Update for next iteration

				sqlDB.Exec(b, `
					INSERT INTO a.bench_table (id, val)
					SELECT generate_series($1, $2), 'test-value'
				`, startID, endID)

				benchFeed.metrics.recordLatency(timeutil.Since(start))
				benchFeed.metrics.updateMsgCount(batchSize)
			}

			metrics := benchFeed.GetMetrics()
			elapsed := timeutil.Since(metrics.startTime)
			throughput := float64(metrics.msgCount) / elapsed.Seconds()
			b.ReportMetric(throughput, "msgs/sec")

			metrics.mu.Lock()
			sortedLatencies := append([]time.Duration(nil), metrics.latencies...)
			metrics.mu.Unlock()

			if len(sortedLatencies) > 0 {
				slices.Sort(sortedLatencies)
				p50Idx := len(sortedLatencies) * 50 / 100
				p95Idx := len(sortedLatencies) * 95 / 100
				p99Idx := len(sortedLatencies) * 99 / 100

				b.ReportMetric(float64(sortedLatencies[p50Idx].Microseconds()), "p50-latency-us")
				b.ReportMetric(float64(sortedLatencies[p95Idx].Microseconds()), "p95-latency-us")
				b.ReportMetric(float64(sortedLatencies[p99Idx].Microseconds()), "p99-latency-us")
			}
		})
	}
}
