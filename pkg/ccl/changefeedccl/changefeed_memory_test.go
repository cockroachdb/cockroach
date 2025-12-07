// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// TestChangefeedUnwatchedFamilyMemoryMonitoring verifies that changefeeds
// correctly release memory allocations for events corresponding to unwatched
// column families after discarding them. This is a regression test for #154776.
func TestChangefeedUnwatchedFamilyMemoryMonitoring(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	rnd, _ := randutil.NewTestRand()

	testFn := func(t *testing.T, s TestServer, f cdctest.TestFeedFactory) {
		sqlDB := sqlutils.MakeSQLRunner(s.DB)

		// Set a low memory limit.
		changefeedbase.PerChangefeedMemLimit.Override(
			ctx, &s.Server.ClusterSettings().SV, 1<<20 /* 1 MiB */)

		// Create a table with two column families.
		sqlDB.Exec(t, `CREATE TABLE foo (
			id INT PRIMARY KEY,
			a STRING,
			b STRING,
			FAMILY f1 (id, a),
			FAMILY f2 (b)
		)`)

		// Insert initial data.
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a', 'b')`)

		var args []any
		if _, ok := f.(*webhookFeedFactory); ok {
			args = append(args, optOutOfMetamorphicEnrichedEnvelope{
				reason: "enriched envelopes do not support column families for webhook sinks",
			})
		}

		// Start changefeed watching only f1 with diff enabled.
		// Events from f2 should trigger ErrUnwatchedFamily.
		feed := feed(t, f,
			`CREATE CHANGEFEED FOR foo FAMILY f1 WITH diff, initial_scan='no', resolved`, args...)
		defer closeFeed(t, feed)

		// Update a watched column to generate an event.
		sqlDB.Exec(t, `UPDATE foo SET a = 'a_1' WHERE id = 1`)
		assertPayloads(t, feed, []string{
			`foo.f1: [1]->{"after": {"a": "a_1", "id": 1}, "before": {"a": "a", "id": 1}}`,
		})

		// Generate a lot of events for the unwatched family. If the memory
		// allocations are being leaked, this would cause the changefeed to
		// exceed the previously configured 1 MiB limit and become stuck
		// when it attempts to process more events after the limit is hit.
		const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
		for range 1000 {
			// Each update will create an event of size ~2 KiB
			// (~1 KiB for each of before/after).
			data := make([]byte, 1<<10 /* 1 KiB */)
			for i := range data {
				data[i] = charset[rnd.Intn(len(charset))]
			}
			sqlDB.Exec(t, `UPDATE foo SET b = $1 WHERE id = 1`, data)
		}

		// Update watched column again to verify the feed is still progressing.
		// If the memory allocations leaked, this assertion would time out
		// because the changefeed would be stuck.
		sqlDB.Exec(t, `UPDATE foo SET a = 'a_2' WHERE id = 1`)
		assertPayloads(t, feed, []string{
			`foo.f1: [1]->{"after": {"a": "a_2", "id": 1}, "before": {"a": "a_1", "id": 1}}`,
		})
	}

	cdcTest(t, testFn)
}
