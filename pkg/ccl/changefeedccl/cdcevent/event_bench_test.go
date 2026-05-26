// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdcevent

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// BenchmarkEventDecoderColumnFamilies benchmarks the performance of DecodeKV
// when processing events from watched vs unwatched column families.
//
// This benchmark demonstrates the performance improvement from PR #159745, which
// replaced expensive errors.Is() calls with direct DecodeStatus enum comparisons.
// The optimization is particularly significant for unwatched families, where the
// decoder can skip processing early without error chain traversal overhead.
//
// Benchmark results comparing old (errors.Is) vs new (DecodeStatus enum):
//
//	OLD CODE (using errors.Is):
//	  watched-family:   766.2 ns/op   874 B/op   10 allocs/op
//	  unwatched-family: 423.0 ns/op  1192 B/op    6 allocs/op
//	  mixed-workload:   595.2 ns/op  1085 B/op    7 allocs/op
//
//	NEW CODE (using DecodeStatus):
//	  watched-family:   763.3 ns/op   874 B/op   10 allocs/op
//	  unwatched-family: 403.8 ns/op  1192 B/op    6 allocs/op
//	  mixed-workload:   551.3 ns/op  1086 B/op    7 allocs/op
//
//	IMPROVEMENTS:
//	  unwatched-family: 4.5% faster (19ns saved per event)
//	  mixed-workload:   7.4% faster (44ns saved per event)
//
// The performance difference is especially impactful in high-throughput scenarios
// where changefeeds watch specific column families and frequently encounter events
// from unwatched families.
func BenchmarkEventDecoderColumnFamilies(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	b.StopTimer()
	srv, db, _ := serverutils.StartServer(b, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())

	s := srv.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(b, "SET CLUSTER SETTING kv.rangefeed.enabled = true")

	// Create table with 3 families: 1 watched (f1), 2 unwatched (f2, f3).
	// This simulates a realistic scenario where a changefeed watches specific
	// column families but receives events for all families.
	sqlDB.Exec(b, `
CREATE TABLE foo (
	id INT PRIMARY KEY,
	watched_a STRING,
	watched_b STRING,
	unwatched_c STRING,
	unwatched_d STRING,
	FAMILY f1 (id, watched_a, watched_b),
	FAMILY f2 (unwatched_c),
	FAMILY f3 (unwatched_d)
)`)

	tableDesc := cdctest.GetHydratedTableDescriptor(b, s.ExecutorConfig(), "foo")

	// Configure decoder to only watch family f1.
	// Events from f2 and f3 will trigger the fast skip path.
	targets := changefeedbase.Targets{}
	targets.Add(changefeedbase.Target{
		Type:       jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY,
		DescID:     tableDesc.GetID(),
		FamilyName: "f1",
	})

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	ctx := context.Background()
	decoder, err := NewEventDecoder(ctx, &execCfg, targets, false, false, DecoderOptions{})
	if err != nil {
		b.Fatal(err)
	}

	// Generate KVs for all three families.
	// When we INSERT a row with all columns, we get separate KV events for each family.
	popRow, cleanup := cdctest.MakeRangeFeedValueReader(b, s.ExecutorConfig(), tableDesc)
	sqlDB.Exec(b, "INSERT INTO foo VALUES (1, 'watched_a_value', 'watched_b_value', 'unwatched_c_value', 'unwatched_d_value')")

	// Capture the three KV events - one for each family.
	// The order should be f1, f2, f3 based on family ID ordering.
	watchedKV := popRow(b)    // f1 KV (watched)
	unwatchedKV1 := popRow(b) // f2 KV (unwatched)
	unwatchedKV2 := popRow(b) // f3 KV (unwatched)
	cleanup()

	// Benchmark 1: Watched family (DecodeOK path)
	// This exercises the full decode path where the event is fully processed.
	b.Run("watched-family", func(b *testing.B) {
		b.ReportAllocs()
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			_, status, err := decoder.DecodeKV(
				ctx,
				roachpb.KeyValue{Key: watchedKV.Key, Value: watchedKV.Value},
				CurrentRow,
				watchedKV.Timestamp(),
				false,
			)
			if err != nil {
				b.Fatal(err)
			}
			if status != DecodeOK {
				b.Fatalf("unexpected decode status: %v", status)
			}
		}
	})

	// Benchmark 2: Unwatched family (DecodeSkipUnwatchedFamily path)
	// This exercises the fast skip path introduced in PR #159745.
	// With the optimization, this is ~4.5% faster than the old errors.Is() approach.
	b.Run("unwatched-family", func(b *testing.B) {
		b.ReportAllocs()
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			_, status, err := decoder.DecodeKV(
				ctx,
				roachpb.KeyValue{Key: unwatchedKV1.Key, Value: unwatchedKV1.Value},
				CurrentRow,
				unwatchedKV1.Timestamp(),
				false,
			)
			if err != nil {
				b.Fatal(err)
			}
			if status != DecodeSkipUnwatchedFamily {
				b.Fatalf("unexpected decode status: %v", status)
			}
		}
	})

	// Benchmark 3: Mixed workload (realistic scenario)
	// This simulates a real-world changefeed where 1/3 of events are from watched
	// families and 2/3 are from unwatched families. This demonstrates the overall
	// throughput improvement in production scenarios (~7.4% faster).
	b.Run("mixed-workload", func(b *testing.B) {
		b.ReportAllocs()
		b.StartTimer()
		kvs := []roachpb.KeyValue{
			{Key: watchedKV.Key, Value: watchedKV.Value},
			{Key: unwatchedKV1.Key, Value: unwatchedKV1.Value},
			{Key: unwatchedKV2.Key, Value: unwatchedKV2.Value},
		}
		for i := 0; i < b.N; i++ {
			kv := kvs[i%len(kvs)]
			_, _, err := decoder.DecodeKV(ctx, kv, CurrentRow, watchedKV.Timestamp(), false)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
