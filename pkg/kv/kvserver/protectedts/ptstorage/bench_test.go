// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ptstorage_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptstorage"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// workerCounts is the parallelism sweep shared by the throughput benchmarks
// in this file.
// var workerCounts = []int{1, 2, 4, 8, 16, 32, 64, 128}
var workerCounts = []int{128}

// startBenchServer brings up a single-node in-process server suitable for the
// throughput benchmarks.
func startBenchServer(b *testing.B) (protectedts.Storage, func()) {
	ctx := context.Background()
	srv := serverutils.StartServerOnly(b, base.TestServerArgs{})
	s := srv.ApplicationLayer()

	protectedts.MaxBytes.Override(ctx, &s.ClusterSettings().SV, 0)
	protectedts.MaxSpans.Override(ctx, &s.ClusterSettings().SV, 0)

	pts := ptstorage.WithDatabase(
		ptstorage.New(s.ClusterSettings(), &protectedts.TestingKnobs{}),
		s.InternalDB().(isql.DB),
	)
	return pts, func() { srv.Stopper().Stop(ctx) }
}

// runConcurrent invokes op b.N times spread across `workers` goroutines and
// then waits for them all to finish. op receives the per-invocation index in
// [0, b.N), which is useful for indexing into a per-iteration data set.
//
// Errors returned by op are counted and reported as the "errs/op" metric
// rather than failing the benchmark. Under heavy contention some operations
// can exhaust the internal kv retry budget; counting rather than failing lets
// us still produce a comparable baseline number.
func runConcurrent(b *testing.B, ctx context.Context, workers int, op func(idx int) error) {
	var (
		next atomic.Int64
		errs atomic.Int64
	)

	b.ResetTimer()
	err := ctxgroup.GroupWorkers(ctx, workers, func(context.Context, int) error {
		for {
			idx := int(next.Add(1) - 1)
			if idx >= b.N {
				return nil
			}
			if err := op(idx); err != nil {
				errs.Add(1)
			}
		}
	})
	require.NoError(b, err)
	b.StopTimer()
	b.ReportMetric(float64(errs.Load())/float64(b.N), "errs/op")
}

// BenchmarkProtect measures the throughput of protectedts.Storage.Protect
// calls as a function of writer concurrency. Each iteration writes one new
// record. Per-op wall-clock time reported by the framework is the inverse of
// sustained throughput: ops/sec = 1e9 / ns/op.
func BenchmarkProtect(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	pts, stop := startBenchServer(b)
	defer stop()

	now := hlc.Timestamp{WallTime: 1}
	target := ptpb.MakeSchemaObjectsTarget([]descpb.ID{42})

	for _, workers := range workerCounts {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			runConcurrent(b, ctx, workers, func(_ int) error {
				rec := ptpb.Record{
					ID:        uuid.MakeV4().GetBytes(),
					Timestamp: now,
					Mode:      ptpb.PROTECT_AFTER,
					Target:    target,
				}
				return pts.Protect(ctx, &rec)
			})
		})
	}
}

// BenchmarkRelease measures the throughput of protectedts.Storage.Release
// calls as a function of writer concurrency. Each sub-benchmark first
// pre-populates b.N records (the setup time is excluded from the
// measurement) and then releases each one exactly once.
//
// Like Protect, Release is a read-modify-write against the singleton meta
// row, so it shares Protect's contention shape on that row.
func BenchmarkRelease(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	pts, stop := startBenchServer(b)
	defer stop()

	now := hlc.Timestamp{WallTime: 1}
	target := ptpb.MakeSchemaObjectsTarget([]descpb.ID{42})

	for _, workers := range workerCounts {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			ids := prePopulate(b, ctx, pts, workers, b.N, now, target)
			runConcurrent(b, ctx, workers, func(idx int) error {
				return pts.Release(ctx, ids[idx])
			})
		})
	}
}

// prePopulate inserts n protected timestamp records in parallel using
// `workers` goroutines and returns their IDs. It is intended for use as
// benchmark setup (before b.ResetTimer); a Protect failure here is fatal
// because it means the benchmark has nothing meaningful to measure.
func prePopulate(
	b *testing.B,
	ctx context.Context,
	pts protectedts.Storage,
	workers, n int,
	ts hlc.Timestamp,
	target *ptpb.Target,
) []uuid.UUID {
	ids := make([]uuid.UUID, n)
	var next atomic.Int64
	err := ctxgroup.GroupWorkers(ctx, workers, func(context.Context, int) error {
		for {
			idx := int(next.Add(1) - 1)
			if idx >= n {
				return nil
			}
			id := uuid.MakeV4()
			rec := ptpb.Record{
				ID:        id.GetBytes(),
				Timestamp: ts,
				Mode:      ptpb.PROTECT_AFTER,
				Target:    target,
			}
			if err := pts.Protect(ctx, &rec); err != nil {
				return err
			}
			ids[idx] = id
		}
	})
	require.NoError(b, err)
	return ids
}
