// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sctestdeps

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
)

type backfillTrackerDeps interface {
	Catalog() scexec.Catalog
	Codec() keys.SQLCodec
}

type testBackfillerTracker struct {
	deps backfillTrackerDeps
}

// BackfillProgressTracker implements the scexec.Dependencies interface.
func (s *TestState) BackfillProgressTracker() scexec.BackfillerTracker {
	return s.backfillTracker
}

var _ scexec.BackfillerTracker = (*testBackfillerTracker)(nil)

func (s *testBackfillerTracker) GetBackfillProgress(
	ctx context.Context, b scexec.Backfill,
) (scexec.BackfillProgress, error) {
	return scexec.BackfillProgress{Backfill: b}, nil
}

func (s *testBackfillerTracker) GetMergeProgress(
	ctx context.Context, m scexec.Merge,
) (scexec.MergeProgress, error) {
	return scexec.MergeProgress{Merge: m}, nil

}

func (s *testBackfillerTracker) SetBackfillProgress(
	ctx context.Context, progress scexec.BackfillProgress,
) error {
	return nil
}

func (s *testBackfillerTracker) SetMergeProgress(
	ctx context.Context, progress scexec.MergeProgress,
) error {
	return nil
}

func (s *testBackfillerTracker) FlushCheckpoint(ctx context.Context) error {
	return nil
}

func (s *testBackfillerTracker) FlushFractionCompleted(ctx context.Context) error {
	return nil
}

// StartPeriodicFlush implements the scexec.PeriodicProgressFlusher interface.
func (s *TestState) StartPeriodicFlush(
	ctx context.Context,
) (close func(context.Context) error, _ error) {
	return func(ctx context.Context) error { return nil }, nil
}

type testBackfiller struct {
	s *TestState
}

var _ scexec.Backfiller = (*testBackfiller)(nil)

// BackfillIndexes implements the scexec.Backfiller interface.
func (s *testBackfiller) BackfillIndexes(
	_ context.Context,
	progress scexec.BackfillProgress,
	_ scexec.BackfillerProgressWriter,
	_ *jobs.Job,
	tbl catalog.TableDescriptor,
) error {
	s.s.LogSideEffectf(
		"backfill indexes %v from index #%d in table #%d",
		progress.DestIndexIDs, progress.SourceIndexID, tbl.GetID(),
	)
	return nil
}

// MaybePrepareDestIndexesForBackfill implements the scexec.Backfiller interface.
func (s *testBackfiller) MaybePrepareDestIndexesForBackfill(
	ctx context.Context, progress scexec.BackfillProgress, descriptor catalog.TableDescriptor,
) (scexec.BackfillProgress, error) {
	return progress, nil
}

var _ scexec.Merger = (*testBackfiller)(nil)

// MergeIndexes implements the scexec.Merger interface.
func (s *testBackfiller) MergeIndexes(
	_ context.Context,
	job *jobs.Job,
	progress scexec.MergeProgress,
	_ scexec.BackfillerProgressWriter,
	tbl catalog.TableDescriptor,
) error {
	s.s.LogSideEffectf(
		"merge temporary indexes %v into backfilled indexes %v in table #%d",
		progress.SourceIndexIDs, progress.DestIndexIDs, tbl.GetID(),
	)
	return nil
}
