// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sctestdeps

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
)

type backfillTrackerDeps interface {
	Catalog() scexec.Catalog
	Codec() keys.SQLCodec
}

type testBackfillTracker struct {
	deps backfillTrackerDeps
}

// BackfillProgressTracker implements the scexec.Dependencies interface.
func (s *TestState) BackfillProgressTracker() scexec.BackfillTracker {
	return s.backfillTracker
}

var _ scexec.BackfillTracker = (*testBackfillTracker)(nil)

func (s *testBackfillTracker) GetBackfillProgress(
	ctx context.Context, b scexec.Backfill,
) (scexec.BackfillProgress, error) {
	desc, err := s.deps.Catalog().MustReadImmutableDescriptor(ctx, b.TableID)
	if err != nil {
		return scexec.BackfillProgress{}, err
	}
	table, err := catalog.AsTableDescriptor(desc)
	if err != nil {
		return scexec.BackfillProgress{}, err
	}
	return scexec.BackfillProgress{
		Backfill:  b,
		SpansToDo: []roachpb.Span{table.IndexSpan(s.deps.Codec(), b.SourceIndexID)},
	}, nil
}

func (s *testBackfillTracker) SetBackfillProgress(
	ctx context.Context, progress scexec.BackfillProgress,
) error {
	return nil
}

func (s *testBackfillTracker) FlushCheckpoint(ctx context.Context) error {
	return nil
}

func (s *testBackfillTracker) FlushFractionCompleted(ctx context.Context) error {
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

// BackfillIndex implements the scexec.Backfiller interface.
func (s *testBackfiller) BackfillIndex(
	_ context.Context,
	progress scexec.BackfillProgress,
	_ scexec.BackfillProgressWriter,
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

var _ scexec.IndexSpanSplitter = (*indexSpanSplitter)(nil)

type indexSpanSplitter struct{}

// MaybeSplitIndexSpans implements the scexec.IndexSpanSplitter interface.
func (s *indexSpanSplitter) MaybeSplitIndexSpans(
	_ context.Context, _ catalog.TableDescriptor, _ catalog.Index,
) error {
	return nil
}
