// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scexec

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"golang.org/x/sync/errgroup"
)

// TODO(ajwerner): Consider separating out the dependencies for the
// backfill from that of mutations and validation. The fact that there
// is a transaction hanging around in the dependencies is more likely to
// be confusing than valuable. Not much is being done transactionally.

func executeBackfillOps(ctx context.Context, deps Dependencies, execute []scop.Op) (err error) {
	backfillsToExecute, tables, err := extractBackfillsFromOps(
		ctx, deps.Catalog(), execute,
	)
	if err != nil {
		return err
	}
	tracker := deps.BackfillProgressTracker()
	progresses, err := loadProgressesAndMaybePerformInitialScan(
		ctx, deps, backfillsToExecute, tracker, tables,
	)
	if err != nil {
		return err
	}
	return runBackfills(ctx, deps, tracker, progresses, tables)
}

func extractBackfillsFromOps(
	ctx context.Context, cat Catalog, execute []scop.Op,
) ([]Backfill, map[descpb.ID]catalog.TableDescriptor, error) {
	var backfillsToExecute []Backfill
	tables := map[descpb.ID]catalog.TableDescriptor{}
	for _, op := range execute {
		switch op := op.(type) {
		case *scop.BackfillIndex:
			table, ok := tables[op.TableID]
			if !ok {
				desc, err := cat.MustReadImmutableDescriptor(ctx, op.TableID)
				if err != nil {
					return nil, nil, err
				}
				table = desc.(catalog.TableDescriptor)
				tables[op.TableID] = table
			}
			backfillsToExecute = append(backfillsToExecute, Backfill{
				TableID:       op.TableID,
				SourceIndexID: table.GetPrimaryIndexID(),
				DestIndexIDs:  []descpb.IndexID{op.IndexID},
			})
		default:
			panic("unimplemented")
		}
	}
	return mergeBackfillFromSameSource(backfillsToExecute), tables, nil
}

func mergeBackfillFromSameSource(backfillsToExecute []Backfill) []Backfill {
	sort.Slice(backfillsToExecute, func(i, j int) bool {
		if backfillsToExecute[i].TableID == backfillsToExecute[j].TableID {
			return backfillsToExecute[i].SourceIndexID < backfillsToExecute[j].SourceIndexID
		}
		return backfillsToExecute[i].TableID < backfillsToExecute[j].TableID
	})
	truncated := backfillsToExecute[:0]
	sameSource := func(a, b Backfill) bool {
		return a.TableID == b.TableID && a.SourceIndexID == b.SourceIndexID
	}
	for _, bf := range backfillsToExecute {
		if len(truncated) == 0 || !sameSource(truncated[len(truncated)-1], bf) {
			truncated = append(truncated, bf)
		} else {
			ord := len(truncated) - 1
			curIDs := truncated[ord].DestIndexIDs
			curIDs = curIDs[:len(curIDs):len(curIDs)]
			truncated[ord].DestIndexIDs = append(curIDs, bf.DestIndexIDs...)
		}
	}
	backfillsToExecute = truncated
	return backfillsToExecute
}

func loadProgressesAndMaybePerformInitialScan(
	ctx context.Context,
	deps Dependencies,
	backfillsToExecute []Backfill,
	tracker BackfillTracker,
	tables map[descpb.ID]catalog.TableDescriptor,
) ([]BackfillProgress, error) {
	progresses, err := loadProgresses(ctx, backfillsToExecute, tracker)
	if err != nil {
		return nil, err
	}
	{
		didScan, err := maybeScanDestinationIndexes(ctx, deps, progresses, tables, tracker)
		if err != nil {
			return nil, err
		}
		if didScan {
			if err := tracker.FlushCheckpoint(ctx); err != nil {
				return nil, err
			}
		}
	}
	return progresses, nil
}

// maybeScanDestinationIndexes runs a scan on any backfills in progresses
// which do not have their MinimumWriteTimestamp set. If a scan occurs for
// a backfill, the corresponding entry in progresses will be populated with
// the scan timestamp and the progress will be reported to the tracker.
// If any index was scanned successfully, didScan will be true.
func maybeScanDestinationIndexes(
	ctx context.Context,
	deps Dependencies,
	progresses []BackfillProgress,
	tables map[descpb.ID]catalog.TableDescriptor,
	tracker BackfillProgressWriter,
) (didScan bool, _ error) {
	g, ctx := errgroup.WithContext(ctx)
	for i := range progresses {
		if !progresses[i].MinimumWriteTimestamp.IsEmpty() {
			continue
		}
		didScan = true
		i := i // copy for closure
		g.Go(func() (err error) {
			if progresses[i], err = deps.IndexBackfiller().MaybePrepareDestIndexesForBackfill(
				ctx, progresses[i], tables[progresses[i].TableID],
			); err != nil {
				return err
			}
			return tracker.SetBackfillProgress(ctx, progresses[i])
		})
	}
	if err := g.Wait(); err != nil {
		return false, err
	}
	return didScan, nil
}

func loadProgresses(
	ctx context.Context, backfillsToExecute []Backfill, tracker BackfillProgressReader,
) ([]BackfillProgress, error) {
	var progresses []BackfillProgress
	for _, bf := range backfillsToExecute {
		progress, err := tracker.GetBackfillProgress(ctx, bf)
		if err != nil {
			return nil, err
		}
		progresses = append(progresses, progress)
	}
	return progresses, nil
}

func runBackfills(
	ctx context.Context,
	deps Dependencies,
	tracker BackfillTracker,
	progresses []BackfillProgress,
	tables map[descpb.ID]catalog.TableDescriptor,
) error {

	bf := deps.IndexBackfiller()
	g, gCtx := errgroup.WithContext(ctx)
	for i := range progresses {
		p := progresses[i] // copy for closure
		g.Go(func() error {
			return executeBackfill(
				gCtx, deps.IndexSpanSplitter(), bf, p, tracker, tables[p.TableID],
			)
		})
	}

	stop := deps.PeriodicProgressFlusher().StartPeriodicUpdates(ctx, tracker)
	defer func() { _ = stop() }()
	if err := g.Wait(); err != nil {
		return err
	}
	if err := stop(); err != nil {
		return err
	}
	if err := tracker.FlushCheckpoint(ctx); err != nil {
		return err
	}
	return tracker.FlushFractionCompleted(ctx)
}

func executeBackfill(
	ctx context.Context,
	splitter IndexSpanSplitter,
	backfiller Backfiller,
	progress BackfillProgress,
	tracker BackfillProgressWriter,
	table catalog.TableDescriptor,
) error {
	// Split off the index span prior to backfilling.
	// TODO(ajwerner): Consider parallelizing splits.
	// TODO(ajwerner): Consider checkpointing the splits or not doing it if
	// the TODO spans are not the whole set.
	for _, destIndexID := range progress.DestIndexIDs {
		mut, err := scmutationexec.FindMutation(table,
			scmutationexec.MakeIndexIDMutationSelector(destIndexID))
		if err != nil {
			return err
		}

		// Must be the right index given the above call.
		idxToBackfill := mut.AsIndex()
		if err := splitter.MaybeSplitIndexSpans(ctx, table, idxToBackfill); err != nil {
			return err
		}
	}

	return backfiller.BackfillIndex(ctx, progress, tracker, table)
}
