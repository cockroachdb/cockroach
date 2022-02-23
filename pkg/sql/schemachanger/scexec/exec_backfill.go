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
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// TODO(ajwerner): Consider separating out the dependencies for the
// backfill from that of mutations and validation. The fact that there
// is a transaction hanging around in the dependencies is more likely to
// be confusing than valuable. Not much is being done transactionally.

func executeBackfillOps(ctx context.Context, deps Dependencies, execute []scop.Op) (err error) {
	backfillsToExecute := extractBackfillsFromOps(execute)
	tables, err := getTableDescriptorsForBackfills(ctx, deps.Catalog(), backfillsToExecute)
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

func getTableDescriptorsForBackfills(
	ctx context.Context, cat Catalog, backfills []Backfill,
) (_ map[descpb.ID]catalog.TableDescriptor, err error) {
	var descIDs catalog.DescriptorIDSet
	for _, bf := range backfills {
		descIDs.Add(bf.TableID)
	}
	tables := make(map[descpb.ID]catalog.TableDescriptor, descIDs.Len())
	for _, id := range descIDs.Ordered() {
		desc, err := cat.MustReadImmutableDescriptors(ctx, id)
		if err != nil {
			return nil, err
		}
		tbl, ok := desc[0].(catalog.TableDescriptor)
		if !ok {
			return nil, errors.AssertionFailedf("descriptor %d is not a table", id)
		}
		tables[id] = tbl
	}
	return tables, nil
}

func extractBackfillsFromOps(execute []scop.Op) []Backfill {
	var backfillsToExecute []Backfill
	for _, op := range execute {
		switch op := op.(type) {
		case *scop.BackfillIndex:
			backfillsToExecute = append(backfillsToExecute, Backfill{
				TableID:       op.TableID,
				SourceIndexID: op.SourceIndexID,
				DestIndexIDs:  []descpb.IndexID{op.IndexID},
			})
		default:
			panic("unimplemented")
		}
	}
	return mergeBackfillsFromSameSource(backfillsToExecute)
}

// mergeBackfillsFromSameSource will take a slice of backfills which
// potentially have more than one source and will compact them into
// exactly one backfill for each (TableID, SourceIndexID) tuple. The
// DestIndexIDs will be sorted.
func mergeBackfillsFromSameSource(toExecute []Backfill) []Backfill {
	sort.Slice(toExecute, func(i, j int) bool {
		if toExecute[i].TableID == toExecute[j].TableID {
			return toExecute[i].SourceIndexID < toExecute[j].SourceIndexID
		}
		return toExecute[i].TableID < toExecute[j].TableID
	})
	truncated := toExecute[:0]
	sameSource := func(a, b Backfill) bool {
		return a.TableID == b.TableID && a.SourceIndexID == b.SourceIndexID
	}
	for _, bf := range toExecute {
		if len(truncated) == 0 || !sameSource(truncated[len(truncated)-1], bf) {
			truncated = append(truncated, bf)
		} else {
			ord := len(truncated) - 1
			curIDs := truncated[ord].DestIndexIDs
			curIDs = curIDs[:len(curIDs):len(curIDs)]
			truncated[ord].DestIndexIDs = append(curIDs, bf.DestIndexIDs...)
		}
	}
	toExecute = truncated

	// Make sure all the DestIndexIDs are sorted.
	for i := range toExecute {
		dstIDs := toExecute[i].DestIndexIDs
		sort.Slice(dstIDs, func(i, j int) bool { return dstIDs[i] < dstIDs[j] })
	}
	return toExecute
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
	for i := range progresses {
		if didScan = didScan || progresses[i].MinimumWriteTimestamp.IsEmpty(); didScan {
			break
		}
	}
	if !didScan {
		return false, nil
	}
	const op = "scan destination indexes"
	if err := forEachProgressConcurrently(ctx, op, progresses, func(
		ctx context.Context, p *BackfillProgress,
	) error {
		if !p.MinimumWriteTimestamp.IsEmpty() {
			return nil
		}
		updated, err := deps.IndexBackfiller().MaybePrepareDestIndexesForBackfill(
			ctx, *p, tables[p.TableID],
		)
		if err != nil {
			return err
		}
		*p = updated
		return tracker.SetBackfillProgress(ctx, updated)
	}); err != nil {
		return false, err
	}
	return true, nil
}

func forEachProgressConcurrently(
	ctx context.Context,
	op redact.SafeString,
	progresses []BackfillProgress,
	f func(context.Context, *BackfillProgress) error,
) error {
	g := ctxgroup.WithContext(ctx)
	run := func(i int) {
		g.GoCtx(func(ctx context.Context) (err error) {
			defer func() {
				switch r := recover().(type) {
				case nil:
					return
				case error:
					err = errors.Wrapf(r, "failed to %s", op)
				default:
					err = errors.AssertionFailedf("failed to %s: %v", op, r)
				}
			}()
			return f(ctx, &progresses[i])
		})
	}
	for i := range progresses {
		run(i)
	}
	return g.Wait()
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

	stop := deps.PeriodicProgressFlusher().StartPeriodicUpdates(ctx, tracker)
	defer func() { _ = stop() }()
	bf := deps.IndexBackfiller()
	const op = "run backfills"
	if err := forEachProgressConcurrently(ctx, op, progresses, func(
		ctx context.Context, p *BackfillProgress,
	) error {
		return runBackfill(
			ctx, deps.IndexSpanSplitter(), bf, *p, tracker, tables[p.TableID],
		)
	}); err != nil {
		return err
	}
	if err := stop(); err != nil {
		return err
	}
	if err := tracker.FlushFractionCompleted(ctx); err != nil {
		return err
	}
	return tracker.FlushCheckpoint(ctx)
}

func runBackfill(
	ctx context.Context,
	splitter IndexSpanSplitter,
	backfiller Backfiller,
	progress BackfillProgress,
	tracker BackfillProgressWriter,
	table catalog.TableDescriptor,
) error {
	// Split off the index span prior to backfilling.
	// TODO(ajwerner): Consider parallelizing splits.
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
