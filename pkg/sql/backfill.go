// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

const (
	// TODO(vivek): Replace these constants with a runtime budget for the
	// operation chunk involved.

	// columnTruncateAndBackfillChunkSize is the maximum number of columns
	// processed per chunk during column truncate or backfill.
	columnTruncateAndBackfillChunkSize = 200

	// indexTruncateChunkSize is the maximum number of index entries truncated
	// per chunk during an index truncation. This value is larger than the
	// other chunk constants because the operation involves only running a
	// DeleteRange().
	indexTruncateChunkSize = 600

	// indexTxnBackfillChunkSize is the maximum number index entries backfilled
	// per chunk during an index backfill done in a txn. The index backfill
	// involves a table scan, and a number of individual ops presented in a batch.
	// This value is smaller than ColumnTruncateAndBackfillChunkSize, because it
	// involves a number of individual index row updates that can be scattered
	// over many ranges.
	indexTxnBackfillChunkSize = 100

	// checkpointInterval is the interval after which a checkpoint of the
	// schema change is posted.
	checkpointInterval = 1 * time.Minute
)

var indexBulkBackfillChunkSize = settings.RegisterIntSetting(
	"schemachanger.bulk_index_backfill.batch_size",
	"number of rows to process at a time during bulk index backfill",
	5000000,
)

var _ sort.Interface = columnsByID{}
var _ sort.Interface = indexesByID{}

type columnsByID []sqlbase.ColumnDescriptor

func (cds columnsByID) Len() int {
	return len(cds)
}
func (cds columnsByID) Less(i, j int) bool {
	return cds[i].ID < cds[j].ID
}
func (cds columnsByID) Swap(i, j int) {
	cds[i], cds[j] = cds[j], cds[i]
}

type indexesByID []sqlbase.IndexDescriptor

func (ids indexesByID) Len() int {
	return len(ids)
}
func (ids indexesByID) Less(i, j int) bool {
	return ids[i].ID < ids[j].ID
}
func (ids indexesByID) Swap(i, j int) {
	ids[i], ids[j] = ids[j], ids[i]
}

func (sc *SchemaChanger) getChunkSize(chunkSize int64) int64 {
	if sc.testingKnobs.BackfillChunkSize > 0 {
		return sc.testingKnobs.BackfillChunkSize
	}
	return chunkSize
}

// runBackfill runs the backfill for the schema changer.
func (sc *SchemaChanger) runBackfill(
	ctx context.Context,
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	evalCtx *extendedEvalContext,
) error {
	if sc.testingKnobs.RunBeforeBackfill != nil {
		if err := sc.testingKnobs.RunBeforeBackfill(); err != nil {
			return err
		}
	}
	if err := sc.ExtendLease(ctx, lease); err != nil {
		return err
	}

	// Mutations are applied in a FIFO order. Only apply the first set of
	// mutations. Collect the elements that are part of the mutation.
	var droppedIndexDescs []sqlbase.IndexDescriptor
	var addedIndexDescs []sqlbase.IndexDescriptor

	var checksToValidate []sqlbase.ConstraintToValidate

	var tableDesc *sqlbase.TableDescriptor
	if err := sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		tableDesc, err = sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
		return err
	}); err != nil {
		return err
	}
	// Short circuit the backfill if the table has been deleted.
	if tableDesc.Dropped() {
		return nil
	}
	version := tableDesc.Version

	log.Infof(ctx, "Running backfill for %q, v=%d, m=%d",
		tableDesc.Name, tableDesc.Version, sc.mutationID)

	needColumnBackfill := false
	for _, m := range tableDesc.Mutations {
		if m.MutationID != sc.mutationID {
			break
		}
		switch m.Direction {
		case sqlbase.DescriptorMutation_ADD:
			switch t := m.Descriptor_.(type) {
			case *sqlbase.DescriptorMutation_Column:
				if sqlbase.ColumnNeedsBackfill(m.GetColumn()) {
					needColumnBackfill = true
				}
			case *sqlbase.DescriptorMutation_Index:
				addedIndexDescs = append(addedIndexDescs, *t.Index)
			case *sqlbase.DescriptorMutation_Constraint:
				switch t.Constraint.ConstraintType {
				case sqlbase.ConstraintToValidate_CHECK:
					checksToValidate = append(checksToValidate, *t.Constraint)
				default:
					return errors.Errorf("unsupported constraint type: %d", t.Constraint.ConstraintType)
				}
			default:
				return errors.Errorf("unsupported mutation: %+v", m)
			}

		case sqlbase.DescriptorMutation_DROP:
			switch t := m.Descriptor_.(type) {
			case *sqlbase.DescriptorMutation_Column:
				needColumnBackfill = true
			case *sqlbase.DescriptorMutation_Index:
				if !sc.canClearRangeForDrop(t.Index) {
					droppedIndexDescs = append(droppedIndexDescs, *t.Index)
				}
			case *sqlbase.DescriptorMutation_Constraint:
				// no-op
			default:
				return errors.Errorf("unsupported mutation: %+v", m)
			}
		}
	}

	// First drop indexes, then add/drop columns, and only then add indexes.

	// Drop indexes not to be removed by `ClearRange`.
	if len(droppedIndexDescs) > 0 {
		if err := sc.truncateIndexes(ctx, lease, version, droppedIndexDescs); err != nil {
			return err
		}
	}

	// Add and drop columns.
	if needColumnBackfill {
		if err := sc.truncateAndBackfillColumns(ctx, evalCtx, lease, version); err != nil {
			return err
		}
	}

	// Add new indexes.
	if len(addedIndexDescs) > 0 {
		// Check if bulk-adding is enabled and supported by indexes (ie non-unique).
		if err := sc.backfillIndexes(ctx, evalCtx, lease, version); err != nil {
			return err
		}
	}

	// Validate check constraints.
	if len(checksToValidate) > 0 {
		if err := sc.validateChecks(ctx, evalCtx, lease, checksToValidate); err != nil {
			return err
		}
	}
	return nil
}

func (sc *SchemaChanger) validateChecks(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	checks []sqlbase.ConstraintToValidate,
) error {
	if testDisableTableLeases {
		return nil
	}
	readAsOf := sc.clock.Now()
	return sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		txn.SetFixedTimestamp(ctx, readAsOf)
		tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
		if err != nil {
			return err
		}

		if err := sc.ExtendLease(ctx, lease); err != nil {
			return err
		}

		grp := ctxgroup.WithContext(ctx)

		// Notify when validation is finished (or has returned an error) for a check.
		countDone := make(chan struct{}, len(checks))

		for _, c := range checks {
			grp.GoCtx(func(ctx context.Context) error {
				defer func() { countDone <- struct{}{} }()

				// Make the mutations public in a private copy of the descriptor
				// and add it to the TableCollection, so that we can use SQL below to perform
				// the validation. We wouldn't have needed to do this if we could have
				// updated the descriptor and run validation in the same transaction. However,
				// our current system is incapable of running long running schema changes
				// (the validation can take many minutes). So we pretend that the schema
				// has been updated and actually update it in a separate transaction that
				// follows this one.
				desc, err := sqlbase.NewImmutableTableDescriptor(*tableDesc).MakeFirstMutationPublic()
				if err != nil {
					return err
				}
				// Create a new eval context only because the eval context cannot be shared across many
				// goroutines.
				newEvalCtx := createSchemaChangeEvalCtx(ctx, readAsOf, evalCtx.Tracing, sc.ieFactory)
				return validateCheckInTxn(ctx, sc.leaseMgr, &newEvalCtx.EvalContext, desc, txn, &c.Name)
			})
		}

		// Periodic schema change lease extension.
		grp.GoCtx(func(ctx context.Context) error {
			count := len(checks)
			refreshTimer := timeutil.NewTimer()
			defer refreshTimer.Stop()
			refreshTimer.Reset(checkpointInterval)
			for {
				select {
				case <-countDone:
					count--
					if count == 0 {
						// Stop.
						return nil
					}

				case <-refreshTimer.C:
					refreshTimer.Read = true
					refreshTimer.Reset(checkpointInterval)
					if err := sc.ExtendLease(ctx, lease); err != nil {
						return err
					}

				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
		return grp.Wait()
	})
}

func (sc *SchemaChanger) getTableVersion(
	ctx context.Context, txn *client.Txn, tc *TableCollection, version sqlbase.DescriptorVersion,
) (*sqlbase.ImmutableTableDescriptor, error) {
	tableDesc, err := tc.getTableVersionByID(ctx, txn, sc.tableID, ObjectLookupFlags{})
	if err != nil {
		return nil, err
	}
	if version != tableDesc.Version {
		return nil, makeErrTableVersionMismatch(tableDesc.Version, version)
	}
	return tableDesc, nil
}

func (sc *SchemaChanger) truncateIndexes(
	ctx context.Context,
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	version sqlbase.DescriptorVersion,
	dropped []sqlbase.IndexDescriptor,
) error {
	chunkSize := sc.getChunkSize(indexTruncateChunkSize)
	if sc.testingKnobs.BackfillChunkSize > 0 {
		chunkSize = sc.testingKnobs.BackfillChunkSize
	}
	alloc := &sqlbase.DatumAlloc{}
	for _, desc := range dropped {
		var resume roachpb.Span
		for rowIdx, done := int64(0), false; !done; rowIdx += chunkSize {
			// First extend the schema change lease.
			if err := sc.ExtendLease(ctx, lease); err != nil {
				return err
			}

			resumeAt := resume
			if log.V(2) {
				log.Infof(ctx, "drop index (%d, %d) at row: %d, span: %s",
					sc.tableID, sc.mutationID, rowIdx, resume)
			}
			if err := sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				if fn := sc.execCfg.DistSQLRunTestingKnobs.RunBeforeBackfillChunk; fn != nil {
					if err := fn(resume); err != nil {
						return err
					}
				}
				if fn := sc.execCfg.DistSQLRunTestingKnobs.RunAfterBackfillChunk; fn != nil {
					defer fn()
				}

				tc := &TableCollection{leaseMgr: sc.leaseMgr}
				defer tc.releaseTables(ctx)
				tableDesc, err := sc.getTableVersion(ctx, txn, tc, version)
				if err != nil {
					return err
				}

				rd, err := row.MakeDeleter(
					txn, tableDesc, nil, nil, row.SkipFKs, nil /* *tree.EvalContext */, alloc,
				)
				if err != nil {
					return err
				}
				td := tableDeleter{rd: rd, alloc: alloc}
				if err := td.init(txn, nil /* *tree.EvalContext */); err != nil {
					return err
				}
				if !sc.canClearRangeForDrop(&desc) {
					resume, err = td.deleteIndex(
						ctx,
						&desc,
						resumeAt,
						chunkSize,
						false, /* traceKV */
					)
					done = resume.Key == nil
					return err
				}
				done = true
				return td.clearIndex(ctx, &desc)
			}); err != nil {
				return err
			}
		}
		if err := sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			return removeIndexZoneConfigs(ctx, txn, sc.execCfg, sc.tableID, dropped)
		}); err != nil {
			return err
		}
	}
	return nil
}

type backfillType int

const (
	_ backfillType = iota
	columnBackfill
	indexBackfill
)

// getJobIDForMutationWithDescriptor returns a job id associated with a mutation given
// a table descriptor. Unlike getJobIDForMutation this doesn't need transaction.
func getJobIDForMutationWithDescriptor(
	ctx context.Context, tableDesc *sqlbase.TableDescriptor, mutationID sqlbase.MutationID,
) (int64, error) {
	for _, job := range tableDesc.MutationJobs {
		if job.MutationID == mutationID {
			return job.JobID, nil
		}
	}

	return 0, errors.Errorf("job not found for table id %d, mutation %d", tableDesc.ID, mutationID)
}

// nRanges returns the number of ranges that cover a set of spans.
func (sc *SchemaChanger) nRanges(
	ctx context.Context, txn *client.Txn, spans []roachpb.Span,
) (int, error) {
	spanResolver := sc.distSQLPlanner.spanResolver.NewSpanResolverIterator(txn)
	rangeIds := make(map[int64]struct{})
	for _, span := range spans {
		// For each span, iterate the spanResolver until it's exhausted, storing
		// the found range ids in the map to de-duplicate them.
		spanResolver.Seek(ctx, span, kv.Ascending)
		for {
			if !spanResolver.Valid() {
				return 0, spanResolver.Error()
			}
			rangeIds[int64(spanResolver.Desc().RangeID)] = struct{}{}
			if !spanResolver.NeedAnother() {
				break
			}
			spanResolver.Next(ctx)
		}
	}

	return len(rangeIds), nil
}

// distBackfill runs (or continues) a backfill for the first mutation
// enqueued on the SchemaChanger's table descriptor that passes the input
// MutationFilter.
func (sc *SchemaChanger) distBackfill(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	version sqlbase.DescriptorVersion,
	backfillType backfillType,
	backfillChunkSize int64,
	filter backfill.MutationFilter,
) error {
	duration := checkpointInterval
	if sc.testingKnobs.WriteCheckpointInterval > 0 {
		duration = sc.testingKnobs.WriteCheckpointInterval
	}
	chunkSize := sc.getChunkSize(backfillChunkSize)

	origNRanges := -1
	origFractionCompleted := sc.job.FractionCompleted()
	fractionLeft := 1 - origFractionCompleted
	readAsOf := sc.clock.Now()
	for {
		var spans []roachpb.Span
		if err := sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			var err error
			spans, _, _, err = distsqlrun.GetResumeSpans(
				ctx, sc.jobRegistry, txn, sc.tableID, sc.mutationID, filter)
			return err
		}); err != nil {
			return err
		}

		if len(spans) <= 0 {
			break
		}

		if err := sc.ExtendLease(ctx, lease); err != nil {
			return err
		}
		log.VEventf(ctx, 2, "backfill: process %+v spans", spans)
		if err := sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			// Report schema change progress. We define progress at this point
			// as the the fraction of fully-backfilled ranges of the primary index of
			// the table being scanned. Since we may have already modified the
			// fraction completed of our job from the 10% allocated to completing the
			// schema change state machine or from a previous backfill attempt,
			// we scale that fraction of ranges completed by the remaining fraction
			// of the job's progress bar.
			nRanges, err := sc.nRanges(ctx, txn, spans)
			if err != nil {
				return err
			}
			if origNRanges == -1 {
				origNRanges = nRanges
			}

			if nRanges < origNRanges {
				fractionRangesFinished := float32(origNRanges-nRanges) / float32(origNRanges)
				fractionCompleted := origFractionCompleted + fractionLeft*fractionRangesFinished
				if err := sc.job.FractionProgressed(ctx, jobs.FractionUpdater(fractionCompleted)); err != nil {
					return jobs.SimplifyInvalidStatusError(err)
				}
			}

			tc := &TableCollection{leaseMgr: sc.leaseMgr}
			// Use a leased table descriptor for the backfill.
			defer tc.releaseTables(ctx)
			tableDesc, err := sc.getTableVersion(ctx, txn, tc, version)
			if err != nil {
				return err
			}
			// otherTableDescs contains any other table descriptors required by the
			// backfiller processor.
			var otherTableDescs []sqlbase.TableDescriptor
			if backfillType == columnBackfill {
				fkTables, err := row.MakeFkMetadata(
					ctx,
					tableDesc,
					row.CheckUpdates,
					row.NoLookup,
					row.NoCheckPrivilege,
					nil, /* AnalyzeExprFunction */
					nil, /* CheckHelper */
				)
				if err != nil {
					return err
				}

				for k := range fkTables {
					table, err := tc.getTableVersionByID(ctx, txn, k, ObjectLookupFlags{})
					if err != nil {
						return err
					}
					otherTableDescs = append(otherTableDescs, *table.TableDesc())
				}
			}
			rw := &errOnlyResultWriter{}
			recv := MakeDistSQLReceiver(
				ctx,
				rw,
				tree.Rows, /* stmtType - doesn't matter here since no result are produced */
				sc.rangeDescriptorCache,
				sc.leaseHolderCache,
				nil, /* txn - the flow does not run wholly in a txn */
				func(ts hlc.Timestamp) {
					_ = sc.clock.Update(ts)
				},
				evalCtx.Tracing,
			)
			defer recv.Release()
			planCtx := sc.distSQLPlanner.NewPlanningCtx(ctx, evalCtx, txn)
			plan, err := sc.distSQLPlanner.createBackfiller(
				planCtx, backfillType, *tableDesc.TableDesc(), duration, chunkSize, spans, otherTableDescs, readAsOf,
			)
			if err != nil {
				return err
			}
			sc.distSQLPlanner.Run(
				planCtx,
				nil, /* txn - the processors manage their own transactions */
				&plan, recv, evalCtx,
				nil, /* finishedSetupFn */
			)
			return rw.Err()
		}); err != nil {
			return err
		}
	}
	return nil
}

// validate the new indexes being added
func (sc *SchemaChanger) validateIndexes(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
) error {
	if testDisableTableLeases {
		return nil
	}
	readAsOf := sc.clock.Now()
	return sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		txn.SetFixedTimestamp(ctx, readAsOf)
		tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
		if err != nil {
			return err
		}

		if err := sc.ExtendLease(ctx, lease); err != nil {
			return err
		}

		var forwardIndexes []*sqlbase.IndexDescriptor
		var invertedIndexes []*sqlbase.IndexDescriptor

		for _, m := range tableDesc.Mutations {
			if sc.mutationID != m.MutationID {
				break
			}
			idx := m.GetIndex()
			if idx == nil || m.Direction == sqlbase.DescriptorMutation_DROP {
				continue
			}
			switch idx.Type {
			case sqlbase.IndexDescriptor_FORWARD:
				forwardIndexes = append(forwardIndexes, idx)
			case sqlbase.IndexDescriptor_INVERTED:
				invertedIndexes = append(invertedIndexes, idx)
			}
		}
		if len(forwardIndexes) == 0 && len(invertedIndexes) == 0 {
			return nil
		}

		grp := ctxgroup.WithContext(ctx)

		forwardIndexesDone := make(chan struct{})
		invertedIndexesDone := make(chan struct{})

		grp.GoCtx(func(ctx context.Context) error {
			defer close(forwardIndexesDone)
			if len(forwardIndexes) > 0 {
				return sc.validateForwardIndexes(ctx, evalCtx, txn, tableDesc, readAsOf, forwardIndexes)
			}
			return nil
		})

		grp.GoCtx(func(ctx context.Context) error {
			defer close(invertedIndexesDone)
			if len(invertedIndexes) > 0 {
				return sc.validateInvertedIndexes(ctx, evalCtx, txn, tableDesc, readAsOf, invertedIndexes)
			}
			return nil
		})

		// Periodic schema change lease extension.
		grp.GoCtx(func(ctx context.Context) error {
			forwardDone := false
			invertedDone := false

			refreshTimer := timeutil.NewTimer()
			defer refreshTimer.Stop()
			refreshTimer.Reset(checkpointInterval)
			for {
				if forwardDone && invertedDone {
					return nil
				}
				select {
				case <-forwardIndexesDone:
					forwardDone = true

				case <-invertedIndexesDone:
					invertedDone = true

				case <-refreshTimer.C:
					refreshTimer.Read = true
					refreshTimer.Reset(checkpointInterval)
					if err := sc.ExtendLease(ctx, lease); err != nil {
						return err
					}

				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
		return grp.Wait()
	})
}

func (sc *SchemaChanger) validateInvertedIndexes(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	txn *client.Txn,
	tableDesc *TableDescriptor,
	readAsOf hlc.Timestamp,
	indexes []*sqlbase.IndexDescriptor,
) error {
	grp := ctxgroup.WithContext(ctx)

	expectedCount := make([]int64, len(indexes))
	countReady := make([]chan struct{}, len(indexes))

	for i, idx := range indexes {
		i, idx := i, idx
		countReady[i] = make(chan struct{})

		grp.GoCtx(func(ctx context.Context) error {
			// Inverted indexes currently can't be interleaved, so a KV scan can be
			// used to get the index length.
			// TODO (lucy): Switch to using DistSQL to get the count, so that we get
			// distributed execution and avoid bypassing the SQL decoding
			start := timeutil.Now()
			var idxLen int64
			key := tableDesc.IndexSpan(idx.ID).Key
			endKey := tableDesc.IndexSpan(idx.ID).EndKey
			for {
				kvs, err := txn.Scan(ctx, key, endKey, 1000000)
				if err != nil {
					return err
				}
				if len(kvs) == 0 {
					break
				}
				idxLen += int64(len(kvs))
				key = kvs[len(kvs)-1].Key.PrefixEnd()
			}
			log.Infof(ctx, "inverted index %s/%s count = %d, took %s",
				tableDesc.Name, idx.Name, idxLen, timeutil.Since(start))
			select {
			case <-countReady[i]:
				if idxLen != expectedCount[i] {
					// JSON columns cannot have unique indexes, so if the expected and
					// actual counts do not match, it's always a bug rather than a
					// uniqueness violation.
					return errors.Errorf("validation of index %s failed: expected %d rows, found %d",
						idx.Name, expectedCount[i], idxLen)
				}
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})

		grp.GoCtx(func(ctx context.Context) error {
			defer close(countReady[i])

			start := timeutil.Now()
			if len(idx.ColumnNames) != 1 {
				panic(fmt.Sprintf("expected inverted index %s to have exactly 1 column, but found columns %+v",
					idx.Name, idx.ColumnNames))
			}
			col := idx.ColumnNames[0]
			row, err := evalCtx.InternalExecutor.QueryRow(ctx, "verify-inverted-idx-count", txn,
				fmt.Sprintf(
					`SELECT coalesce(sum_int(crdb_internal.json_num_index_entries(%s)), 0) FROM [%d AS t]`,
					col, tableDesc.ID,
				),
			)
			if err != nil {
				return err
			}
			expectedCount[i] = int64(tree.MustBeDInt(row[0]))
			log.Infof(ctx, "JSON column %s/%s expected inverted index count = %d, took %s",
				tableDesc.Name, col, expectedCount[i], timeutil.Since(start))
			return nil
		})
	}

	return grp.Wait()
}

func (sc *SchemaChanger) validateForwardIndexes(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	txn *client.Txn,
	tableDesc *TableDescriptor,
	readAsOf hlc.Timestamp,
	indexes []*sqlbase.IndexDescriptor,
) error {
	grp := ctxgroup.WithContext(ctx)

	var tableRowCount int64
	// Close when table count is ready.
	tableCountReady := make(chan struct{})
	// Compute the size of each index.
	for _, idx := range indexes {
		idx := idx
		grp.GoCtx(func(ctx context.Context) error {
			start := timeutil.Now()
			// Make the mutations public in a private copy of the descriptor
			// and add it to the TableCollection, so that we can use SQL below to perform
			// the validation. We wouldn't have needed to do this if we could have
			// updated the descriptor and run validation in the same transaction. However,
			// our current system is incapable of running long running schema changes
			// (the validation can take many minutes). So we pretend that the schema
			// has been updated and actually update it in a separate transaction that
			// follows this one.
			desc, err := sqlbase.NewImmutableTableDescriptor(*tableDesc).MakeFirstMutationPublic()
			if err != nil {
				return err
			}
			tc := &TableCollection{leaseMgr: sc.leaseMgr}
			// pretend that the schema has been modified.
			if err := tc.addUncommittedTable(*desc); err != nil {
				return err
			}

			// Create a new eval context only because the eval context cannot be shared across many
			// goroutines.
			newEvalCtx := createSchemaChangeEvalCtx(ctx, readAsOf, evalCtx.Tracing, sc.ieFactory)
			// TODO(vivek): This is not a great API. Leaving #34304 open.
			ie := newEvalCtx.InternalExecutor.(*SessionBoundInternalExecutor)
			ie.impl.tcModifier = tc
			defer func() {
				ie.impl.tcModifier = nil
			}()

			row, err := newEvalCtx.InternalExecutor.QueryRow(ctx, "verify-idx-count", txn,
				fmt.Sprintf(`SELECT count(*) FROM [%d AS t]@[%d]`, tableDesc.ID, idx.ID))
			if err != nil {
				return err
			}
			idxLen := int64(tree.MustBeDInt(row[0]))

			log.Infof(ctx, "index %s/%s row count = %d, took %s",
				tableDesc.Name, idx.Name, idxLen, timeutil.Since(start))

			select {
			case <-tableCountReady:
				if idxLen != tableRowCount {
					// TODO(vivek): find the offending row and include it in the error.
					return pgerror.NewErrorf(
						pgerror.CodeUniqueViolationError,
						"%d entries, expected %d violates unique constraint %q",
						idxLen, tableRowCount, idx.Name,
					)
				}

			case <-ctx.Done():
				return ctx.Err()
			}

			return nil
		})
	}

	grp.GoCtx(func(ctx context.Context) error {
		defer close(tableCountReady)
		var tableRowCountTime time.Duration
		start := timeutil.Now()
		// Count the number of rows in the table.
		cnt, err := evalCtx.InternalExecutor.QueryRow(ctx, "VERIFY INDEX", txn,
			fmt.Sprintf(`SELECT count(1) FROM [%d AS t]`, tableDesc.ID))
		if err != nil {
			return err
		}
		tableRowCount = int64(tree.MustBeDInt(cnt[0]))
		tableRowCountTime = timeutil.Since(start)
		log.Infof(ctx, "table %s row count = %d, took %s",
			tableDesc.Name, tableRowCount, tableRowCountTime)
		return nil
	})

	return grp.Wait()
}

func (sc *SchemaChanger) backfillIndexes(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	version sqlbase.DescriptorVersion,
) error {
	if fn := sc.testingKnobs.RunBeforeIndexBackfill; fn != nil {
		fn()
	}

	chunkSize := int64(indexTxnBackfillChunkSize)
	bulk := backfill.BulkWriteIndex.Get(&sc.settings.SV)
	if bulk {
		chunkSize = indexBulkBackfillChunkSize.Get(&sc.settings.SV)
	}

	if err := sc.distBackfill(
		ctx, evalCtx, lease, version, indexBackfill, chunkSize,
		backfill.IndexMutationFilter); err != nil {
		return err
	}
	return sc.validateIndexes(ctx, evalCtx, lease)
}

func (sc *SchemaChanger) truncateAndBackfillColumns(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	version sqlbase.DescriptorVersion,
) error {
	return sc.distBackfill(
		ctx, evalCtx,
		lease, version, columnBackfill, columnTruncateAndBackfillChunkSize,
		backfill.ColumnMutationFilter)
}

// runSchemaChangesInTxn runs all the schema changes immediately in a
// transaction. This is called when a CREATE TABLE is followed by
// schema changes in the same transaction. The CREATE TABLE is
// invisible to the rest of the cluster, so the schema changes
// can be executed immediately on the same version of the table.
func runSchemaChangesInTxn(
	ctx context.Context,
	txn *client.Txn,
	tc *TableCollection,
	execCfg *ExecutorConfig,
	evalCtx *tree.EvalContext,
	tableDesc *sqlbase.MutableTableDescriptor,
	traceKV bool,
) error {
	if len(tableDesc.DrainingNames) > 0 {
		// Reclaim all the old names. Leave the data and descriptor
		// cleanup for later.
		b := txn.NewBatch()
		for _, drain := range tableDesc.DrainingNames {
			tbKey := tableKey{drain.ParentID, drain.Name}.Key()
			b.Del(tbKey)
		}
		tableDesc.DrainingNames = nil
		if err := txn.Run(ctx, b); err != nil {
			return err
		}
	}

	if tableDesc.Dropped() {
		return nil
	}

	// Only needed because columnBackfillInTxn() backfills
	// all column mutations.
	doneColumnBackfill := false
	// Checks are validated after all other mutations have been applied.
	var checksToValidate []sqlbase.ConstraintToValidate

	for _, m := range tableDesc.Mutations {
		immutDesc := sqlbase.NewImmutableTableDescriptor(*tableDesc.TableDesc())
		switch m.Direction {
		case sqlbase.DescriptorMutation_ADD:
			switch t := m.Descriptor_.(type) {
			case *sqlbase.DescriptorMutation_Column:
				if doneColumnBackfill || !sqlbase.ColumnNeedsBackfill(m.GetColumn()) {
					break
				}
				if err := columnBackfillInTxn(ctx, txn, tc, evalCtx, immutDesc, traceKV); err != nil {
					return err
				}
				doneColumnBackfill = true

			case *sqlbase.DescriptorMutation_Index:
				if err := indexBackfillInTxn(ctx, txn, immutDesc, traceKV); err != nil {
					return err
				}

			case *sqlbase.DescriptorMutation_Constraint:
				switch t.Constraint.ConstraintType {
				case sqlbase.ConstraintToValidate_CHECK:
					checksToValidate = append(checksToValidate, *t.Constraint)
				default:
					return errors.Errorf("unsupported constraint type: %d", t.Constraint.ConstraintType)
				}

			default:
				return errors.Errorf("unsupported mutation: %+v", m)
			}

		case sqlbase.DescriptorMutation_DROP:
			// Drop the name and drop the associated data later.
			switch m.Descriptor_.(type) {
			case *sqlbase.DescriptorMutation_Column:
				if doneColumnBackfill {
					break
				}
				if err := columnBackfillInTxn(ctx, txn, tc, evalCtx, immutDesc, traceKV); err != nil {
					return err
				}
				doneColumnBackfill = true

			case *sqlbase.DescriptorMutation_Index:
				if err := indexTruncateInTxn(ctx, txn, execCfg, immutDesc, traceKV); err != nil {
					return err
				}

			case *sqlbase.DescriptorMutation_Constraint:
				return errors.Errorf("constraint validation mutation cannot be in the DROP state within the same transaction: %+v", m)

			default:
				return errors.Errorf("unsupported mutation: %+v", m)
			}

		}
		if err := tableDesc.MakeMutationComplete(m); err != nil {
			return err
		}
	}
	tableDesc.Mutations = nil

	// Now that the table descriptor is in a valid state with all column and index
	// mutations applied, it can be used for validating check constraints
	for _, c := range checksToValidate {
		if err := validateCheckInTxn(ctx, tc.leaseMgr, evalCtx, tableDesc, txn, &c.Name); err != nil {
			return err
		}
	}
	return nil
}

// validateCheckInTxn validates check constraints within the provided
// transaction. The table descriptor that is passed in will be used for the
// InternalExecutor that performs the validation query.
func validateCheckInTxn(
	ctx context.Context,
	leaseMgr *LeaseManager,
	evalCtx *tree.EvalContext,
	tableDesc *MutableTableDescriptor,
	txn *client.Txn,
	checkName *string,
) error {
	newTc := &TableCollection{leaseMgr: leaseMgr}
	// pretend that the schema has been modified.
	if err := newTc.addUncommittedTable(*tableDesc); err != nil {
		return err
	}

	ie := evalCtx.InternalExecutor.(*SessionBoundInternalExecutor)
	ie.impl.tcModifier = newTc
	defer func() {
		ie.impl.tcModifier = nil
	}()

	check, err := tableDesc.FindCheckByName(*checkName)
	if err != nil {
		return err
	}
	return validateCheckExpr(ctx, check.Expr, tableDesc.TableDesc(), ie, txn)
}

// columnBackfillInTxn backfills columns for all mutation columns in
// the mutation list.
func columnBackfillInTxn(
	ctx context.Context,
	txn *client.Txn,
	tc *TableCollection,
	evalCtx *tree.EvalContext,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	traceKV bool,
) error {
	// A column backfill in the ADD state is a noop.
	if tableDesc.Adding() {
		return nil
	}
	var backfiller backfill.ColumnBackfiller
	if err := backfiller.Init(evalCtx, tableDesc); err != nil {
		return err
	}
	// otherTableDescs contains any other table descriptors required by the
	// backfiller processor.
	var otherTableDescs []*sqlbase.ImmutableTableDescriptor
	fkTables, err := row.MakeFkMetadata(
		ctx,
		tableDesc,
		row.CheckUpdates,
		row.NoLookup,
		row.NoCheckPrivilege,
		nil, /* AnalyzeExprFunction */
		nil, /* CheckHelper */
	)
	if err != nil {
		return err
	}
	// All the FKs here are guaranteed to be created in the same transaction
	// or else this table would be created in the ADD state.
	for k := range fkTables {
		t := tc.getUncommittedTableByID(k)
		if (uncommittedTable{}) == t || !t.IsNewTable() {
			return errors.Errorf(
				"table %s not created in the same transaction as id = %d", tableDesc.Name, k)
		}
		otherTableDescs = append(otherTableDescs, t.ImmutableTableDescriptor)
	}
	sp := tableDesc.PrimaryIndexSpan()
	for sp.Key != nil {
		var err error
		sp.Key, err = backfiller.RunColumnBackfillChunk(ctx,
			txn, tableDesc, otherTableDescs, sp, columnTruncateAndBackfillChunkSize,
			false /*alsoCommit*/, traceKV)
		if err != nil {
			return err
		}
	}
	return nil
}

func indexBackfillInTxn(
	ctx context.Context, txn *client.Txn, tableDesc *sqlbase.ImmutableTableDescriptor, traceKV bool,
) error {
	var backfiller backfill.IndexBackfiller
	if err := backfiller.Init(tableDesc); err != nil {
		return err
	}
	sp := tableDesc.PrimaryIndexSpan()
	for sp.Key != nil {
		var err error
		sp.Key, err = backfiller.RunIndexBackfillChunk(ctx,
			txn, tableDesc, sp, indexTxnBackfillChunkSize, false /* alsoCommit */, traceKV)
		if err != nil {
			return err
		}
	}
	return nil
}

func indexTruncateInTxn(
	ctx context.Context,
	txn *client.Txn,
	execCfg *ExecutorConfig,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	traceKV bool,
) error {
	alloc := &sqlbase.DatumAlloc{}
	idx := tableDesc.Mutations[0].GetIndex()
	var sp roachpb.Span
	for done := false; !done; done = sp.Key == nil {
		rd, err := row.MakeDeleter(
			txn, tableDesc, nil, nil, row.SkipFKs, nil /* *tree.EvalContext */, alloc,
		)
		if err != nil {
			return err
		}
		td := tableDeleter{rd: rd, alloc: alloc}
		if err := td.init(txn, nil /* *tree.EvalContext */); err != nil {
			return err
		}
		sp, err = td.deleteIndex(
			ctx, idx, sp, indexTruncateChunkSize, traceKV,
		)
		if err != nil {
			return err
		}
	}
	// Remove index zone configs.
	return removeIndexZoneConfigs(ctx, txn, execCfg, tableDesc.ID, []sqlbase.IndexDescriptor{*idx})
}
