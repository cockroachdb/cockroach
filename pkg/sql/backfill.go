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

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

	// indexBackfillChunkSize is the maximum number index entries backfilled
	// per chunk during an index backfill. The index backfill involves a table
	// scan, and a number of individual ops presented in a batch. This value
	// is smaller than ColumnTruncateAndBackfillChunkSize, because it involves
	// a number of individual index row updates that can be scattered over
	// many ranges.
	indexBackfillChunkSize = 100

	// checkpointInterval is the interval after which a checkpoint of the
	// schema change is posted.
	checkpointInterval = 10 * time.Second
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
	// Indexes within the Mutations slice for checkpointing.
	mutationSentinel := -1
	var droppedIndexMutationIdx int

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

	log.VEventf(ctx, 0, "Running backfill for %q, v=%d, m=%d",
		tableDesc.Name, tableDesc.Version, sc.mutationID)

	needColumnBackfill := false
	for i, m := range tableDesc.Mutations {
		if m.MutationID != sc.mutationID {
			break
		}
		switch m.Direction {
		case sqlbase.DescriptorMutation_ADD:
			switch t := m.Descriptor_.(type) {
			case *sqlbase.DescriptorMutation_Column:
				desc := m.GetColumn()
				if desc.DefaultExpr != nil || !desc.Nullable {
					needColumnBackfill = true
				}
			case *sqlbase.DescriptorMutation_Index:
				addedIndexDescs = append(addedIndexDescs, *t.Index)
			default:
				return errors.Errorf("unsupported mutation: %+v", m)
			}

		case sqlbase.DescriptorMutation_DROP:
			switch t := m.Descriptor_.(type) {
			case *sqlbase.DescriptorMutation_Column:
				needColumnBackfill = true
			case *sqlbase.DescriptorMutation_Index:
				droppedIndexDescs = append(droppedIndexDescs, *t.Index)
				if droppedIndexMutationIdx == mutationSentinel {
					droppedIndexMutationIdx = i
				}
			default:
				return errors.Errorf("unsupported mutation: %+v", m)
			}
		}
	}

	// First drop indexes, then add/drop columns, and only then add indexes.

	// Drop indexes.
	if err := sc.truncateIndexes(
		ctx, lease, version, droppedIndexDescs, droppedIndexMutationIdx,
	); err != nil {
		return err
	}

	// Remove index zone configs.
	if err := sc.removeIndexZoneConfigs(ctx, tableDesc.ID, droppedIndexDescs); err != nil {
		return err
	}

	// Add and drop columns.
	if needColumnBackfill {
		if err := sc.truncateAndBackfillColumns(ctx, evalCtx, lease, version); err != nil {
			return err
		}
	}

	// Add new indexes.
	if len(addedIndexDescs) > 0 {
		if err := sc.backfillIndexes(ctx, evalCtx, lease, version); err != nil {
			return err
		}
	}

	return nil
}

func (sc *SchemaChanger) getTableVersion(
	ctx context.Context, txn *client.Txn, tc *TableCollection, version sqlbase.DescriptorVersion,
) (*sqlbase.TableDescriptor, error) {
	tableDesc, err := tc.getTableVersionByID(ctx, txn, sc.tableID)
	if err != nil {
		return nil, err
	}
	if version != tableDesc.Version {
		return nil, errors.Errorf("table version mismatch: %d, expected=%d", tableDesc.Version, version)
	}
	return tableDesc, nil
}

func (sc *SchemaChanger) removeIndexZoneConfigs(
	ctx context.Context, tableID sqlbase.ID, indexDescs []sqlbase.IndexDescriptor,
) error {
	if len(indexDescs) == 0 {
		return nil
	}

	return sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
		if err != nil {
			return err
		}

		zone, err := getZoneConfigRaw(ctx, txn, sc.tableID)
		if err != nil {
			return err
		}

		for _, indexDesc := range indexDescs {
			zone.DeleteIndexSubzones(uint32(indexDesc.ID))
		}

		hasNewSubzones := false
		_, err = writeZoneConfig(ctx, txn, sc.tableID, tableDesc, zone, sc.execCfg, hasNewSubzones)
		if sqlbase.IsCCLRequiredError(err) {
			return sqlbase.NewCCLRequiredError(fmt.Errorf("schema change requires a CCL binary "+
				"because table %q has at least one remaining index or partition with a zone config",
				tableDesc.Name))
		}
		return err
	})
}

func (sc *SchemaChanger) truncateIndexes(
	ctx context.Context,
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	version sqlbase.DescriptorVersion,
	dropped []sqlbase.IndexDescriptor,
	mutationIdx int,
) error {
	chunkSize := sc.getChunkSize(indexTruncateChunkSize)
	if sc.testingKnobs.BackfillChunkSize > 0 {
		chunkSize = sc.testingKnobs.BackfillChunkSize
	}
	alloc := &sqlbase.DatumAlloc{}
	for _, desc := range dropped {
		var resume roachpb.Span
		for row, done := int64(0), false; !done; row += chunkSize {
			// First extend the schema change lease.
			if err := sc.ExtendLease(ctx, lease); err != nil {
				return err
			}

			resumeAt := resume
			if log.V(2) {
				log.Infof(ctx, "drop index (%d, %d) at row: %d, span: %s",
					sc.tableID, sc.mutationID, row, resume)
			}
			if err := sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				if sc.testingKnobs.RunBeforeBackfillChunk != nil {
					if err := sc.testingKnobs.RunBeforeBackfillChunk(resume); err != nil {
						return err
					}
				}
				if sc.testingKnobs.RunAfterBackfillChunk != nil {
					defer sc.testingKnobs.RunAfterBackfillChunk()
				}

				tc := &TableCollection{leaseMgr: sc.leaseMgr}
				defer func() {
					if err := tc.releaseTables(ctx, dontBlockForDBCacheUpdate); err != nil {
						log.Warningf(ctx, "error releasing tables: %s", err)
					}
				}()
				tableDesc, err := sc.getTableVersion(ctx, txn, tc, version)
				if err != nil {
					return err
				}

				rd, err := sqlbase.MakeRowDeleter(
					txn, tableDesc, nil, nil, sqlbase.SkipFKs, nil /* *tree.EvalContext */, alloc,
				)
				if err != nil {
					return err
				}
				td := tableDeleter{rd: rd, alloc: alloc}
				if err := td.init(txn, nil /* *tree.EvalContext */); err != nil {
					return err
				}
				resume, err = td.deleteIndex(
					ctx, &desc, resumeAt, chunkSize, noAutoCommit, false, /* traceKV */
				)
				done = resume.Key == nil
				return err
			}); err != nil {
				return err
			}
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

// getMutationToBackfill returns the the first mutation enqueued on the table
// descriptor that passes the input mutationFilter. It also returns the index
// of that mutation in the table descriptor mutation list.
//
// Returns nil if the backfill is complete.
func (sc *SchemaChanger) getMutationToBackfill(
	ctx context.Context,
	version sqlbase.DescriptorVersion,
	backfillType backfillType,
	filter distsqlrun.MutationFilter,
) (*sqlbase.DescriptorMutation, int, error) {
	var mutation *sqlbase.DescriptorMutation
	var mutationIdx int
	err := sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		mutation = nil
		tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
		if err != nil {
			return err
		}
		if tableDesc.Version != version {
			return errors.Errorf("table version mismatch: %d, expected: %d", tableDesc.Version, version)
		}
		if len(tableDesc.Mutations) > 0 {
			mutationID := tableDesc.Mutations[0].MutationID
			for i := range tableDesc.Mutations {
				if tableDesc.Mutations[i].MutationID != mutationID {
					break
				}
				if filter(tableDesc.Mutations[i]) {
					mutation = &tableDesc.Mutations[i]
					mutationIdx = i
					break
				}
			}
		}
		return nil
	})
	return mutation, mutationIdx, err
}

// getJobIDForMutation returns the jobID associated with a mutationId.
func (sc *SchemaChanger) getJobIDForMutation(
	ctx context.Context, version sqlbase.DescriptorVersion, mutationID sqlbase.MutationID,
) (int64, error) {
	var jobID int64
	err := sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {

		tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
		if err != nil {
			return err
		}
		if tableDesc.Version != version {
			return errors.Errorf("table version mismatch: %d, expected: %d", tableDesc.Version, version)
		}

		if len(tableDesc.MutationJobs) > 0 {
			for _, job := range tableDesc.MutationJobs {
				if job.MutationID == mutationID {
					jobID = job.JobID
					break
				}
			}
		}
		return nil
	})
	return jobID, err
}

// getJobIDForMutationWithDescriptor returns a job id associated with a mutation given
// a table descriptor. Unlike getJobIDForMutation this doesn't need transaction.
func (sc *SchemaChanger) getJobIDForMutationWithDescriptor(
	ctx context.Context, tableDesc *sqlbase.TableDescriptor, mutationID sqlbase.MutationID,
) (int64, error) {
	if len(tableDesc.MutationJobs) > 0 {
		for _, job := range tableDesc.MutationJobs {
			if job.MutationID == mutationID {
				return job.JobID, nil
			}
		}
	}

	return 0, errors.Errorf("mutation id not found %v", mutationID)
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
	filter distsqlrun.MutationFilter,
) error {
	duration := checkpointInterval
	if sc.testingKnobs.WriteCheckpointInterval > 0 {
		duration = sc.testingKnobs.WriteCheckpointInterval
	}
	chunkSize := sc.getChunkSize(backfillChunkSize)

	origNRanges := -1
	origFractionCompleted := sc.job.Payload().FractionCompleted
	fractionLeft := 1 - origFractionCompleted
	for {
		// Repeat until getMutationToBackfill returns a mutation with no remaining
		// ResumeSpans, indicating that the backfill is complete.
		mutation, mutationIdx, err := sc.getMutationToBackfill(ctx, version, backfillType, filter)
		if err != nil {
			return err
		}
		jobID, err := sc.getJobIDForMutation(ctx, version, mutation.MutationID)

		var tableDesc *sqlbase.TableDescriptor
		err = sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			tableDesc, err = sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
			return err
		})
		if err != nil {
			return err
		}
		resumeSpanIndex := distsqlrun.GetResumeSpanIndexofMutationID(tableDesc, mutationIdx)
		spans, err := distsqlrun.GetResumeSpansFromJob(ctx, sc.jobRegistry, nil, jobID, resumeSpanIndex)
		if err != nil {
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
				if err := sc.job.Progressed(ctx, jobs.FractionUpdater(fractionCompleted)); err != nil {
					log.Infof(ctx, "Ignoring error reporting progress %f for job %d: %v", fractionCompleted, *sc.job.ID(), err)
				}
			}

			tc := &TableCollection{leaseMgr: sc.leaseMgr}
			// Use a leased table descriptor for the backfill.
			defer func() {
				if err := tc.releaseTables(ctx, dontBlockForDBCacheUpdate); err != nil {
					log.Warningf(ctx, "error releasing tables: %s", err)
				}
			}()
			tableDesc, err := sc.getTableVersion(ctx, txn, tc, version)
			if err != nil {
				return err
			}
			// otherTableDescs contains any other table descriptors required by the
			// backfiller processor.
			var otherTableDescs []sqlbase.TableDescriptor
			if backfillType == columnBackfill {
				fkTables, _ := sqlbase.TablesNeededForFKs(
					ctx,
					*tableDesc,
					sqlbase.CheckUpdates,
					sqlbase.NoLookup,
					sqlbase.NoCheckPrivilege,
					nil, /* AnalyzeExprFunction */
				)
				for k := range fkTables {
					table, err := tc.getTableVersionByID(ctx, txn, k)
					if err != nil {
						return err
					}
					otherTableDescs = append(otherTableDescs, *table)
				}
			}
			rw := &errOnlyResultWriter{}
			recv := makeDistSQLReceiver(
				ctx,
				rw,
				tree.Rows, /* stmtType - doesn't matter here since no result are produced */
				sc.rangeDescriptorCache,
				sc.leaseHolderCache,
				nil, /* txn - the flow does not run wholly in a txn */
				func(ts hlc.Timestamp) {
					_ = sc.clock.Update(ts)
				},
			)
			planCtx := sc.distSQLPlanner.newPlanningCtx(ctx, evalCtx, txn)
			plan, err := sc.distSQLPlanner.createBackfiller(
				&planCtx, backfillType, *tableDesc, duration, chunkSize, spans, otherTableDescs, sc.readAsOf,
			)
			if err != nil {
				return err
			}
			sc.distSQLPlanner.Run(
				&planCtx,
				nil, /* txn - the processors manage their own transactions */
				&plan, recv, evalCtx,
			)
			return rw.Err()
		}); err != nil {
			return err
		}
	}
	return nil
}

func (sc *SchemaChanger) backfillIndexes(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	version sqlbase.DescriptorVersion,
) error {
	// Pick a read timestamp for our index backfill, or reuse the previously
	// stored one.
	if err := sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		details := *sc.job.WithTxn(txn).Payload().Details.(*jobs.Payload_SchemaChange).SchemaChange
		if details.ReadAsOf == (hlc.Timestamp{}) {
			details.ReadAsOf = txn.CommitTimestamp()
			if err := sc.job.WithTxn(txn).SetDetails(ctx, details); err != nil {
				return errors.Wrapf(err, "failed to store readAsOf on job %d", *sc.job.ID())
			}
		}
		sc.readAsOf = details.ReadAsOf
		return nil
	}); err != nil {
		return err
	}

	if fn := sc.testingKnobs.RunBeforeIndexBackfill; fn != nil {
		fn()
	}

	return sc.distBackfill(
		ctx, evalCtx, lease, version, indexBackfill, indexBackfillChunkSize,
		distsqlrun.IndexMutationFilter)
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
		distsqlrun.ColumnMutationFilter)
}
