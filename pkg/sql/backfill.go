// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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
	checkpointInterval = 2 * time.Minute
)

var indexBulkBackfillChunkSize = settings.RegisterIntSetting(
	"schemachanger.bulk_index_backfill.batch_size",
	"number of rows to process at a time during bulk index backfill",
	50000,
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
	var addedIndexSpans []roachpb.Span

	var constraintsToAddBeforeValidation []sqlbase.ConstraintToUpdate
	var constraintsToValidate []sqlbase.ConstraintToUpdate

	tableDesc, err := sc.updateJobRunningStatus(ctx, RunningStatusBackfill)
	if err != nil {
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
				addedIndexSpans = append(addedIndexSpans, tableDesc.IndexSpan(t.Index.ID))
			case *sqlbase.DescriptorMutation_Constraint:
				switch t.Constraint.ConstraintType {
				case sqlbase.ConstraintToUpdate_CHECK:
					if t.Constraint.Check.Validity == sqlbase.ConstraintValidity_Validating {
						constraintsToAddBeforeValidation = append(constraintsToAddBeforeValidation, *t.Constraint)
						constraintsToValidate = append(constraintsToValidate, *t.Constraint)
					}
					// TODO (tyler): we do not yet support the NOT VALID foreign keys,
					//  because we don't add the Foreign Key mutations
				case sqlbase.ConstraintToUpdate_FOREIGN_KEY:
					if t.Constraint.ForeignKey.Validity == sqlbase.ConstraintValidity_Validating {
						constraintsToAddBeforeValidation = append(constraintsToAddBeforeValidation, *t.Constraint)
						constraintsToValidate = append(constraintsToValidate, *t.Constraint)
					}
				case sqlbase.ConstraintToUpdate_NOT_NULL:
					// NOT NULL constraints are always validated before they can be added
					constraintsToAddBeforeValidation = append(constraintsToAddBeforeValidation, *t.Constraint)
					constraintsToValidate = append(constraintsToValidate, *t.Constraint)
				}
			default:
				return errors.AssertionFailedf(
					"unsupported mutation: %+v", m)
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
				// Only possible during a rollback
				if !m.Rollback {
					return errors.AssertionFailedf(
						"trying to drop constraint through schema changer outside of a rollback: %+v", t)
				}
				// no-op
			default:
				return errors.AssertionFailedf(
					"unsupported mutation: %+v", m)
			}
		}
	}

	// First drop indexes, then add/drop columns, and only then add indexes and constraints.

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
	if len(addedIndexSpans) > 0 {
		// Check if bulk-adding is enabled and supported by indexes (ie non-unique).
		if err := sc.backfillIndexes(ctx, evalCtx, lease, version, addedIndexSpans); err != nil {
			return err
		}
	}

	// Add check and foreign key constraints, publish the new version of the table descriptor,
	// and wait until the entire cluster is on the new version. This is basically
	// a state transition for the schema change, which must happen after the
	// columns are backfilled and before constraint validation begins. This
	// ensures that 1) all columns are writable and backfilled when the constraint
	// starts being enforced on insert/update (which is relevant in the case where
	// a constraint references both public and non-public columns), and 2) the
	// validation occurs only when the entire cluster is already enforcing the
	// constraint on insert/update.
	if len(constraintsToAddBeforeValidation) > 0 {
		if err := sc.AddConstraints(ctx, constraintsToAddBeforeValidation); err != nil {
			return err
		}
	}

	// Validate check and foreign key constraints.
	if len(constraintsToValidate) > 0 {
		if err := sc.validateConstraints(ctx, evalCtx, lease, constraintsToValidate); err != nil {
			return err
		}
	}
	return nil
}

// AddConstraints publishes a new version of the given table descriptor with the
// given constraint added to it, and waits until the entire cluster is on
// the new version of the table descriptor.
func (sc *SchemaChanger) AddConstraints(
	ctx context.Context, constraints []sqlbase.ConstraintToUpdate,
) error {
	fksByBackrefTable := make(map[sqlbase.ID][]*sqlbase.ConstraintToUpdate)
	for i := range constraints {
		c := &constraints[i]
		if c.ConstraintType == sqlbase.ConstraintToUpdate_FOREIGN_KEY &&
			c.ForeignKey.ReferencedTableID != sc.tableID {
			fksByBackrefTable[c.ForeignKey.ReferencedTableID] = append(fksByBackrefTable[c.ForeignKey.ReferencedTableID], c)
		}
	}
	tableIDsToUpdate := make([]sqlbase.ID, 0, len(fksByBackrefTable)+1)
	tableIDsToUpdate = append(tableIDsToUpdate, sc.tableID)
	for id := range fksByBackrefTable {
		tableIDsToUpdate = append(tableIDsToUpdate, id)
	}

	// Create update closure for the table and all other tables with backreferences
	update := func(descs map[sqlbase.ID]*sqlbase.MutableTableDescriptor) error {
		scTable, ok := descs[sc.tableID]
		if !ok {
			return errors.AssertionFailedf("required table with ID %d not provided to update closure", sc.tableID)
		}
		for i := range constraints {
			constraint := &constraints[i]
			switch constraint.ConstraintType {
			case sqlbase.ConstraintToUpdate_CHECK, sqlbase.ConstraintToUpdate_NOT_NULL:
				found := false
				for _, c := range scTable.Checks {
					if c.Name == constraint.Name {
						log.VEventf(
							ctx, 2,
							"backfiller tried to add constraint %+v but found existing constraint %+v, presumably due to a retry",
							constraint, c,
						)
						found = true
						break
					}
				}
				if !found {
					scTable.Checks = append(scTable.Checks, &constraints[i].Check)
				}
			case sqlbase.ConstraintToUpdate_FOREIGN_KEY:
				var foundExisting bool
				for _, def := range scTable.OutboundFKs {
					if def.Name == constraint.Name {
						if log.V(2) {
							log.VEventf(
								ctx, 2,
								"backfiller tried to add constraint %+v but found existing constraint %+v, presumably due to a retry",
								constraint, def,
							)
						}
						foundExisting = true
						break
					}
				}
				if !foundExisting {
					scTable.OutboundFKs = append(scTable.OutboundFKs, &constraint.ForeignKey)
					backrefTable, ok := descs[constraint.ForeignKey.ReferencedTableID]
					if !ok {
						return errors.AssertionFailedf("required table with ID %d not provided to update closure", sc.tableID)
					}
					backrefTable.InboundFKs = append(backrefTable.InboundFKs, &constraint.ForeignKey)
				}
			}
		}
		return nil
	}

	if _, err := sc.leaseMgr.PublishMultiple(ctx, tableIDsToUpdate, update, nil); err != nil {
		return err
	}
	if err := sc.waitToUpdateLeases(ctx, sc.tableID); err != nil {
		return err
	}
	for id := range fksByBackrefTable {
		if err := sc.waitToUpdateLeases(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (sc *SchemaChanger) validateConstraints(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	lease *sqlbase.TableDescriptor_SchemaChangeLease,
	constraints []sqlbase.ConstraintToUpdate,
) error {
	if testDisableTableLeases {
		return nil
	}

	_, err := sc.updateJobRunningStatus(ctx, RunningStatusValidation)
	if err != nil {
		return err
	}

	if fn := sc.testingKnobs.RunBeforeConstraintValidation; fn != nil {
		if err := fn(); err != nil {
			return err
		}
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
		countDone := make(chan struct{}, len(constraints))

		for i := range constraints {
			c := constraints[i]
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
				desc, err := sqlbase.NewImmutableTableDescriptor(*tableDesc).MakeFirstMutationPublic(sqlbase.IgnoreConstraints)
				if err != nil {
					return err
				}
				// Create a new eval context only because the eval context cannot be shared across many
				// goroutines.
				newEvalCtx := createSchemaChangeEvalCtx(ctx, readAsOf, evalCtx.Tracing, sc.ieFactory)
				switch c.ConstraintType {
				case sqlbase.ConstraintToUpdate_CHECK:
					if err := validateCheckInTxn(ctx, sc.leaseMgr, &newEvalCtx.EvalContext, desc, txn, c.Check.Name); err != nil {
						return err
					}
				case sqlbase.ConstraintToUpdate_FOREIGN_KEY:
					if err := validateFkInTxn(ctx, sc.leaseMgr, &newEvalCtx.EvalContext, desc, txn, c.Name); err != nil {
						return err
					}
				case sqlbase.ConstraintToUpdate_NOT_NULL:
					if err := validateCheckInTxn(ctx, sc.leaseMgr, &newEvalCtx.EvalContext, desc, txn, c.Check.Name); err != nil {
						// TODO (lucy): This should distinguish between constraint
						// validation errors and other types of unexpected errors, and
						// return a different error code in the former case
						return errors.Wrap(err, "validation of NOT NULL constraint failed")
					}
				default:
					return errors.Errorf("unsupported constraint type: %d", c.ConstraintType)
				}
				return nil
			})
		}

		// Periodic schema change lease extension.
		grp.GoCtx(func(ctx context.Context) error {
			count := len(constraints)
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

	return 0, errors.AssertionFailedf(
		"job not found for table id %d, mutation %d", tableDesc.ID, mutationID)
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
	targetSpans []roachpb.Span,
) error {
	duration := checkpointInterval
	if sc.testingKnobs.WriteCheckpointInterval > 0 {
		duration = sc.testingKnobs.WriteCheckpointInterval
	}
	chunkSize := sc.getChunkSize(backfillChunkSize)

	if err := sc.ExtendLease(ctx, lease); err != nil {
		return err
	}

	// start a background goroutine to extend the lease minutely.
	extendLeases := make(chan struct{})
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		tickLease := time.NewTicker(schemaChangeLeaseDuration.Get(&sc.settings.SV) / time.Duration(4))
		defer tickLease.Stop()
		const checkCancelFreq = time.Second * 30
		tickJobCancel := time.NewTicker(checkCancelFreq)
		defer tickJobCancel.Stop()
		ctxDone := ctx.Done()
		for {
			select {
			case <-extendLeases:
				return nil
			case <-ctxDone:
				return nil
			case <-tickJobCancel.C:
				if err := sc.job.CheckStatus(ctx); err != nil {
					return jobs.SimplifyInvalidStatusError(err)
				}
			case <-tickLease.C:
				if err := sc.ExtendLease(ctx, lease); err != nil {
					return err
				}
			}
		}
	})

	g.GoCtx(func(ctx context.Context) error {
		defer close(extendLeases)
		origNRanges := -1
		origFractionCompleted := sc.job.FractionCompleted()
		fractionLeft := 1 - origFractionCompleted
		readAsOf := sc.clock.Now()
		// Index backfilling ingests SSTs that don't play nicely with running txns
		// since they just add their keys blindly. Running a Scan of the target
		// spans at the time the SSTs' keys will be written will calcify history up
		// to then since the scan will resolve intents and populate tscache to keep
		// anything else from sneaking under us. Since these are new indexes, these
		// spans should be essentially empty, so this should be a pretty quick and
		// cheap scan.
		if backfillType == indexBackfill {
			const pageSize = 10000
			noop := func(_ []client.KeyValue) error { return nil }
			if err := sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
				txn.SetFixedTimestamp(ctx, readAsOf)
				for _, span := range targetSpans {
					// TODO(dt): a Count() request would be nice here if the target isn't
					// empty, since we don't need to drag all the results back just to
					// then ignore them -- we just need the iteration on the far end.
					if err := txn.Iterate(ctx, span.Key, span.EndKey, pageSize, noop); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
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
				)()
				return rw.Err()
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return g.Wait()
}

// update the job running status.
func (sc *SchemaChanger) updateJobRunningStatus(
	ctx context.Context, status jobs.RunningStatus,
) (*sqlbase.TableDescriptor, error) {
	var tableDesc *sqlbase.TableDescriptor
	err := sc.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		tableDesc, err = sqlbase.GetTableDescFromID(ctx, txn, sc.tableID)
		if err != nil {
			return err
		}
		// Update running status of job.
		updateJobRunningProgress := false
		for _, mutation := range tableDesc.Mutations {
			if mutation.MutationID != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}

			switch mutation.Direction {
			case sqlbase.DescriptorMutation_ADD:
				switch mutation.State {
				case sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY:
					updateJobRunningProgress = true
				}

			case sqlbase.DescriptorMutation_DROP:
				switch mutation.State {
				case sqlbase.DescriptorMutation_DELETE_ONLY:
					updateJobRunningProgress = true
				}
			}
		}
		if updateJobRunningProgress && !tableDesc.Dropped() {
			if err := sc.job.WithTxn(txn).RunningStatus(ctx, func(
				ctx context.Context, details jobspb.Details) (jobs.RunningStatus, error) {
				return status, nil
			}); err != nil {
				return errors.Wrapf(err, "failed to update running status of job %d", errors.Safe(*sc.job.ID()))
			}
		}
		return nil
	})
	return tableDesc, err
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

	_, err := sc.updateJobRunningStatus(ctx, RunningStatusValidation)
	if err != nil {
		return err
	}

	if fn := sc.testingKnobs.RunBeforeIndexValidation; fn != nil {
		if err := fn(); err != nil {
			return err
		}
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
					return errors.AssertionFailedf(
						"validation of index %s failed: expected %d rows, found %d",
						idx.Name, errors.Safe(expectedCount[i]), errors.Safe(idxLen))
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
			desc, err := sqlbase.NewImmutableTableDescriptor(*tableDesc).MakeFirstMutationPublic(sqlbase.IgnoreConstraints)
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
				fmt.Sprintf(`SELECT count(1) FROM [%d AS t]@[%d] AS OF SYSTEM TIME %s`,
					tableDesc.ID, idx.ID, readAsOf.AsOfSystemTime()))
			if err != nil {
				return err
			}
			idxLen := int64(tree.MustBeDInt(row[0]))

			log.Infof(ctx, "validation: index %s/%s row count = %d, took %s",
				tableDesc.Name, idx.Name, idxLen, timeutil.Since(start))

			select {
			case <-tableCountReady:
				if idxLen != tableRowCount {
					// TODO(vivek): find the offending row and include it in the error.
					return pgerror.Newf(
						pgcode.UniqueViolation,
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
			fmt.Sprintf(`SELECT count(1) FROM [%d AS t] AS OF SYSTEM TIME %s`,
				tableDesc.ID, readAsOf.AsOfSystemTime()))
		if err != nil {
			return err
		}
		tableRowCount = int64(tree.MustBeDInt(cnt[0]))
		tableRowCountTime = timeutil.Since(start)
		log.Infof(ctx, "validation: table %s row count = %d, took %s",
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
	addingSpans []roachpb.Span,
) error {
	if fn := sc.testingKnobs.RunBeforeIndexBackfill; fn != nil {
		fn()
	}

	// TODO(jeffreyxiao): Remove check in 20.1.
	// If the cluster supports sticky bits, then we should use the sticky bit to
	// ensure that the splits are not automatically split by the merge queue. If
	// the cluster does not support sticky bits, we disable the merge queue via
	// gossip, so we can just set the split to expire immediately.
	stickyBitEnabled := sc.execCfg.Settings.Version.IsActive(cluster.VersionStickyBit)
	expirationTime := hlc.Timestamp{}
	if !stickyBitEnabled {
		disableCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		sc.execCfg.Gossip.DisableMerges(disableCtx, []uint32{uint32(sc.tableID)})
	} else {
		expirationTime = sc.db.Clock().Now().Add(time.Hour.Nanoseconds(), 0)
	}

	for _, span := range addingSpans {
		if err := sc.db.AdminSplit(ctx, span.Key, span.Key, expirationTime); err != nil {
			return err
		}
	}

	chunkSize := indexBulkBackfillChunkSize.Get(&sc.settings.SV)
	if err := sc.distBackfill(
		ctx, evalCtx, lease, version, indexBackfill, chunkSize,
		backfill.IndexMutationFilter, addingSpans); err != nil {
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
		backfill.ColumnMutationFilter, nil)
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
			tbKey := sqlbase.NewTableKey(drain.ParentID, drain.Name).Key()
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
	var constraintsToValidate []sqlbase.ConstraintToUpdate

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
				case sqlbase.ConstraintToUpdate_CHECK, sqlbase.ConstraintToUpdate_NOT_NULL:
					tableDesc.Checks = append(tableDesc.Checks, &t.Constraint.Check)
				case sqlbase.ConstraintToUpdate_FOREIGN_KEY:
					tableDesc.OutboundFKs = append(tableDesc.OutboundFKs, &t.Constraint.ForeignKey)
				default:
					return errors.AssertionFailedf(
						"unsupported constraint type: %d", errors.Safe(t.Constraint.ConstraintType))
				}
				constraintsToValidate = append(constraintsToValidate, *t.Constraint)

			default:
				return errors.AssertionFailedf(
					"unsupported mutation: %+v", m)
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
				return errors.AssertionFailedf(
					"constraint validation mutation cannot be in the DROP state within the same transaction: %+v", m)

			default:
				return errors.AssertionFailedf("unsupported mutation: %+v", m)
			}

		}
		if err := tableDesc.MakeMutationComplete(m); err != nil {
			return err
		}
	}
	tableDesc.Mutations = nil

	// Now that the table descriptor is in a valid state with all column and index
	// mutations applied, it can be used for validating check constraints
	for _, c := range constraintsToValidate {
		switch c.ConstraintType {
		case sqlbase.ConstraintToUpdate_CHECK, sqlbase.ConstraintToUpdate_NOT_NULL:
			if err := validateCheckInTxn(ctx, tc.leaseMgr, evalCtx, tableDesc, txn, c.Check.Name); err != nil {
				return err
			}
		case sqlbase.ConstraintToUpdate_FOREIGN_KEY:
			// We can't support adding a validated foreign key constraint in the same
			// transaction as the CREATE TABLE statement. This would require adding
			// the backreference to the other table and then validating the constraint
			// for whatever rows were inserted into the referencing table in this
			// transaction, which requires multiple schema changer states across
			// multiple transactions.
			// TODO (lucy): Add a validation job that runs after the user transaction.
			// This won't roll back the original transaction if validation fails, but
			// it will at least leave the constraint in the Validated state if
			// validation succeeds.

			// For now, revert the constraint to an unvalidated state.
			for _, desc := range tableDesc.OutboundFKs {
				if desc.Name == c.ForeignKey.Name {
					desc.Validity = sqlbase.ConstraintValidity_Unvalidated
					break
				}
			}
		default:
			return errors.AssertionFailedf(
				"unsupported constraint type: %d", errors.Safe(c.ConstraintType))
		}
	}
	return nil
}

// validateCheckInTxn validates check constraints within the provided
// transaction. If the provided table descriptor version is newer than the
// cluster version, it will be used in the InternalExecutor that performs the
// validation query.
// TODO (lucy): The special case where the table descriptor version is the same
// as the cluster version only happens because the query in VALIDATE CONSTRAINT
// still runs in the user transaction instead of a step in the schema changer.
// When that's no longer true, this function should be updated.
func validateCheckInTxn(
	ctx context.Context,
	leaseMgr *LeaseManager,
	evalCtx *tree.EvalContext,
	tableDesc *MutableTableDescriptor,
	txn *client.Txn,
	checkName string,
) error {
	ie := evalCtx.InternalExecutor.(*SessionBoundInternalExecutor)
	if tableDesc.Version > tableDesc.ClusterVersion.Version {
		newTc := &TableCollection{leaseMgr: leaseMgr}
		// pretend that the schema has been modified.
		if err := newTc.addUncommittedTable(*tableDesc); err != nil {
			return err
		}

		ie.impl.tcModifier = newTc
		defer func() {
			ie.impl.tcModifier = nil
		}()
	}

	check, err := tableDesc.FindCheckByName(checkName)
	if err != nil {
		return err
	}
	return validateCheckExpr(ctx, check.Expr, tableDesc.TableDesc(), ie, txn)
}

// validateFkInTxn validates foreign key constraints within the provided
// transaction. If the provided table descriptor version is newer than the
// cluster version, it will be used in the InternalExecutor that performs the
// validation query.
// TODO (lucy): The special case where the table descriptor version is the same
// as the cluster version only happens because the query in VALIDATE CONSTRAINT
// still runs in the user transaction instead of a step in the schema changer.
// When that's no longer true, this function should be updated.
func validateFkInTxn(
	ctx context.Context,
	leaseMgr *LeaseManager,
	evalCtx *tree.EvalContext,
	tableDesc *MutableTableDescriptor,
	txn *client.Txn,
	fkName string,
) error {
	ie := evalCtx.InternalExecutor.(*SessionBoundInternalExecutor)
	if tableDesc.Version > tableDesc.ClusterVersion.Version {
		newTc := &TableCollection{leaseMgr: leaseMgr}
		// pretend that the schema has been modified.
		if err := newTc.addUncommittedTable(*tableDesc); err != nil {
			return err
		}

		ie.impl.tcModifier = newTc
		defer func() {
			ie.impl.tcModifier = nil
		}()
	}

	var fk *sqlbase.ForeignKeyConstraint
	for _, def := range tableDesc.OutboundFKs {
		if def.Name == fkName {
			fk = def
			break
		}
	}
	if fk == nil {
		return errors.AssertionFailedf("foreign key %s does not exist", fkName)
	}

	return validateForeignKey(ctx, tableDesc.TableDesc(), fk, ie, txn)
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
			return errors.AssertionFailedf(
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
