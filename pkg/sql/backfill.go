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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	// indexTruncateChunkSize is the maximum number of index entries truncated
	// per chunk during an index truncation. This value is larger than the
	// other chunk constants because the operation involves only running a
	// DeleteRange().
	indexTruncateChunkSize = row.TableTruncateChunkSize

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

// indexBackfillBatchSize is the maximum number of rows we construct index
// entries for before we attempt to fill in a single index batch before queueing
// it up for ingestion and progress reporting in the index backfiller processor.
var indexBackfillBatchSize = settings.RegisterIntSetting(
	settings.TenantWritable,
	"bulkio.index_backfill.batch_size",
	"the number of rows for which we construct index entries in a single batch",
	50000,
	settings.NonNegativeInt, /* validateFn */
)

// columnBackfillBatchSize is the maximum number of rows we update at once when
// adding or removing columns.
var columnBackfillBatchSize = settings.RegisterIntSetting(
	settings.TenantWritable,
	"bulkio.column_backfill.batch_size",
	"the number of rows updated at a time to add/remove columns",
	200,
	settings.NonNegativeInt, /* validateFn */
)

var _ sort.Interface = columnsByID{}
var _ sort.Interface = indexesByID{}

type columnsByID []descpb.ColumnDescriptor

func (cds columnsByID) Len() int {
	return len(cds)
}
func (cds columnsByID) Less(i, j int) bool {
	return cds[i].ID < cds[j].ID
}
func (cds columnsByID) Swap(i, j int) {
	cds[i], cds[j] = cds[j], cds[i]
}

type indexesByID []descpb.IndexDescriptor

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

// scTxnFn is the type of functions that operates using transactions in the backfiller.
type scTxnFn func(ctx context.Context, txn *kv.Txn, evalCtx *extendedEvalContext) error

// historicalTxnRunner is the type of the callback used by the various
// helper functions to run checks at a fixed timestamp (logically, at
// the start of the backfill).
type historicalTxnRunner func(ctx context.Context, fn scTxnFn) error

// makeFixedTimestampRunner creates a historicalTxnRunner suitable for use by the helpers.
func (sc *SchemaChanger) makeFixedTimestampRunner(readAsOf hlc.Timestamp) historicalTxnRunner {
	runner := func(ctx context.Context, retryable scTxnFn) error {
		return sc.fixedTimestampTxn(ctx, readAsOf, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
			// We need to re-create the evalCtx since the txn may retry.
			evalCtx := createSchemaChangeEvalCtx(ctx, sc.execCfg, readAsOf, descriptors)
			return retryable(ctx, txn, &evalCtx)
		})
	}
	return runner
}

// makeFixedTimestampRunner creates a HistoricalTxnRunner suitable for use by the helpers.
func (sc *SchemaChanger) makeFixedTimestampInternalExecRunner(
	readAsOf hlc.Timestamp,
) sqlutil.HistoricalInternalExecTxnRunner {
	runner := func(ctx context.Context, retryable sqlutil.InternalExecFn) error {
		return sc.fixedTimestampTxn(ctx, readAsOf, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
			// We need to re-create the evalCtx since the txn may retry.
			ie := sc.ieFactory(ctx, NewFakeSessionData(sc.execCfg.SV()))
			return retryable(ctx, txn, ie)
		})
	}
	return runner
}

func (sc *SchemaChanger) fixedTimestampTxn(
	ctx context.Context,
	readAsOf hlc.Timestamp,
	retryable func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error,
) error {
	return sc.txn(ctx, func(ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
		if err := txn.SetFixedTimestamp(ctx, readAsOf); err != nil {
			return err
		}
		return retryable(ctx, txn, descriptors)
	})
}

// runBackfill runs the backfill for the schema changer.
//
// This operates over multiple goroutines concurrently and is thus not
// able to reuse the original kv.Txn safely. The various
// function that it calls make their own txns.
func (sc *SchemaChanger) runBackfill(ctx context.Context) error {
	if sc.testingKnobs.RunBeforeBackfill != nil {
		if err := sc.testingKnobs.RunBeforeBackfill(); err != nil {
			return err
		}
	}

	// Mutations are applied in a FIFO order. Only apply the first set of
	// mutations. Collect the elements that are part of the mutation.
	var addedIndexSpans []roachpb.Span
	var addedIndexes []descpb.IndexID
	var temporaryIndexes []descpb.IndexID

	var constraintsToDrop []catalog.ConstraintToUpdate
	var constraintsToAddBeforeValidation []catalog.ConstraintToUpdate
	var constraintsToValidate []catalog.ConstraintToUpdate

	var viewToRefresh catalog.MaterializedViewRefresh

	// Note that this descriptor is intentionally not leased. If the schema change
	// held the lease, certain non-mutation related schema changes would not be
	// able to proceed. That might be okay and even desirable. The bigger reason
	// to not hold a lease throughout the duration of this schema change stage
	// is more practical. The lease manager (and associated descriptor
	// infrastructure) does not provide a mechanism to hold a lease over a long
	// period of time and update the transaction commit deadline. As such, when
	// the schema change job attempts to mutate the descriptor later in this
	// method, the descriptor will need to be re-read and the operation should be
	// revalidated against the new state of the descriptor. Any work to hold
	// leases during mutations will need to consider the user experience when the
	// user would like to issue schema changes to be applied asynchronously.
	// Perhaps such schema changes could avoid waiting for a single version and
	// thus avoid blocked. This will get ironed out in the context of
	// transactional schema changes. In all likelihood, not holding a lease here
	// is the right thing to do as we would never want this operation to fail
	// because a new mutation was enqueued.
	tableDesc, err := sc.updateJobRunningStatus(ctx, RunningStatusBackfill)
	if err != nil {
		return err
	}

	// Short circuit the backfill if the table has been deleted.
	if tableDesc.Dropped() {
		return nil
	}
	version := tableDesc.GetVersion()

	log.Infof(ctx, "running backfill for %q, v=%d", tableDesc.GetName(), tableDesc.GetVersion())

	needColumnBackfill := false
	for _, m := range tableDesc.AllMutations() {
		if m.MutationID() != sc.mutationID {
			break
		}
		// If the current mutation is discarded, then
		// skip over processing.
		if discarded, _ := isCurrentMutationDiscarded(tableDesc, m, m.MutationOrdinal()+1); discarded {
			continue
		}

		if m.Adding() {
			if col := m.AsColumn(); col != nil {
				// Its possible have a mix of columns that need a backfill and others
				// that don't, so preserve the flag if its already been flipped.
				needColumnBackfill = needColumnBackfill || catalog.ColumnNeedsBackfill(col)
			} else if idx := m.AsIndex(); idx != nil {
				if idx.IsTemporaryIndexForBackfill() {
					temporaryIndexes = append(temporaryIndexes, idx.GetID())
				} else {
					addedIndexSpans = append(addedIndexSpans, tableDesc.IndexSpan(sc.execCfg.Codec, idx.GetID()))
					addedIndexes = append(addedIndexes, idx.GetID())
				}
			} else if c := m.AsConstraint(); c != nil {
				isValidating := c.IsCheck() && c.Check().Validity == descpb.ConstraintValidity_Validating ||
					c.IsForeignKey() && c.ForeignKey().Validity == descpb.ConstraintValidity_Validating ||
					c.IsUniqueWithoutIndex() && c.UniqueWithoutIndex().Validity == descpb.ConstraintValidity_Validating ||
					c.IsNotNull()
				isSkippingValidation, err := shouldSkipConstraintValidation(tableDesc, c)
				if err != nil {
					return err
				}
				if isValidating {
					constraintsToAddBeforeValidation = append(constraintsToAddBeforeValidation, c)
				}
				if isValidating && !isSkippingValidation {
					constraintsToValidate = append(constraintsToValidate, c)
				}
			} else if mvRefresh := m.AsMaterializedViewRefresh(); mvRefresh != nil {
				viewToRefresh = mvRefresh
			} else if m.AsPrimaryKeySwap() != nil || m.AsComputedColumnSwap() != nil || m.AsModifyRowLevelTTL() != nil {
				// The backfiller doesn't need to do anything here.
			} else {
				return errors.AssertionFailedf("unsupported mutation: %+v", m)
			}
		} else if m.Dropped() {
			if col := m.AsColumn(); col != nil {
				// Its possible have a mix of columns that need a backfill and others
				// that don't, so preserve the flag if its already been flipped.
				needColumnBackfill = needColumnBackfill || catalog.ColumnNeedsBackfill(col)
			} else if idx := m.AsIndex(); idx != nil {
				// no-op. Handled in (*schemaChanger).done by queueing an index gc job.
			} else if c := m.AsConstraint(); c != nil {
				constraintsToDrop = append(constraintsToDrop, c)
			} else if m.AsPrimaryKeySwap() != nil || m.AsComputedColumnSwap() != nil || m.AsMaterializedViewRefresh() != nil || m.AsModifyRowLevelTTL() != nil {
				// The backfiller doesn't need to do anything here.
			} else {
				return errors.AssertionFailedf("unsupported mutation: %+v", m)
			}
		}
	}

	// If we were requested to refresh a view, then do so.
	if viewToRefresh != nil {
		if err := sc.refreshMaterializedView(ctx, tableDesc, viewToRefresh); err != nil {
			return err
		}
	}

	// First drop constraints and indexes, then add/drop columns, and only then
	// add indexes and constraints.

	// Drop constraints.
	if len(constraintsToDrop) > 0 {
		descs, err := sc.dropConstraints(ctx, constraintsToDrop)
		if err != nil {
			return err
		}
		version = descs[tableDesc.GetID()].GetVersion()
	}

	// Add and drop columns.
	if needColumnBackfill {
		if err := sc.truncateAndBackfillColumns(ctx, version); err != nil {
			return err
		}
	}

	// Add new indexes.
	if len(addedIndexSpans) > 0 {
		// Check if bulk-adding is enabled and supported by indexes (ie non-unique).
		if err := sc.backfillIndexes(ctx, version, addedIndexSpans, addedIndexes, temporaryIndexes); err != nil {
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
		if err := sc.addConstraints(ctx, constraintsToAddBeforeValidation); err != nil {
			return err
		}
	}

	// Validate check and foreign key constraints.
	if len(constraintsToValidate) > 0 {
		if err := sc.validateConstraints(ctx, constraintsToValidate); err != nil {
			return err
		}
	}

	log.Infof(ctx, "completed backfill for %q, v=%d", tableDesc.GetName(), tableDesc.GetVersion())

	if sc.testingKnobs.RunAfterBackfill != nil {
		if err := sc.testingKnobs.RunAfterBackfill(sc.job.ID()); err != nil {
			return err
		}
	}

	return nil
}

// shouldSkipConstraintValidation checks if a validating constraint should skip
// validation and be added directly. A Check Constraint can skip validation if it's
// created for a shard column internally.
func shouldSkipConstraintValidation(
	tableDesc catalog.TableDescriptor, c catalog.ConstraintToUpdate,
) (bool, error) {
	if !c.IsCheck() {
		return false, nil
	}

	check := c.Check()
	// The check constraint on shard column is always on the shard column itself.
	if len(check.ColumnIDs) != 1 {
		return false, nil
	}

	checkCol, err := tableDesc.FindColumnWithID(check.ColumnIDs[0])
	if err != nil {
		return false, err
	}

	// We only want to skip validation when the shard column is first added and
	// the constraint is created internally since the shard column computation is
	// well defined. Note that we show the shard column in `SHOW CREATE TABLE`,
	// and we don't prevent users from adding other constraints on it. For those
	// constraints, we still want to validate.
	return tableDesc.IsShardColumn(checkCol) && checkCol.Adding(), nil
}

// dropConstraints publishes a new version of the given table descriptor with
// the given constraint removed from it, and waits until the entire cluster is
// on the new version of the table descriptor. It returns the new table descs.
func (sc *SchemaChanger) dropConstraints(
	ctx context.Context, constraints []catalog.ConstraintToUpdate,
) (map[descpb.ID]catalog.TableDescriptor, error) {
	log.Infof(ctx, "dropping %d constraints", len(constraints))

	fksByBackrefTable := make(map[descpb.ID][]catalog.ConstraintToUpdate)
	for _, c := range constraints {
		if c.IsForeignKey() {
			id := c.ForeignKey().ReferencedTableID
			if id != sc.descID {
				fksByBackrefTable[id] = append(fksByBackrefTable[id], c)
			}
		}
	}

	// Create update closure for the table and all other tables with backreferences.
	if err := sc.txn(ctx, func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
		scTable, err := descsCol.GetMutableTableVersionByID(ctx, sc.descID, txn)
		if err != nil {
			return err
		}
		b := txn.NewBatch()
		for _, constraint := range constraints {
			if constraint.IsCheck() || constraint.IsNotNull() {
				found := false
				for j, c := range scTable.Checks {
					if c.Name == constraint.GetName() {
						scTable.Checks = append(scTable.Checks[:j], scTable.Checks[j+1:]...)
						found = true
						break
					}
				}
				if !found {
					log.VEventf(
						ctx, 2,
						"backfiller tried to drop constraint %+v but it was not found, "+
							"presumably due to a retry or rollback",
						constraint.ConstraintToUpdateDesc(),
					)
				}
			} else if constraint.IsForeignKey() {
				var foundExisting bool
				for j := range scTable.OutboundFKs {
					def := &scTable.OutboundFKs[j]
					if def.Name != constraint.GetName() {
						continue
					}
					backrefTable, err := descsCol.GetMutableTableVersionByID(ctx,
						constraint.ForeignKey().ReferencedTableID, txn)
					if err != nil {
						return err
					}
					if err := removeFKBackReferenceFromTable(
						backrefTable, def.Name, scTable,
					); err != nil {
						return err
					}
					if err := descsCol.WriteDescToBatch(
						ctx, true /* kvTrace */, backrefTable, b,
					); err != nil {
						return err
					}
					scTable.OutboundFKs = append(scTable.OutboundFKs[:j], scTable.OutboundFKs[j+1:]...)
					foundExisting = true
					break
				}
				if !foundExisting {
					log.VEventf(
						ctx, 2,
						"backfiller tried to drop constraint %+v but it was not found, "+
							"presumably due to a retry or rollback",
						constraint.ConstraintToUpdateDesc(),
					)
				}
			} else if constraint.IsUniqueWithoutIndex() {
				found := false
				for j, c := range scTable.UniqueWithoutIndexConstraints {
					if c.Name == constraint.GetName() {
						scTable.UniqueWithoutIndexConstraints = append(
							scTable.UniqueWithoutIndexConstraints[:j],
							scTable.UniqueWithoutIndexConstraints[j+1:]...,
						)
						found = true
						break
					}
				}
				if !found {
					log.VEventf(
						ctx, 2,
						"backfiller tried to drop constraint %+v but it was not found, "+
							"presumably due to a retry or rollback",
						constraint.ConstraintToUpdateDesc(),
					)
				}
			}
		}
		if err := descsCol.WriteDescToBatch(
			ctx, true /* kvTrace */, scTable, b,
		); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	}); err != nil {
		return nil, err
	}

	log.Info(ctx, "finished dropping constraints")
	tableDescs := make(map[descpb.ID]catalog.TableDescriptor, len(fksByBackrefTable)+1)
	if err := sc.txn(ctx, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) (err error) {
		if tableDescs[sc.descID], err = descsCol.GetImmutableTableByID(
			ctx, txn, sc.descID, tree.ObjectLookupFlags{},
		); err != nil {
			return err
		}
		for id := range fksByBackrefTable {
			if tableDescs[id], err = descsCol.GetImmutableTableByID(
				ctx, txn, id, tree.ObjectLookupFlags{},
			); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return tableDescs, nil
}

// addConstraints publishes a new version of the given table descriptor with the
// given constraint added to it, and waits until the entire cluster is on
// the new version of the table descriptor.
func (sc *SchemaChanger) addConstraints(
	ctx context.Context, constraints []catalog.ConstraintToUpdate,
) error {
	log.Infof(ctx, "adding %d constraints", len(constraints))

	fksByBackrefTable := make(map[descpb.ID][]catalog.ConstraintToUpdate)
	for _, c := range constraints {
		if c.IsForeignKey() {
			id := c.ForeignKey().ReferencedTableID
			if id != sc.descID {
				fksByBackrefTable[id] = append(fksByBackrefTable[id], c)
			}
		}
	}

	// Create update closure for the table and all other tables with backreferences
	if err := sc.txn(ctx, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		scTable, err := descsCol.GetMutableTableVersionByID(ctx, sc.descID, txn)
		if err != nil {
			return err
		}

		b := txn.NewBatch()
		for _, constraint := range constraints {
			if constraint.IsCheck() || constraint.IsNotNull() {
				found := false
				for _, c := range scTable.Checks {
					if c.Name == constraint.GetName() {
						log.VEventf(
							ctx, 2,
							"backfiller tried to add constraint %+v but found existing constraint %+v, "+
								"presumably due to a retry or rollback",
							constraint, c,
						)
						// Ensure the constraint on the descriptor is set to Validating, in
						// case we're in the middle of rolling back DROP CONSTRAINT
						c.Validity = descpb.ConstraintValidity_Validating
						found = true
						break
					}
				}
				if !found {
					scTable.Checks = append(scTable.Checks, &constraint.ConstraintToUpdateDesc().Check)
				}
			} else if constraint.IsForeignKey() {
				var foundExisting bool
				for j := range scTable.OutboundFKs {
					def := &scTable.OutboundFKs[j]
					if def.Name == constraint.GetName() {
						if log.V(2) {
							log.VEventf(
								ctx, 2,
								"backfiller tried to add constraint %+v but found existing constraint %+v, "+
									"presumably due to a retry or rollback",
								constraint.ConstraintToUpdateDesc(), def,
							)
						}
						// Ensure the constraint on the descriptor is set to Validating, in
						// case we're in the middle of rolling back DROP CONSTRAINT
						def.Validity = descpb.ConstraintValidity_Validating
						foundExisting = true
						break
					}
				}
				if !foundExisting {
					scTable.OutboundFKs = append(scTable.OutboundFKs, constraint.ForeignKey())
					backrefTable, err := descsCol.GetMutableTableVersionByID(ctx, constraint.ForeignKey().ReferencedTableID, txn)
					if err != nil {
						return err
					}
					// Check that a unique constraint for the FK still exists on the
					// referenced table. It's possible for the unique index found during
					// planning to have been dropped in the meantime, since only the
					// presence of the backreference prevents it.
					_, err = tabledesc.FindFKReferencedUniqueConstraint(backrefTable, constraint.ForeignKey().ReferencedColumnIDs)
					if err != nil {
						return err
					}
					backrefTable.InboundFKs = append(backrefTable.InboundFKs, constraint.ForeignKey())

					// Note that this code may add the same descriptor to the batch
					// multiple times if it is referenced multiple times. That's fine as
					// the last put will win but it's perhaps not ideal. We could add
					// code to deduplicate but it doesn't seem worth the hassle.
					if backrefTable != scTable {
						if err := descsCol.WriteDescToBatch(
							ctx, true /* kvTrace */, backrefTable, b,
						); err != nil {
							return err
						}
					}
				}
			} else if constraint.IsUniqueWithoutIndex() {
				found := false
				for _, c := range scTable.UniqueWithoutIndexConstraints {
					if c.Name == constraint.GetName() {
						log.VEventf(
							ctx, 2,
							"backfiller tried to add constraint %+v but found existing constraint %+v, "+
								"presumably due to a retry or rollback",
							constraint.ConstraintToUpdateDesc(), c,
						)
						// Ensure the constraint on the descriptor is set to Validating, in
						// case we're in the middle of rolling back DROP CONSTRAINT
						c.Validity = descpb.ConstraintValidity_Validating
						found = true
						break
					}
				}
				if !found {
					scTable.UniqueWithoutIndexConstraints = append(scTable.UniqueWithoutIndexConstraints,
						constraint.UniqueWithoutIndex())
				}
			}
		}
		if err := descsCol.WriteDescToBatch(
			ctx, true /* kvTrace */, scTable, b,
		); err != nil {
			return err
		}
		return txn.Run(ctx, b)
	}); err != nil {
		return err
	}
	log.Info(ctx, "finished adding constraints")
	return nil
}

// validateConstraints checks that the current table data obeys the
// provided constraints.
//
// This operates over multiple goroutines concurrently and is thus not
// able to reuse the original kv.Txn safely, so it makes its own.
func (sc *SchemaChanger) validateConstraints(
	ctx context.Context, constraints []catalog.ConstraintToUpdate,
) error {
	if lease.TestingTableLeasesAreDisabled() {
		return nil
	}
	log.Infof(ctx, "validating %d new constraints", len(constraints))

	_, err := sc.updateJobRunningStatus(ctx, RunningStatusValidation)
	if err != nil {
		return err
	}

	if fn := sc.testingKnobs.RunBeforeConstraintValidation; fn != nil {
		if err := fn(constraints); err != nil {
			return err
		}
	}

	readAsOf := sc.clock.Now()
	var tableDesc catalog.TableDescriptor

	if err := sc.fixedTimestampTxn(ctx, readAsOf, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {
		flags := tree.ObjectLookupFlagsWithRequired()
		flags.AvoidLeased = true
		tableDesc, err = descriptors.GetImmutableTableByID(ctx, txn, sc.descID, flags)
		return err
	}); err != nil {
		return err
	}

	grp := ctxgroup.WithContext(ctx)
	// The various checks below operate at a fixed timestamp.
	runHistoricalTxn := sc.makeFixedTimestampRunner(readAsOf)

	for i := range constraints {
		c := constraints[i]
		grp.GoCtx(func(ctx context.Context) error {
			// Make the mutations public in a private copy of the descriptor
			// and add it to the Collection, so that we can use SQL below to perform
			// the validation. We wouldn't have needed to do this if we could have
			// updated the descriptor and run validation in the same transaction. However,
			// our current system is incapable of running long running schema changes
			// (the validation can take many minutes). So we pretend that the schema
			// has been updated and actually update it in a separate transaction that
			// follows this one.
			descI, err := tableDesc.MakeFirstMutationPublic(catalog.IgnoreConstraints)
			if err != nil {
				return err
			}
			desc := descI.(*tabledesc.Mutable)
			// Each check operates at the historical timestamp.
			return runHistoricalTxn(ctx, func(ctx context.Context, txn *kv.Txn, evalCtx *extendedEvalContext) error {
				// If the constraint is a check constraint that fails validation, we
				// need a semaContext set up that can resolve types in order to pretty
				// print the check expression back to the user.
				evalCtx.Txn = txn
				// Use the DistSQLTypeResolver because we need to resolve types by ID.
				collection := evalCtx.Descs
				resolver := descs.NewDistSQLTypeResolver(collection, txn)
				semaCtx := tree.MakeSemaContext()
				semaCtx.TypeResolver = &resolver
				// TODO (rohany): When to release this? As of now this is only going to get released
				//  after the check is validated.
				defer func() { collection.ReleaseAll(ctx) }()
				if c.IsCheck() {
					if err := validateCheckInTxn(
						ctx, &semaCtx, sc.ieFactory, evalCtx.SessionData(), desc, txn, c.Check().Expr,
					); err != nil {
						return err
					}
				} else if c.IsForeignKey() {
					if err := validateFkInTxn(ctx, sc.ieFactory, evalCtx.SessionData(), desc, txn, collection, c.GetName()); err != nil {
						return err
					}
				} else if c.IsUniqueWithoutIndex() {
					if err := validateUniqueWithoutIndexConstraintInTxn(ctx, sc.ieFactory(ctx, evalCtx.SessionData()), desc, txn, c.GetName()); err != nil {
						return err
					}
				} else if c.IsNotNull() {
					if err := validateCheckInTxn(
						ctx, &semaCtx, sc.ieFactory, evalCtx.SessionData(), desc, txn, c.Check().Expr,
					); err != nil {
						// TODO (lucy): This should distinguish between constraint
						// validation errors and other types of unexpected errors, and
						// return a different error code in the former case
						return errors.Wrap(err, "validation of NOT NULL constraint failed")
					}
				} else {
					return errors.Errorf("unsupported constraint type: %d", c.ConstraintToUpdateDesc().ConstraintType)
				}
				return nil
			})
		})
	}
	if err := grp.Wait(); err != nil {
		return err
	}
	log.Info(ctx, "finished validating new constraints")
	return nil
}

// getTableVersion retrieves the descriptor for the table being
// targeted by the schema changer using the provided txn, and asserts
// that the retrieved descriptor is at the given version. An error is
// returned otherwise.
//
// It operates entirely on the current goroutine and is thus able to
// reuse an existing kv.Txn safely.
func (sc *SchemaChanger) getTableVersion(
	ctx context.Context, txn *kv.Txn, tc *descs.Collection, version descpb.DescriptorVersion,
) (catalog.TableDescriptor, error) {
	tableDesc, err := tc.GetImmutableTableByID(ctx, txn, sc.descID, tree.ObjectLookupFlags{})
	if err != nil {
		return nil, err
	}
	if version != tableDesc.GetVersion() {
		return nil, makeErrTableVersionMismatch(tableDesc.GetVersion(), version)
	}
	return tableDesc, nil
}

// getJobIDForMutationWithDescriptor returns a job id associated with a mutation given
// a table descriptor. Unlike getJobIDForMutation this doesn't need transaction.
// TODO (lucy): This is not a good way to look up all schema change jobs
// associated with a table. We should get rid of MutationJobs and start looking
// up the jobs in the jobs table instead.
func getJobIDForMutationWithDescriptor(
	ctx context.Context, tableDesc catalog.TableDescriptor, mutationID descpb.MutationID,
) (jobspb.JobID, error) {
	for _, job := range tableDesc.GetMutationJobs() {
		if job.MutationID == mutationID {
			return job.JobID, nil
		}
	}

	return jobspb.InvalidJobID, errors.AssertionFailedf(
		"job not found for table id %d, mutation %d", tableDesc.GetID(), mutationID)
}

// numRangesInSpans returns the number of ranges that cover a set of spans.
//
// It operates entirely on the current goroutine and is thus able to
// reuse an existing kv.Txn safely.
func numRangesInSpans(
	ctx context.Context, db *kv.DB, distSQLPlanner *DistSQLPlanner, spans []roachpb.Span,
) (int, error) {
	txn := db.NewTxn(ctx, "num-ranges-in-spans")
	spanResolver := distSQLPlanner.spanResolver.NewSpanResolverIterator(txn)
	rangeIds := make(map[int64]struct{})
	for _, span := range spans {
		// For each span, iterate the spanResolver until it's exhausted, storing
		// the found range ids in the map to de-duplicate them.
		spanResolver.Seek(ctx, span, kvcoord.Ascending)
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

// NumRangesInSpanContainedBy returns the number of ranges that covers
// a span and how many of those ranged are wholly contained in containedBy.
//
// It operates entirely on the current goroutine and is thus able to
// reuse an existing kv.Txn safely.
func NumRangesInSpanContainedBy(
	ctx context.Context,
	db *kv.DB,
	distSQLPlanner *DistSQLPlanner,
	outerSpan roachpb.Span,
	containedBy []roachpb.Span,
) (total, inContainedBy int, _ error) {
	txn := db.NewTxn(ctx, "num-ranges-in-spans")
	spanResolver := distSQLPlanner.spanResolver.NewSpanResolverIterator(txn)
	// For each span, iterate the spanResolver until it's exhausted, storing
	// the found range ids in the map to de-duplicate them.
	spanResolver.Seek(ctx, outerSpan, kvcoord.Ascending)
	var g roachpb.SpanGroup
	g.Add(containedBy...)
	for {
		if !spanResolver.Valid() {
			return 0, 0, spanResolver.Error()
		}
		total++
		desc := spanResolver.Desc()
		if g.Encloses(desc.RSpan().AsRawSpanWithNoLocals().Intersect(outerSpan)) {
			inContainedBy++
		}
		if !spanResolver.NeedAnother() {
			break
		}
		spanResolver.Next(ctx)
	}
	return total, inContainedBy, nil
}

// TODO(adityamaru): Consider moving this to sql/backfill. It has a lot of
// schema changer dependencies which will need to be passed around.
func (sc *SchemaChanger) distIndexBackfill(
	ctx context.Context,
	version descpb.DescriptorVersion,
	targetSpans []roachpb.Span,
	addedIndexes []descpb.IndexID,
	writeAtRequestTimestamp bool,
	filter backfill.MutationFilter,
) error {

	// Variables to track progress of the index backfill.
	origNRanges := -1
	origFractionCompleted := sc.job.FractionCompleted()
	fractionLeft := 1 - origFractionCompleted

	// Gather the initial resume spans for the table.
	var todoSpans []roachpb.Span
	var mutationIdx int

	if err := DescsTxn(ctx, sc.execCfg, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) (err error) {
		todoSpans, _, mutationIdx, err = rowexec.GetResumeSpans(
			ctx, sc.jobRegistry, txn, sc.execCfg.Codec, col, sc.descID, sc.mutationID, filter)
		return err
	}); err != nil {
		return err
	}

	log.VEventf(ctx, 2, "indexbackfill: initial resume spans %+v", todoSpans)

	if todoSpans == nil {
		return nil
	}

	writeAsOf := sc.job.Details().(jobspb.SchemaChangeDetails).WriteTimestamp
	if writeAsOf.IsEmpty() {
		if err := sc.job.RunningStatus(ctx, nil /* txn */, func(_ context.Context, _ jobspb.Details) (jobs.RunningStatus, error) {
			return jobs.RunningStatus("scanning target index for in-progress transactions"), nil
		}); err != nil {
			return errors.Wrapf(err, "failed to update running status of job %d", errors.Safe(sc.job.ID()))
		}
		writeAsOf = sc.clock.Now()
		log.Infof(ctx, "starting scan of target index as of %v...", writeAsOf)
		// Index backfilling ingests SSTs that don't play nicely with running txns
		// since they just add their keys blindly. Running a Scan of the target
		// spans at the time the SSTs' keys will be written will calcify history up
		// to then since the scan will resolve intents and populate tscache to keep
		// anything else from sneaking under us. Since these are new indexes, these
		// spans should be essentially empty, so this should be a pretty quick and
		// cheap scan.
		const pageSize = 10000
		noop := func(_ []kv.KeyValue) error { return nil }
		if err := sc.fixedTimestampTxn(ctx, writeAsOf, func(
			ctx context.Context, txn *kv.Txn, _ *descs.Collection,
		) error {
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
		log.Infof(ctx, "persisting target safe write time %v...", writeAsOf)
		if err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			details := sc.job.Details().(jobspb.SchemaChangeDetails)
			details.WriteTimestamp = writeAsOf
			return sc.job.SetDetails(ctx, txn, details)
		}); err != nil {
			return err
		}
		if err := sc.job.RunningStatus(ctx, nil /* txn */, func(_ context.Context, _ jobspb.Details) (jobs.RunningStatus, error) {
			return RunningStatusBackfill, nil
		}); err != nil {
			return errors.Wrapf(err, "failed to update running status of job %d", errors.Safe(sc.job.ID()))
		}
	} else {
		log.Infof(ctx, "writing at persisted safe write time %v...", writeAsOf)
	}

	readAsOf := sc.clock.Now()

	var p *PhysicalPlan
	var evalCtx extendedEvalContext
	var planCtx *PlanningCtx
	// The txn is used to fetch a tableDesc, partition the spans and set the
	// evalCtx ts all of which is during planning of the DistSQL flow.
	if err := sc.txn(ctx, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {

		// It is okay to release the lease on the descriptor before running the
		// index backfill flow because any schema change that would invalidate the
		// index being backfilled, would be queued behind the backfill in the
		// mutations slice.
		// NB: There are tradeoffs to holding the lease throughout the backfill. It
		// results in disallowing certain kinds of schema changes to complete eg:
		// changing privileges. There might be a more principled solution in
		// dropping and acquiring fresh leases at regular checkpoint but it is not
		// clear what this buys us in terms of checking the descriptors validity.
		// Thus, in favor of simpler code and no correctness concerns we release
		// the lease once the flow is planned.
		tableDesc, err := sc.getTableVersion(ctx, txn, descriptors, version)
		if err != nil {
			return err
		}
		evalCtx = createSchemaChangeEvalCtx(ctx, sc.execCfg, txn.ReadTimestamp(), descriptors)
		planCtx = sc.distSQLPlanner.NewPlanningCtx(ctx, &evalCtx, nil, /* planner */
			txn, DistributionTypeSystemTenantOnly)
		indexBatchSize := indexBackfillBatchSize.Get(&sc.execCfg.Settings.SV)
		chunkSize := sc.getChunkSize(indexBatchSize)
		spec, err := initIndexBackfillerSpec(*tableDesc.TableDesc(), writeAsOf, readAsOf, writeAtRequestTimestamp, chunkSize, addedIndexes)
		if err != nil {
			return err
		}
		p, err = sc.distSQLPlanner.createBackfillerPhysicalPlan(planCtx, spec, todoSpans)
		return err
	}); err != nil {
		return err
	}

	// Processors stream back the completed spans via metadata.
	//
	// mu synchronizes reads and writes to updatedTodoSpans between the processor
	// streaming back progress and the updates to the job details/progress
	// fraction.
	mu := struct {
		syncutil.Mutex
		updatedTodoSpans []roachpb.Span
	}{}
	var updateJobProgress func() error
	var updateJobDetails func() error
	metaFn := func(_ context.Context, meta *execinfrapb.ProducerMetadata) error {
		if meta.BulkProcessorProgress != nil {
			todoSpans = roachpb.SubtractSpans(todoSpans,
				meta.BulkProcessorProgress.CompletedSpans)
			mu.Lock()
			mu.updatedTodoSpans = make([]roachpb.Span, len(todoSpans))
			copy(mu.updatedTodoSpans, todoSpans)
			mu.Unlock()

			if sc.testingKnobs.AlwaysUpdateIndexBackfillDetails {
				if err := updateJobDetails(); err != nil {
					return err
				}
			}

			if sc.testingKnobs.AlwaysUpdateIndexBackfillProgress {
				if err := updateJobProgress(); err != nil {
					return err
				}
			}
		}
		return nil
	}
	cbw := MetadataCallbackWriter{rowResultWriter: &errOnlyResultWriter{}, fn: metaFn}
	recv := MakeDistSQLReceiver(
		ctx,
		&cbw,
		tree.Rows, /* stmtType - doesn't matter here since no result are produced */
		sc.rangeDescriptorCache,
		nil, /* txn - the flow does not run wholly in a txn */
		sc.clock,
		evalCtx.Tracing,
		sc.execCfg.ContentionRegistry,
		nil, /* testingPushCallback */
	)
	defer recv.Release()

	getTodoSpansForUpdate := func() []roachpb.Span {
		mu.Lock()
		defer mu.Unlock()
		if mu.updatedTodoSpans == nil {
			return nil
		}
		return append(
			make([]roachpb.Span, 0, len(mu.updatedTodoSpans)),
			mu.updatedTodoSpans...,
		)
	}
	updateJobProgress = func() error {
		// Report schema change progress. We define progress at this point as the
		// the fraction of fully-backfilled ranges of the primary index of the
		// table being scanned. Since we may have already modified the fraction
		// completed of our job from the 10% allocated to completing the schema
		// change state machine or from a previous backfill attempt, we scale that
		// fraction of ranges completed by the remaining fraction of the job's
		// progress bar.
		updatedTodoSpans := getTodoSpansForUpdate()
		if updatedTodoSpans == nil {
			return nil
		}
		nRanges, err := numRangesInSpans(ctx, sc.db, sc.distSQLPlanner, updatedTodoSpans)
		if err != nil {
			return err
		}
		if origNRanges == -1 {
			origNRanges = nRanges
		}
		return sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			// No processor has returned completed spans yet.
			if nRanges < origNRanges {
				fractionRangesFinished := float32(origNRanges-nRanges) / float32(origNRanges)
				fractionCompleted := origFractionCompleted + fractionLeft*fractionRangesFinished
				if err := sc.job.FractionProgressed(ctx, txn,
					jobs.FractionUpdater(fractionCompleted)); err != nil {
					return jobs.SimplifyInvalidStatusError(err)
				}
			}
			return nil
		})
	}

	// updateJobMu ensures only one goroutine is calling
	// updateJobDetails at a time to avoid a data race in
	// SetResumeSpansInJob. This mutex should be uncontended when
	// sc.testingKnobs.AlwaysUpdateIndexBackfillDetails is false.
	var updateJobMu syncutil.Mutex
	updateJobDetails = func() error {
		updatedTodoSpans := getTodoSpansForUpdate()
		return sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			updateJobMu.Lock()
			defer updateJobMu.Unlock()
			// No processor has returned completed spans yet.
			if updatedTodoSpans == nil {
				return nil
			}
			log.VEventf(ctx, 2, "writing todo spans to job details: %+v", updatedTodoSpans)
			return rowexec.SetResumeSpansInJob(ctx, updatedTodoSpans, mutationIdx, txn, sc.job)
		})
	}

	// Setup periodic progress update.
	stopProgress := make(chan struct{})
	duration := 10 * time.Second
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		tick := time.NewTicker(duration)
		defer tick.Stop()
		done := ctx.Done()
		for {
			select {
			case <-stopProgress:
				return nil
			case <-done:
				return ctx.Err()
			case <-tick.C:
				if err := updateJobProgress(); err != nil {
					return err
				}
			}
		}
	})

	// Setup periodic job details update.
	stopJobDetailsUpdate := make(chan struct{})
	detailsDuration := backfill.IndexBackfillCheckpointInterval.Get(&sc.settings.SV)
	g.GoCtx(func(ctx context.Context) error {
		tick := time.NewTicker(detailsDuration)
		defer tick.Stop()
		done := ctx.Done()
		for {
			select {
			case <-stopJobDetailsUpdate:
				return nil
			case <-done:
				return ctx.Err()
			case <-tick.C:
				if err := updateJobDetails(); err != nil {
					return err
				}
			}
		}
	})

	// Run index backfill physical plan.
	g.GoCtx(func(ctx context.Context) error {
		defer close(stopProgress)
		defer close(stopJobDetailsUpdate)
		if err := sc.jobRegistry.CheckPausepoint("indexbackfill.before_flow"); err != nil {
			return err
		}
		// Copy the evalCtx, as dsp.Run() might change it.
		evalCtxCopy := evalCtx
		sc.distSQLPlanner.Run(
			planCtx,
			nil, /* txn - the processors manage their own transactions */
			p, recv, &evalCtxCopy,
			nil, /* finishedSetupFn */
		)()
		return cbw.Err()
	})

	if err := g.Wait(); err != nil {
		return err
	}

	// Update progress and details to mark a completed job.
	if err := updateJobDetails(); err != nil {
		return err
	}
	if err := updateJobProgress(); err != nil {
		return err
	}

	return nil
}

// distColumnBackfill runs (or continues) a backfill for the first mutation
// enqueued on the SchemaChanger's table descriptor that passes the input
// MutationFilter.
//
// This operates over multiple goroutines concurrently and is thus not
// able to reuse the original kv.Txn safely, so it makes its own.
func (sc *SchemaChanger) distColumnBackfill(
	ctx context.Context,
	version descpb.DescriptorVersion,
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
	// Gather the initial resume spans for the table.
	var todoSpans []roachpb.Span
	var mutationIdx int
	if err := DescsTxn(ctx, sc.execCfg, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) (err error) {
		todoSpans, _, mutationIdx, err = rowexec.GetResumeSpans(
			ctx, sc.jobRegistry, txn, sc.execCfg.Codec, col, sc.descID, sc.mutationID, filter)
		return err
	}); err != nil {
		return err
	}

	for len(todoSpans) > 0 {
		log.VEventf(ctx, 2, "backfill: process %+v spans", todoSpans)
		// Make sure not to update todoSpans inside the transaction closure as it
		// may not commit. Instead write the updated value for todoSpans to this
		// variable and assign to todoSpans after committing.
		var updatedTodoSpans []roachpb.Span
		if err := sc.txn(ctx, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
		) error {
			updatedTodoSpans = todoSpans
			// Report schema change progress. We define progress at this point
			// as the fraction of fully-backfilled ranges of the primary index of
			// the table being scanned. Since we may have already modified the
			// fraction completed of our job from the 10% allocated to completing the
			// schema change state machine or from a previous backfill attempt,
			// we scale that fraction of ranges completed by the remaining fraction
			// of the job's progress bar.
			nRanges, err := numRangesInSpans(ctx, sc.db, sc.distSQLPlanner, todoSpans)
			if err != nil {
				return err
			}
			if origNRanges == -1 {
				origNRanges = nRanges
			}

			if nRanges < origNRanges {
				fractionRangesFinished := float32(origNRanges-nRanges) / float32(origNRanges)
				fractionCompleted := origFractionCompleted + fractionLeft*fractionRangesFinished
				if err := sc.job.FractionProgressed(ctx, txn, jobs.FractionUpdater(fractionCompleted)); err != nil {
					return jobs.SimplifyInvalidStatusError(err)
				}
			}

			tableDesc, err := sc.getTableVersion(ctx, txn, descriptors, version)
			if err != nil {
				return err
			}
			metaFn := func(_ context.Context, meta *execinfrapb.ProducerMetadata) error {
				if meta.BulkProcessorProgress != nil {
					updatedTodoSpans = roachpb.SubtractSpans(updatedTodoSpans,
						meta.BulkProcessorProgress.CompletedSpans)
				}
				return nil
			}
			cbw := MetadataCallbackWriter{rowResultWriter: &errOnlyResultWriter{}, fn: metaFn}
			evalCtx := createSchemaChangeEvalCtx(ctx, sc.execCfg, txn.ReadTimestamp(), descriptors)
			recv := MakeDistSQLReceiver(
				ctx,
				&cbw,
				tree.Rows, /* stmtType - doesn't matter here since no result are produced */
				sc.rangeDescriptorCache,
				nil, /* txn - the flow does not run wholly in a txn */
				sc.clock,
				evalCtx.Tracing,
				sc.execCfg.ContentionRegistry,
				nil, /* testingPushCallback */
			)
			defer recv.Release()

			planCtx := sc.distSQLPlanner.NewPlanningCtx(ctx, &evalCtx, nil /* planner */, txn,
				DistributionTypeSystemTenantOnly)
			spec, err := initColumnBackfillerSpec(*tableDesc.TableDesc(), duration, chunkSize, readAsOf)
			if err != nil {
				return err
			}
			plan, err := sc.distSQLPlanner.createBackfillerPhysicalPlan(planCtx, spec, todoSpans)
			if err != nil {
				return err
			}
			sc.distSQLPlanner.Run(
				planCtx,
				nil, /* txn - the processors manage their own transactions */
				plan, recv, &evalCtx,
				nil, /* finishedSetupFn */
			)()
			return cbw.Err()
		}); err != nil {
			return err
		}
		todoSpans = updatedTodoSpans

		// Record what is left to do for the job.
		// TODO(spaskob): Execute this at a regular cadence.
		if err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return rowexec.SetResumeSpansInJob(ctx, todoSpans, mutationIdx, txn, sc.job)
		}); err != nil {
			return err
		}
	}
	return nil
}

// updateJobRunningStatus updates the status field in the job entry
// with the given value.
//
// The update is performed in a separate txn at the current logical
// timestamp.
// TODO(ajwerner): Fix the transaction and descriptor lifetimes here.
func (sc *SchemaChanger) updateJobRunningStatus(
	ctx context.Context, status jobs.RunningStatus,
) (tableDesc catalog.TableDescriptor, err error) {
	err = DescsTxn(ctx, sc.execCfg, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) (err error) {
		// Read table descriptor without holding a lease.
		tableDesc, err = col.Direct().MustGetTableDescByID(ctx, txn, sc.descID)
		if err != nil {
			return err
		}

		// Update running status of job.
		updateJobRunningProgress := false
		for _, mutation := range tableDesc.AllMutations() {
			if mutation.MutationID() != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}

			if mutation.Adding() && mutation.WriteAndDeleteOnly() {
				updateJobRunningProgress = true
			} else if mutation.Dropped() && mutation.DeleteOnly() {
				updateJobRunningProgress = true
			}
		}
		if updateJobRunningProgress && !tableDesc.Dropped() {
			if err := sc.job.RunningStatus(ctx, txn, func(
				ctx context.Context, details jobspb.Details) (jobs.RunningStatus, error) {
				return status, nil
			}); err != nil {
				return errors.Wrapf(err, "failed to update running status of job %d", errors.Safe(sc.job.ID()))
			}
		}
		return nil
	})
	return tableDesc, err
}

// validateIndexes checks that the new indexes have entries for all the rows.
//
// This operates over multiple goroutines concurrently and is thus not
// able to reuse the original kv.Txn safely, so it makes its own.
func (sc *SchemaChanger) validateIndexes(ctx context.Context) error {
	if lease.TestingTableLeasesAreDisabled() {
		return nil
	}
	log.Info(ctx, "validating new indexes")

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
	var tableDesc catalog.TableDescriptor
	if err := sc.fixedTimestampTxn(ctx, readAsOf, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) (err error) {
		flags := tree.ObjectLookupFlagsWithRequired()
		flags.AvoidLeased = true
		tableDesc, err = descriptors.GetImmutableTableByID(ctx, txn, sc.descID, flags)
		return err
	}); err != nil {
		return err
	}

	var forwardIndexes, invertedIndexes []catalog.Index

	for _, m := range tableDesc.AllMutations() {
		if sc.mutationID != m.MutationID() {
			break
		}
		idx := m.AsIndex()
		// NB: temporary indexes should be Dropped by the point.
		if idx == nil || idx.Dropped() || idx.IsTemporaryIndexForBackfill() {
			continue
		}
		switch idx.GetType() {
		case descpb.IndexDescriptor_FORWARD:
			forwardIndexes = append(forwardIndexes, idx)
		case descpb.IndexDescriptor_INVERTED:
			invertedIndexes = append(invertedIndexes, idx)
		}
	}
	if len(forwardIndexes) == 0 && len(invertedIndexes) == 0 {
		return nil
	}

	grp := ctxgroup.WithContext(ctx)
	runHistoricalTxn := sc.makeFixedTimestampInternalExecRunner(readAsOf)

	if len(forwardIndexes) > 0 {
		grp.GoCtx(func(ctx context.Context) error {
			return ValidateForwardIndexes(
				ctx,
				tableDesc,
				forwardIndexes,
				runHistoricalTxn,
				true,  /* withFirstMutationPubic */
				false, /* gatherAllInvalid */
				sessiondata.InternalExecutorOverride{},
			)
		})
	}
	if len(invertedIndexes) > 0 {
		grp.GoCtx(func(ctx context.Context) error {
			return ValidateInvertedIndexes(
				ctx,
				sc.execCfg.Codec,
				tableDesc,
				invertedIndexes,
				runHistoricalTxn,
				true,  /* withFirstMutationPublic */
				false, /* gatherAllInvalid */
				sessiondata.InternalExecutorOverride{},
			)
		})
	}
	if err := grp.Wait(); err != nil {
		return err
	}
	log.Info(ctx, "finished validating new indexes")
	return nil
}

// InvalidIndexesError is used to represent indexes that failed revalidation.
type InvalidIndexesError struct {
	Indexes []descpb.IndexID
}

func (e InvalidIndexesError) Error() string {
	return fmt.Sprintf("found %d invalid indexes", len(e.Indexes))
}

// ValidateInvertedIndexes checks that the indexes have entries for
// all the items of data in rows.
//
// This operates over multiple goroutines concurrently and is thus not
// able to reuse the original kv.Txn safely.
// Instead it uses the provided runHistoricalTxn which can operate
// at the historical fixed timestamp for checks.
func ValidateInvertedIndexes(
	ctx context.Context,
	codec keys.SQLCodec,
	tableDesc catalog.TableDescriptor,
	indexes []catalog.Index,
	runHistoricalTxn sqlutil.HistoricalInternalExecTxnRunner,
	withFirstMutationPublic bool,
	gatherAllInvalid bool,
	execOverride sessiondata.InternalExecutorOverride,
) error {
	grp := ctxgroup.WithContext(ctx)
	invalid := make(chan descpb.IndexID, len(indexes))

	expectedCount := make([]int64, len(indexes))
	countReady := make([]chan struct{}, len(indexes))

	for i, idx := range indexes {
		// Shadow i and idx to prevent the values from changing within each
		// gorountine.
		i, idx := i, idx
		countReady[i] = make(chan struct{})

		grp.GoCtx(func(ctx context.Context) error {
			// KV scan can be used to get the index length.
			// TODO (lucy): Switch to using DistSQL to get the count, so that we get
			// distributed execution and avoid bypassing the SQL decoding
			start := timeutil.Now()
			var idxLen int64
			span := tableDesc.IndexSpan(codec, idx.GetID())
			key := span.Key
			endKey := span.EndKey
			if err := runHistoricalTxn(ctx, func(ctx context.Context, txn *kv.Txn, _ sqlutil.InternalExecutor) error {
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
				return nil
			}); err != nil {
				return err
			}
			log.Infof(ctx, "inverted index %s/%s count = %d, took %s",
				tableDesc.GetName(), idx.GetName(), idxLen, timeutil.Since(start))
			select {
			case <-countReady[i]:
				if idxLen != expectedCount[i] {
					if gatherAllInvalid {
						invalid <- idx.GetID()
						return nil
					}
					// JSON columns cannot have unique indexes, so if the expected and
					// actual counts do not match, it's always a bug rather than a
					// uniqueness violation.
					return errors.AssertionFailedf(
						"validation of index %s failed: expected %d rows, found %d",
						idx.GetName(), errors.Safe(expectedCount[i]), errors.Safe(idxLen))
				}
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})

		grp.GoCtx(func(ctx context.Context) error {
			c, err := countExpectedRowsForInvertedIndex(ctx, tableDesc, idx, runHistoricalTxn, withFirstMutationPublic, execOverride)
			if err != nil {
				return err
			}
			expectedCount[i] = c
			close(countReady[i])
			return nil
		})
	}

	if err := grp.Wait(); err != nil {
		return err
	}
	close(invalid)
	invalidErr := InvalidIndexesError{}
	for i := range invalid {
		invalidErr.Indexes = append(invalidErr.Indexes, i)
	}
	if len(invalidErr.Indexes) > 0 {
		return invalidErr
	}
	return nil
}

func countExpectedRowsForInvertedIndex(
	ctx context.Context,
	tableDesc catalog.TableDescriptor,
	idx catalog.Index,
	runHistoricalTxn sqlutil.HistoricalInternalExecTxnRunner,
	withFirstMutationPublic bool,
	execOverride sessiondata.InternalExecutorOverride,
) (int64, error) {
	desc := tableDesc
	start := timeutil.Now()
	if withFirstMutationPublic {
		// Make the mutations public in an in-memory copy of the descriptor and
		// add it to the Collection's synthetic descriptors, so that we can use
		// SQL below to perform the validation.
		fakeDesc, err := tableDesc.MakeFirstMutationPublic(catalog.IgnoreConstraints)
		if err != nil {
			return 0, err
		}
		desc = fakeDesc
	}

	colID := idx.InvertedColumnID()
	col, err := desc.FindColumnWithID(colID)
	if err != nil {
		return 0, err
	}

	// colNameOrExpr is the column or expression indexed by the inverted
	// index. It is used in the query below that verifies that the
	// number of entries in the inverted index is correct. If the index
	// is an expression index, the column name cannot be used because it
	// is inaccessible; the query would result in a "column does not
	// exist" error.
	var colNameOrExpr string
	if col.IsExpressionIndexColumn() {
		colNameOrExpr = col.GetComputeExpr()
	} else {
		// Wrap the column name in double-quotes because it might
		// contain special characters, like "-".
		colNameOrExpr = fmt.Sprintf("%q", col.ColName())
	}

	var expectedCount int64
	if err := runHistoricalTxn(ctx, func(ctx context.Context, txn *kv.Txn, ie sqlutil.InternalExecutor) error {
		var stmt string
		geoConfig := idx.GetGeoConfig()
		if geoindex.IsEmptyConfig(&geoConfig) {
			stmt = fmt.Sprintf(
				`SELECT coalesce(sum_int(crdb_internal.num_inverted_index_entries(%s, %d)), 0) FROM [%d AS t]`,
				colNameOrExpr, idx.GetVersion(), desc.GetID(),
			)
		} else {
			stmt = fmt.Sprintf(
				`SELECT coalesce(sum_int(crdb_internal.num_geo_inverted_index_entries(%d, %d, %s)), 0) FROM [%d AS t]`,
				desc.GetID(), idx.GetID(), colNameOrExpr, desc.GetID(),
			)
		}
		// If the index is a partial index the predicate must be added
		// as a filter to the query.
		if idx.IsPartial() {
			stmt = fmt.Sprintf(`%s WHERE %s`, stmt, idx.GetPredicate())
		}
		return ie.WithSyntheticDescriptors([]catalog.Descriptor{desc}, func() error {
			row, err := ie.QueryRowEx(ctx, "verify-inverted-idx-count", txn, execOverride, stmt)
			if err != nil {
				return err
			}
			if row == nil {
				return errors.New("failed to verify inverted index count")
			}
			expectedCount = int64(tree.MustBeDInt(row[0]))
			return nil
		})
	}); err != nil {
		return 0, err
	}
	log.Infof(ctx, "%s %s expected inverted index count = %d, took %s",
		desc.GetName(), colNameOrExpr, expectedCount, timeutil.Since(start))
	return expectedCount, nil

}

// ValidateForwardIndexes checks that the indexes have entries for all the rows.
//
// This operates over multiple goroutines concurrently and is thus not
// able to reuse the original kv.Txn safely.
// Instead it uses the provided runHistoricalTxn which can operate
// at the historical fixed timestamp for checks. Typically it fails as soon as
// any index fails validation as this usually means the schema change should
// rollback. However, if gatherAllInvalid is true, it instead accumulates all
// the indexes which fail and returns them together.
// withFirstMutationPublic should be set to true if we are validating and assuming
// the first mutation is made public. This should be used when finalizing a schema
// change after a backfill.
func ValidateForwardIndexes(
	ctx context.Context,
	tableDesc catalog.TableDescriptor,
	indexes []catalog.Index,
	runHistoricalTxn sqlutil.HistoricalInternalExecTxnRunner,
	withFirstMutationPublic bool,
	gatherAllInvalid bool,
	execOverride sessiondata.InternalExecutorOverride,
) error {
	grp := ctxgroup.WithContext(ctx)

	invalid := make(chan descpb.IndexID, len(indexes))
	var tableRowCount int64
	partialIndexExpectedCounts := make(map[descpb.IndexID]int64, len(indexes))

	// Close when table count is ready.
	tableCountsReady := make(chan struct{})

	// Compute the size of each index.
	for _, idx := range indexes {
		// Shadow idx to prevent its value from changing within each gorountine.
		idx := idx

		grp.GoCtx(func(ctx context.Context) error {
			start := timeutil.Now()
			idxLen, err := countIndexRowsAndMaybeCheckUniqueness(ctx, tableDesc, idx, withFirstMutationPublic, runHistoricalTxn, execOverride)
			if err != nil {
				return err
			}
			log.Infof(ctx, "validation: index %s/%s row count = %d, time so far %s",
				tableDesc.GetName(), idx.GetName(), idxLen, timeutil.Since(start))

			// Now compare with the row count in the table.
			select {
			case <-tableCountsReady:
				expectedCount := tableRowCount
				// If the index is a partial index, the expected number of rows
				// is different than the total number of rows in the table.
				if idx.IsPartial() {
					expectedCount = partialIndexExpectedCounts[idx.GetID()]
				}

				if idxLen != expectedCount {
					if gatherAllInvalid {
						invalid <- idx.GetID()
						return nil
					}
					// TODO(vivek): find the offending row and include it in the error.
					return pgerror.WithConstraintName(pgerror.Newf(pgcode.UniqueViolation,
						"duplicate key value violates unique constraint %q",
						idx.GetName()),
						idx.GetName())

				}
			case <-ctx.Done():
				return ctx.Err()
			}

			return nil
		})
	}

	grp.GoCtx(func(ctx context.Context) error {
		start := timeutil.Now()
		c, err := populateExpectedCounts(ctx, tableDesc, indexes, partialIndexExpectedCounts, withFirstMutationPublic, runHistoricalTxn, execOverride)
		if err != nil {
			return err
		}
		log.Infof(ctx, "validation: table %s row count = %d, took %s",
			tableDesc.GetName(), c, timeutil.Since(start))
		tableRowCount = c
		defer close(tableCountsReady)
		return nil
	})

	if err := grp.Wait(); err != nil {
		return err
	}
	close(invalid)
	invalidErr := InvalidIndexesError{}
	for i := range invalid {
		invalidErr.Indexes = append(invalidErr.Indexes, i)
	}
	if len(invalidErr.Indexes) > 0 {
		return invalidErr
	}
	return nil
}

// populateExpectedCounts returns the row count for the primary index
// of the given table and, for each partial index, populates the given
// map.
func populateExpectedCounts(
	ctx context.Context,
	tableDesc catalog.TableDescriptor,
	indexes []catalog.Index,
	partialIndexExpectedCounts map[descpb.IndexID]int64,
	withFirstMutationPublic bool,
	runHistoricalTxn sqlutil.HistoricalInternalExecTxnRunner,
	execOverride sessiondata.InternalExecutorOverride,
) (int64, error) {
	desc := tableDesc
	if withFirstMutationPublic {
		// The query to count the expected number of rows can reference columns
		// added earlier in the same mutation. Make the mutations public in an
		// in-memory copy of the descriptor and add it to the Collection's synthetic
		// descriptors, so that we can use SQL below to perform the validation.
		fakeDesc, err := tableDesc.MakeFirstMutationPublic(catalog.IgnoreConstraintsAndPKSwaps)
		if err != nil {
			return 0, err
		}
		desc = fakeDesc
	}
	var tableRowCount int64
	if err := runHistoricalTxn(ctx, func(ctx context.Context, txn *kv.Txn, ie sqlutil.InternalExecutor) error {
		var s strings.Builder
		for _, idx := range indexes {
			// For partial indexes, count the number of rows in the table
			// for which the predicate expression evaluates to true.
			if idx.IsPartial() {
				s.WriteString(fmt.Sprintf(`, count(1) FILTER (WHERE %s)`, idx.GetPredicate()))
			}
		}
		partialIndexCounts := s.String()

		// Force the primary index so that the optimizer does not create a
		// query plan that uses the indexes being backfilled.
		query := fmt.Sprintf(`SELECT count(1)%s FROM [%d AS t]@[%d]`, partialIndexCounts, desc.GetID(), desc.GetPrimaryIndexID())

		return ie.WithSyntheticDescriptors([]catalog.Descriptor{desc}, func() error {
			cnt, err := ie.QueryRowEx(ctx, "VERIFY INDEX", txn, execOverride, query)
			if err != nil {
				return err
			}
			if cnt == nil {
				return errors.New("failed to verify index")
			}

			tableRowCount = int64(tree.MustBeDInt(cnt[0]))
			cntIdx := 1
			for _, idx := range indexes {
				if idx.IsPartial() {
					partialIndexExpectedCounts[idx.GetID()] = int64(tree.MustBeDInt(cnt[cntIdx]))
					cntIdx++
				}
			}

			return nil
		})
	}); err != nil {
		return 0, err
	}
	return tableRowCount, nil
}

func countIndexRowsAndMaybeCheckUniqueness(
	ctx context.Context,
	tableDesc catalog.TableDescriptor,
	idx catalog.Index,
	withFirstMutationPublic bool,
	runHistoricalTxn sqlutil.HistoricalInternalExecTxnRunner,
	execOverride sessiondata.InternalExecutorOverride,
) (int64, error) {
	// If we are doing a REGIONAL BY ROW locality change, we can
	// bypass the uniqueness check below as we are only adding or
	// removing an implicit partitioning column.  Scan the
	// mutations if we're assuming the first mutation to be public
	// to see if we have a locality config swap.
	skipUniquenessChecks := false
	if withFirstMutationPublic {
		mutations := tableDesc.AllMutations()
		if len(mutations) > 0 {
			mutationID := mutations[0].MutationID()
			for _, mut := range tableDesc.AllMutations() {
				// We only want to check the first mutation, so break
				// if we detect a new one.
				if mut.MutationID() != mutationID {
					break
				}
				if pkSwap := mut.AsPrimaryKeySwap(); pkSwap != nil {
					if lcSwap := pkSwap.PrimaryKeySwapDesc().LocalityConfigSwap; lcSwap != nil {
						if lcSwap.OldLocalityConfig.GetRegionalByRow() != nil ||
							lcSwap.NewLocalityConfig.GetRegionalByRow() != nil {
							skipUniquenessChecks = true
							break
						}
					}
				}
			}
		}
	}

	desc := tableDesc
	if withFirstMutationPublic {
		// Make the mutations public in an in-memory copy of the descriptor and
		// add it to the Collection's synthetic descriptors, so that we can use
		// SQL below to perform the validation.
		fakeDesc, err := tableDesc.MakeFirstMutationPublic(catalog.IgnoreConstraints)
		if err != nil {
			return 0, err
		}
		desc = fakeDesc
	}

	// Retrieve the row count in the index.
	var idxLen int64
	if err := runHistoricalTxn(ctx, func(ctx context.Context, txn *kv.Txn, ie sqlutil.InternalExecutor) error {
		query := fmt.Sprintf(`SELECT count(1) FROM [%d AS t]@[%d]`, desc.GetID(), idx.GetID())
		// If the index is a partial index the predicate must be added
		// as a filter to the query to force scanning the index.
		if idx.IsPartial() {
			query = fmt.Sprintf(`%s WHERE %s`, query, idx.GetPredicate())
		}
		return ie.WithSyntheticDescriptors([]catalog.Descriptor{desc}, func() error {
			row, err := ie.QueryRowEx(ctx, "verify-idx-count", txn, execOverride, query)
			if err != nil {
				return err
			}
			if row == nil {
				return errors.New("failed to verify index count")
			}
			idxLen = int64(tree.MustBeDInt(row[0]))

			// For implicitly partitioned unique indexes, we need to independently
			// validate that the non-implicitly partitioned columns are unique.
			if idx.IsUnique() && idx.GetPartitioning().NumImplicitColumns() > 0 && !skipUniquenessChecks {
				if err := validateUniqueConstraint(
					ctx,
					tableDesc,
					idx.GetName(),
					idx.IndexDesc().KeyColumnIDs[idx.GetPartitioning().NumImplicitColumns():],
					idx.GetPredicate(),
					ie,
					txn,
					false, /* preExisting */
				); err != nil {
					return err
				}
			}
			return nil
		})
	}); err != nil {
		return 0, err
	}
	return idxLen, nil
}

// backfillIndexes fills the missing columns in the indexes of the
// leased tables.
//
//
// If temporaryIndexes is non-empty, we assume that we are using the
// MVCC-compatible backfilling process. This mutation has already been
// checked to ensure all newly added indexes are using one type of
// index backfill.
//
// The MVCC-compatible index backfilling process has a goal of not
// having to issue AddSStable requests with backdated timestamps.
//
// To do this, we backfill new indexes while they are in a BACKFILLING
// state in which they do not see writes or deletes. While the
// backfill is running a temporary index captures all inflight rights.
//
// When the backfill is completed, the backfilling index is stepped up
// to MERGING and then writes and deletes missed during
// the backfill are merged from the temporary index.
//
// Finally, the new index is brought into the DELETE_AND_WRITE_ONLY
// state for validation.
//
//           ┌─────────────────┐         ┌─────────────────┐             ┌─────────────────┐
//           │                 │         │                 │             │                 │
//           │   PrimaryIndex  │         │     NewIndex    │             │    TempIndex    │
// t0        │     (PUBLIC)    │         │  (BACKFILLING)  │             │  (DELETE_ONLY)  │
//           │                 │         │                 │             │                 │
//           └─────────────────┘         └─────────────────┘             └────────┬────────┘
//                                                                                │
//                                                                       ┌────────▼────────┐
//                                                                       │                 │
//                                                                       │    TempIndex    │
// t1                                                                    │(DELETE_AND_WRITE)   │
//                                                                       │                 │   │
//                                                                       └────────┬────────┘   │
//                                                                                │            │
//           ┌─────────────────┐         ┌─────────────────┐             ┌────────▼────────┐   │ TempIndex receiving writes
//           │                 │         │                 │             │                 │   │
//           │  PrimaryIndex   ├────────►│     NewIndex    │             │    TempIndex    │   │
// t2        │   (PUBLIC)      │ Backfill│  (BACKFILLING)  │             │(DELETE_AND_WRITE│   │
//           │                 │         │                 │             │                 │   │
//           └─────────────────┘         └────────┬────────┘             └─────────────────┘   │
//                                                │                                            │
//                                       ┌────────▼────────┐                                   │
//                                       │                 │                                   │
//                                       │     NewIndex    │                                   │
// t3                                    │  (DELETE_ONLY)  │                                   │
//                                       │                 │                                   │
//                                       └────────┬────────┘                                   │
//                                                │                                            │
//                                       ┌────────▼────────┐                                   │
//                                       │                 │                                   │
//                                       │     NewIndex    │                                   │   │
//                                       │    (MERGING)    │                                   │   │
// t4                                    │                 │                                   │   │ NewIndex receiving writes
//                                       └─────────────────┘                                   │   │
//                                                                                             │   │
//                                       ┌─────────────────┐             ┌─────────────────┐   │   │
//                                       │                 │             │                 │   │   │
//                                       │     NewIndex    │◄────────────┤    TempIndex    │   │   │
// t5                                    │    (MERGING)    │  BatchMerge │(DELETE_AND_WRITE│   │   │
//                                       │                 │             │                 │   │   │
//                                       └────────┬────────┘             └───────┬─────────┘   │   │
//                                                │                              │             │   │
//                                       ┌────────▼────────┐             ┌───────▼─────────┐   │   │
//                                       │                 │             │                 │   │   │
//                                       │     NewIndex    │             │    TempIndex    │       │
// t6                                    │(DELETE_AND_WRITE)             │  (DELETE_ONLY)  │       │
//                                       │                 │             │                 │       │
//                                       └───────┬─────────┘             └───────┬─────────┘       │
//                                               │                               │
//                                               │                               │
//                                               ▼                               ▼
//                                    [validate and make public]             [ dropped ]
//
// This operates over multiple goroutines concurrently and is thus not
// able to reuse the original kv.Txn safely.
func (sc *SchemaChanger) backfillIndexes(
	ctx context.Context,
	version descpb.DescriptorVersion,
	addingSpans []roachpb.Span,
	addedIndexes []descpb.IndexID,
	temporaryIndexes []descpb.IndexID,
) error {
	// If temporary indexes is non-empty, we want a MVCC-compliant
	// backfill. If it is empty, we assume this is an older schema
	// change using the non-MVCC-compliant flow.
	writeAtRequestTimestamp := len(temporaryIndexes) != 0
	log.Infof(ctx, "backfilling %d indexes: %v (writeAtRequestTimestamp: %v)", len(addingSpans), addingSpans, writeAtRequestTimestamp)

	// Split off a new range for each new index span. But only do so for the
	// system tenant. Secondary tenants do not have mandatory split points
	// between tables or indexes.
	if sc.execCfg.Codec.ForSystemTenant() {
		expirationTime := sc.db.Clock().Now().Add(time.Hour.Nanoseconds(), 0)
		for _, span := range addingSpans {
			if err := sc.db.AdminSplit(ctx, span.Key, expirationTime); err != nil {
				return err
			}
		}
	}

	if fn := sc.testingKnobs.RunBeforeIndexBackfill; fn != nil {
		fn()
	}

	// NB: The index backfilling process and index merging process
	// use different ResumeSpans to track their progress, so it is
	// safe to pass addedIndexes here even if the merging has
	// already started.
	if err := sc.distIndexBackfill(
		ctx, version, addingSpans, addedIndexes, writeAtRequestTimestamp, backfill.IndexMutationFilter,
	); err != nil {
		return err
	}

	if writeAtRequestTimestamp {
		if fn := sc.testingKnobs.RunBeforeTempIndexMerge; fn != nil {
			fn()
		}

		// Steps backfilled adding indexes from BACKFILLING to
		// MERGING.
		if err := sc.RunStateMachineAfterIndexBackfill(ctx); err != nil {
			return err
		}

		if err := sc.mergeFromTemporaryIndex(ctx, version, addedIndexes, temporaryIndexes); err != nil {
			return err
		}

		if fn := sc.testingKnobs.RunAfterTempIndexMerge; fn != nil {
			fn()
		}

		if err := sc.runStateMachineAfterTempIndexMerge(ctx); err != nil {
			return err
		}
	}

	if fn := sc.testingKnobs.RunAfterIndexBackfill; fn != nil {
		fn()
	}

	log.Info(ctx, "finished backfilling indexes")
	return sc.validateIndexes(ctx)
}

func (sc *SchemaChanger) mergeFromTemporaryIndex(
	ctx context.Context,
	version descpb.DescriptorVersion,
	addingIndexes []descpb.IndexID,
	temporaryIndexes []descpb.IndexID,
) error {
	var tbl *tabledesc.Mutable
	if err := sc.txn(ctx, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		var err error
		tbl, err = descsCol.GetMutableTableVersionByID(ctx, sc.descID, txn)
		return err
	}); err != nil {
		return err
	}
	tableDesc := tabledesc.NewBuilder(&tbl.ClusterVersion).BuildImmutableTable()
	if err := sc.distIndexMerge(ctx, tableDesc, addingIndexes, temporaryIndexes); err != nil {
		return err
	}
	return nil
}

// runStateMachineAfterTempIndexMerge steps any DELETE_AND_WRITE_ONLY
// temporary indexes to DELETE_ONLY and changes their direction to
// DROP and steps any MERGING indexes to DELETE_AND_WRITE_ONLY
func (sc *SchemaChanger) runStateMachineAfterTempIndexMerge(ctx context.Context) error {
	var runStatus jobs.RunningStatus
	return sc.txn(ctx, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		tbl, err := descsCol.GetMutableTableVersionByID(ctx, sc.descID, txn)
		if err != nil {
			return err
		}
		runStatus = ""
		// Apply mutations belonging to the same version.
		for _, m := range tbl.AllMutations() {
			if m.MutationID() != sc.mutationID {
				// Mutations are applied in a FIFO order. Only apply the first set of
				// mutations if they have the mutation ID we're looking for.
				break
			}
			idx := m.AsIndex()
			if idx == nil {
				// Don't touch anything but indexes.
				continue
			}

			if idx.IsTemporaryIndexForBackfill() && m.Adding() && m.WriteAndDeleteOnly() {
				log.Infof(ctx, "dropping temporary index: %d", idx.IndexDesc().ID)
				tbl.Mutations[m.MutationOrdinal()].State = descpb.DescriptorMutation_DELETE_ONLY
				tbl.Mutations[m.MutationOrdinal()].Direction = descpb.DescriptorMutation_DROP
				runStatus = RunningStatusDeleteOnly
			} else if m.Merging() {
				tbl.Mutations[m.MutationOrdinal()].State = descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
			}
		}
		if runStatus == "" || tbl.Dropped() {
			return nil
		}
		if err := descsCol.WriteDesc(
			ctx, true /* kvTrace */, tbl, txn,
		); err != nil {
			return err
		}
		if sc.job != nil {
			if err := sc.job.RunningStatus(ctx, txn, func(
				ctx context.Context, details jobspb.Details,
			) (jobs.RunningStatus, error) {
				return runStatus, nil
			}); err != nil {
				return errors.Wrap(err, "failed to update job status")
			}
		}
		return nil
	})
}

// truncateAndBackfillColumns performs the backfill operation on the given leased
// table descriptors.
//
// This operates over multiple goroutines concurrently and is thus not
// able to reuse the original kv.Txn safely.
func (sc *SchemaChanger) truncateAndBackfillColumns(
	ctx context.Context, version descpb.DescriptorVersion,
) error {
	log.Infof(ctx, "clearing and backfilling columns")

	if err := sc.distColumnBackfill(
		ctx, version, columnBackfillBatchSize.Get(&sc.settings.SV),
		backfill.ColumnMutationFilter); err != nil {
		return err
	}
	log.Info(ctx, "finished clearing and backfilling columns")
	return nil
}

// runSchemaChangesInTxn runs all the schema changes immediately in a
// transaction. This is called when a CREATE TABLE is followed by
// schema changes in the same transaction. The CREATE TABLE is
// invisible to the rest of the cluster, so the schema changes
// can be executed immediately on the same version of the table.
//
// It operates entirely on the current goroutine and is thus able to
// reuse the planner's kv.Txn safely.
func runSchemaChangesInTxn(
	ctx context.Context, planner *planner, tableDesc *tabledesc.Mutable, traceKV bool,
) error {
	// TODO(postamar): remove if-block in 22.2
	//lint:ignore SA1019 removal of deprecated method call scheduled for 22.2
	if len(tableDesc.GetDrainingNames()) > 0 {
		// Reclaim all the old names. Leave the data and descriptor
		// cleanup for later.
		//lint:ignore SA1019 removal of deprecated method call scheduled for 22.2
		for _, drain := range tableDesc.GetDrainingNames() {
			key := catalogkeys.EncodeNameKey(planner.ExecCfg().Codec, drain)
			if err := planner.Txn().Del(ctx, key); err != nil {
				return err
			}
		}
		//lint:ignore SA1019 removal of deprecated method call scheduled for 22.2
		tableDesc.SetDrainingNames(nil)
	}

	if tableDesc.Dropped() {
		return nil
	}

	// Only needed because columnBackfillInTxn() backfills
	// all column mutations.
	doneColumnBackfill := false

	// Mutations are processed in multiple steps: First we process all mutations
	// for schema changes other than adding check or FK constraints, then we
	// validate those constraints, and only after that do we process the
	// constraint mutations. We need an in-memory copy of the table descriptor
	// that contains newly added columns (since constraints being added can
	// reference them), but that doesn't contain constraints (since otherwise we'd
	// plan the query assuming the constraint holds). This is a different
	// procedure than in the schema changer for existing tables, since all the
	// "steps" in the schema change occur within the same transaction here.
	//
	// In the future it would be good to either unify the two implementations more
	// or make this in-transaction implementation more principled. We expect
	// constraint validation to be refactored and treated as a first-class concept
	// in the world of transactional schema changes.

	// Collect constraint mutations to process later.
	var constraintAdditionMutations []catalog.ConstraintToUpdate

	// We use a range loop here as the processing of some mutations
	// such as the primary key swap mutations result in queueing more
	// mutations that need to be processed.
	for _, m := range tableDesc.AllMutations() {
		// Skip mutations that get canceled by later operations
		if discarded, _ := isCurrentMutationDiscarded(tableDesc, m, m.MutationOrdinal()+1); discarded {
			continue
		}

		// Skip mutations related to temporary mutations since
		// an index creation inside a transaction doesn't use
		// the AddSSTable based backfiller.
		if idx := m.AsIndex(); idx != nil && idx.IsTemporaryIndexForBackfill() {
			continue
		}

		immutDesc := tabledesc.NewBuilder(tableDesc.TableDesc()).BuildImmutableTable()

		if m.Adding() {
			if m.AsPrimaryKeySwap() != nil || m.AsModifyRowLevelTTL() != nil {
				// Don't need to do anything here, as the call to MakeMutationComplete
				// will perform the steps for this operation.
			} else if m.AsComputedColumnSwap() != nil {
				return AlterColTypeInTxnNotSupportedErr
			} else if col := m.AsColumn(); col != nil {
				if !doneColumnBackfill && catalog.ColumnNeedsBackfill(col) {
					if err := columnBackfillInTxn(
						ctx, planner.Txn(), planner.ExecCfg(), planner.EvalContext(), planner.SemaCtx(),
						immutDesc, traceKV,
					); err != nil {
						return err
					}
					doneColumnBackfill = true
				}
			} else if idx := m.AsIndex(); idx != nil {
				if err := indexBackfillInTxn(ctx, planner.Txn(), planner.EvalContext(), planner.SemaCtx(), immutDesc, traceKV); err != nil {
					return err
				}
			} else if c := m.AsConstraint(); c != nil {
				// This is processed later. Do not proceed to MakeMutationComplete.
				constraintAdditionMutations = append(constraintAdditionMutations, c)
				continue
			} else {
				return errors.AssertionFailedf("unsupported mutation: %+v", m)
			}
		} else if m.Dropped() {
			// Drop the name and drop the associated data later.
			if col := m.AsColumn(); col != nil {
				if !doneColumnBackfill && catalog.ColumnNeedsBackfill(col) {
					if err := columnBackfillInTxn(
						ctx, planner.Txn(), planner.ExecCfg(), planner.EvalContext(), planner.SemaCtx(),
						immutDesc, traceKV,
					); err != nil {
						return err
					}
					doneColumnBackfill = true
				}
			} else if idx := m.AsIndex(); idx != nil {
				if err := indexTruncateInTxn(
					ctx, planner.Txn(), planner.ExecCfg(), planner.EvalContext(), immutDesc, idx, traceKV,
				); err != nil {
					return err
				}
			} else if c := m.AsConstraint(); c != nil {
				if c.IsCheck() || c.IsNotNull() {
					for i := range tableDesc.Checks {
						if tableDesc.Checks[i].Name == c.GetName() {
							tableDesc.Checks = append(tableDesc.Checks[:i], tableDesc.Checks[i+1:]...)
							break
						}
					}
				} else if c.IsForeignKey() {
					for i := range tableDesc.OutboundFKs {
						fk := &tableDesc.OutboundFKs[i]
						if fk.Name == c.GetName() {
							if err := planner.removeFKBackReference(ctx, tableDesc, fk); err != nil {
								return err
							}
							tableDesc.OutboundFKs = append(tableDesc.OutboundFKs[:i], tableDesc.OutboundFKs[i+1:]...)
							break
						}
					}
				} else if c.IsUniqueWithoutIndex() {
					for i := range tableDesc.UniqueWithoutIndexConstraints {
						if tableDesc.UniqueWithoutIndexConstraints[i].Name == c.GetName() {
							tableDesc.UniqueWithoutIndexConstraints = append(
								tableDesc.UniqueWithoutIndexConstraints[:i],
								tableDesc.UniqueWithoutIndexConstraints[i+1:]...,
							)
							break
						}
					}
				} else {
					return errors.AssertionFailedf("unsupported constraint type: %d", c.ConstraintToUpdateDesc().ConstraintType)
				}
			}
		}

		if err := tableDesc.MakeMutationComplete(tableDesc.Mutations[m.MutationOrdinal()]); err != nil {
			return err
		}
	}
	// Clear all the mutations except for adding constraints.
	tableDesc.Mutations = make([]descpb.DescriptorMutation, len(constraintAdditionMutations))
	for i, c := range constraintAdditionMutations {
		tableDesc.Mutations[i] = descpb.DescriptorMutation{
			Descriptor_: &descpb.DescriptorMutation_Constraint{Constraint: c.ConstraintToUpdateDesc()},
			Direction:   descpb.DescriptorMutation_ADD,
			MutationID:  c.MutationID(),
		}
		if c.DeleteOnly() {
			tableDesc.Mutations[i].State = descpb.DescriptorMutation_DELETE_ONLY
		} else if c.WriteAndDeleteOnly() {
			tableDesc.Mutations[i].State = descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
		}
	}

	// Now that the table descriptor is in a valid state with all column and index
	// mutations applied, it can be used for validating check/FK constraints.
	for _, c := range constraintAdditionMutations {
		if c.IsCheck() || c.IsNotNull() {
			check := &c.ConstraintToUpdateDesc().Check
			if check.Validity == descpb.ConstraintValidity_Validating {
				if err := validateCheckInTxn(
					ctx, &planner.semaCtx, planner.ExecCfg().InternalExecutorFactory,
					planner.SessionData(), tableDesc, planner.txn, check.Expr,
				); err != nil {
					return err
				}
				check.Validity = descpb.ConstraintValidity_Validated
			}
		} else if c.IsForeignKey() {
			// We can't support adding a validated foreign key constraint in the same
			// transaction as the CREATE TABLE statement. This would require adding
			// the backreference to the other table and then validating the constraint
			// for whatever rows were inserted into the referencing table in this
			// transaction, which requires multiple schema changer states across
			// multiple transactions.
			//
			// We could partially fix this by queuing a validation job to run post-
			// transaction. Better yet would be to absorb this into the transactional
			// schema change framework eventually.
			//
			// For now, just always add the FK as unvalidated.
			c.ConstraintToUpdateDesc().ForeignKey.Validity = descpb.ConstraintValidity_Unvalidated
		} else if c.IsUniqueWithoutIndex() {
			uwi := &c.ConstraintToUpdateDesc().UniqueWithoutIndexConstraint
			if uwi.Validity == descpb.ConstraintValidity_Validating {
				if err := validateUniqueWithoutIndexConstraintInTxn(
					ctx, planner.ExecCfg().InternalExecutor, tableDesc, planner.txn, c.GetName(),
				); err != nil {
					return err
				}
				uwi.Validity = descpb.ConstraintValidity_Validated
			}
		} else {
			return errors.AssertionFailedf("unsupported constraint type: %d", c.ConstraintToUpdateDesc().ConstraintType)
		}

	}

	// Finally, add the constraints. We bypass MakeMutationsComplete (which makes
	// certain assumptions about the state in the usual schema changer) and just
	// update the table descriptor directly.
	for _, c := range constraintAdditionMutations {
		if c.IsCheck() || c.IsNotNull() {
			tableDesc.Checks = append(tableDesc.Checks, &c.ConstraintToUpdateDesc().Check)
		} else if c.IsForeignKey() {
			fk := c.ConstraintToUpdateDesc().ForeignKey
			var referencedTableDesc *tabledesc.Mutable
			// We don't want to lookup/edit a second copy of the same table.
			selfReference := tableDesc.ID == fk.ReferencedTableID
			if selfReference {
				referencedTableDesc = tableDesc
			} else {
				lookup, err := planner.Descriptors().GetMutableTableVersionByID(ctx, fk.ReferencedTableID, planner.Txn())
				if err != nil {
					return errors.Wrapf(err, "error resolving referenced table ID %d", fk.ReferencedTableID)
				}
				referencedTableDesc = lookup
			}
			referencedTableDesc.InboundFKs = append(referencedTableDesc.InboundFKs, fk)
			tableDesc.OutboundFKs = append(tableDesc.OutboundFKs, fk)

			// Write the other table descriptor here if it's not the current table
			// we're already modifying.
			if !selfReference {
				if err := planner.writeSchemaChange(
					ctx, referencedTableDesc, descpb.InvalidMutationID,
					fmt.Sprintf("updating referenced FK table %s(%d) table %s(%d)",
						referencedTableDesc.Name, referencedTableDesc.ID, tableDesc.Name, tableDesc.ID),
				); err != nil {
					return err
				}
			}
		} else if c.IsUniqueWithoutIndex() {
			tableDesc.UniqueWithoutIndexConstraints = append(
				tableDesc.UniqueWithoutIndexConstraints, c.ConstraintToUpdateDesc().UniqueWithoutIndexConstraint,
			)
		} else {
			return errors.AssertionFailedf("unsupported constraint type: %d", c.ConstraintToUpdateDesc().ConstraintType)
		}
	}
	tableDesc.Mutations = nil
	return nil
}

// validateCheckInTxn validates check constraints within the provided
// transaction. If the provided table descriptor version is newer than the
// cluster version, it will be used in the InternalExecutor that performs the
// validation query.
//
// TODO (lucy): The special case where the table descriptor version is the same
// as the cluster version only happens because the query in VALIDATE CONSTRAINT
// still runs in the user transaction instead of a step in the schema changer.
// When that's no longer true, this function should be updated.
//
// It operates entirely on the current goroutine and is thus able to
// reuse an existing kv.Txn safely.
func validateCheckInTxn(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	ief sqlutil.SessionBoundInternalExecutorFactory,
	sessionData *sessiondata.SessionData,
	tableDesc *tabledesc.Mutable,
	txn *kv.Txn,
	checkExpr string,
) error {
	var syntheticDescs []catalog.Descriptor
	if tableDesc.Version > tableDesc.ClusterVersion.Version {
		syntheticDescs = append(syntheticDescs, tableDesc)
	}
	ie := ief(ctx, sessionData)
	return ie.WithSyntheticDescriptors(syntheticDescs, func() error {
		return validateCheckExpr(ctx, semaCtx, sessionData, checkExpr, tableDesc, ie, txn)
	})
}

// validateFkInTxn validates foreign key constraints within the provided
// transaction. If the provided table descriptor version is newer than the
// cluster version, it will be used in the InternalExecutor that performs the
// validation query.
//
// TODO (lucy): The special case where the table descriptor version is the same
// as the cluster version only happens because the query in VALIDATE CONSTRAINT
// still runs in the user transaction instead of a step in the schema changer.
// When that's no longer true, this function should be updated.
//
// It operates entirely on the current goroutine and is thus able to
// reuse an existing kv.Txn safely.
func validateFkInTxn(
	ctx context.Context,
	ief sqlutil.SessionBoundInternalExecutorFactory,
	sd *sessiondata.SessionData,
	srcTable *tabledesc.Mutable,
	txn *kv.Txn,
	descsCol *descs.Collection,
	fkName string,
) error {
	var syntheticTable catalog.TableDescriptor
	if srcTable.Version > srcTable.ClusterVersion.Version {
		syntheticTable = srcTable
	}
	var fk *descpb.ForeignKeyConstraint
	for i := range srcTable.OutboundFKs {
		def := &srcTable.OutboundFKs[i]
		if def.Name == fkName {
			fk = def
			break
		}
	}
	if fk == nil {
		return errors.AssertionFailedf("foreign key %s does not exist", fkName)
	}
	targetTable, err := descsCol.Direct().MustGetTableDescByID(ctx, txn, fk.ReferencedTableID)
	if err != nil {
		return err
	}
	var syntheticDescs []catalog.Descriptor
	if syntheticTable != nil {
		syntheticDescs = append(syntheticDescs, syntheticTable)
		if targetTable.GetID() == syntheticTable.GetID() {
			targetTable = syntheticTable
		}
	}
	ie := ief(ctx, sd)
	return ie.WithSyntheticDescriptors(syntheticDescs, func() error {
		return validateForeignKey(ctx, srcTable, targetTable, fk, ie, txn)
	})
}

// validateUniqueWithoutIndexConstraintInTxn validates a unique constraint
// within the provided transaction. If the provided table descriptor version
// is newer than the cluster version, it will be used in the InternalExecutor
// that performs the validation query.
//
// TODO (lucy): The special case where the table descriptor version is the same
// as the cluster version only happens because the query in VALIDATE CONSTRAINT
// still runs in the user transaction instead of a step in the schema changer.
// When that's no longer true, this function should be updated.
//
// It operates entirely on the current goroutine and is thus able to
// reuse an existing kv.Txn safely.
func validateUniqueWithoutIndexConstraintInTxn(
	ctx context.Context,
	ie sqlutil.InternalExecutor,
	tableDesc *tabledesc.Mutable,
	txn *kv.Txn,
	constraintName string,
) error {
	var syntheticDescs []catalog.Descriptor
	if tableDesc.Version > tableDesc.ClusterVersion.Version {
		syntheticDescs = append(syntheticDescs, tableDesc)
	}

	var uc *descpb.UniqueWithoutIndexConstraint
	for i := range tableDesc.UniqueWithoutIndexConstraints {
		def := &tableDesc.UniqueWithoutIndexConstraints[i]
		if def.Name == constraintName {
			uc = def
			break
		}
	}
	if uc == nil {
		return errors.AssertionFailedf("unique constraint %s does not exist", constraintName)
	}

	return ie.WithSyntheticDescriptors(syntheticDescs, func() error {
		return validateUniqueConstraint(
			ctx, tableDesc, uc.Name, uc.ColumnIDs, uc.Predicate, ie, txn, false, /* preExisting */
		)
	})
}

// columnBackfillInTxn backfills columns for all mutation columns in
// the mutation list.
//
// It operates entirely on the current goroutine and is thus able to
// reuse an existing kv.Txn safely.
func columnBackfillInTxn(
	ctx context.Context,
	txn *kv.Txn,
	execCfg *ExecutorConfig,
	evalCtx *tree.EvalContext,
	semaCtx *tree.SemaContext,
	tableDesc catalog.TableDescriptor,
	traceKV bool,
) error {
	// A column backfill in the ADD state is a noop.
	if tableDesc.Adding() {
		return nil
	}
	var columnBackfillerMon *mon.BytesMonitor
	// This is the planner's memory monitor.
	if evalCtx.Mon != nil {
		columnBackfillerMon = execinfra.NewMonitor(ctx, evalCtx.Mon, "local-column-backfill-mon")
	}

	rowMetrics := execCfg.GetRowMetrics(evalCtx.SessionData().Internal)
	var backfiller backfill.ColumnBackfiller
	if err := backfiller.InitForLocalUse(
		ctx, evalCtx, semaCtx, tableDesc, columnBackfillerMon, rowMetrics,
	); err != nil {
		return err
	}
	defer backfiller.Close(ctx)
	sp := tableDesc.PrimaryIndexSpan(evalCtx.Codec)
	for sp.Key != nil {
		var err error
		sp.Key, err = backfiller.RunColumnBackfillChunk(ctx,
			txn, tableDesc, sp, rowinfra.RowLimit(columnBackfillBatchSize.Get(&evalCtx.Settings.SV)),
			false /*alsoCommit*/, traceKV)
		if err != nil {
			return err
		}
	}

	return nil
}

// indexBackfillInTxn runs one chunk of the index backfill on the
// primary index.
//
// It operates entirely on the current goroutine and is thus able to
// reuse an existing kv.Txn safely.
func indexBackfillInTxn(
	ctx context.Context,
	txn *kv.Txn,
	evalCtx *tree.EvalContext,
	semaCtx *tree.SemaContext,
	tableDesc catalog.TableDescriptor,
	traceKV bool,
) error {
	var indexBackfillerMon *mon.BytesMonitor
	// This is the planner's memory monitor.
	if evalCtx.Mon != nil {
		indexBackfillerMon = execinfra.NewMonitor(ctx, evalCtx.Mon, "local-index-backfill-mon")
	}

	var backfiller backfill.IndexBackfiller
	if err := backfiller.InitForLocalUse(
		ctx, evalCtx, semaCtx, tableDesc, indexBackfillerMon,
	); err != nil {
		return err
	}
	defer backfiller.Close(ctx)
	sp := tableDesc.PrimaryIndexSpan(evalCtx.Codec)
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

// indexTruncateInTxn deletes an index from a table.
// It operates entirely on the current goroutine and is thus able to
// reuse an existing kv.Txn safely.
func indexTruncateInTxn(
	ctx context.Context,
	txn *kv.Txn,
	execCfg *ExecutorConfig,
	evalCtx *tree.EvalContext,
	tableDesc catalog.TableDescriptor,
	idx catalog.Index,
	traceKV bool,
) error {
	alloc := &tree.DatumAlloc{}
	var sp roachpb.Span
	for done := false; !done; done = sp.Key == nil {
		internal := evalCtx.SessionData().Internal
		rd := row.MakeDeleter(
			execCfg.Codec, tableDesc, nil /* requestedCols */, &execCfg.Settings.SV, internal,
			execCfg.GetRowMetrics(internal),
		)
		td := tableDeleter{rd: rd, alloc: alloc}
		if err := td.init(ctx, txn, evalCtx, &evalCtx.Settings.SV); err != nil {
			return err
		}
		var err error
		sp, err = td.deleteIndex(
			ctx, idx, sp, indexTruncateChunkSize, traceKV,
		)
		if err != nil {
			return err
		}
	}
	// Remove index zone configs.
	return RemoveIndexZoneConfigs(ctx, txn, execCfg, tableDesc, []uint32{uint32(idx.GetID())})
}

func (sc *SchemaChanger) distIndexMerge(
	ctx context.Context,
	tableDesc catalog.TableDescriptor,
	addedIndexes []descpb.IndexID,
	temporaryIndexes []descpb.IndexID,
) error {
	// Gather the initial resume spans for the merge process.
	progress, err := extractMergeProgress(sc.job, tableDesc, addedIndexes, temporaryIndexes)
	if err != nil {
		return err
	}

	log.VEventf(ctx, 2, "indexbackfill merge: initial resume spans %+v", progress.TodoSpans)
	if progress.TodoSpans == nil {
		return nil
	}

	// TODO(rui): these can be initialized along with other new schema changer dependencies.
	planner := NewIndexBackfillerMergePlanner(sc.execCfg, sc.execCfg.InternalExecutorFactory)
	tracker := NewIndexMergeTracker(progress, sc.job)
	periodicFlusher := newPeriodicProgressFlusher(sc.settings)

	metaFn := func(ctx context.Context, meta *execinfrapb.ProducerMetadata) error {
		if meta.BulkProcessorProgress != nil {
			idxCompletedSpans := make(map[int32][]roachpb.Span)
			for i, sp := range meta.BulkProcessorProgress.CompletedSpans {
				spanIdx := meta.BulkProcessorProgress.CompletedSpanIdx[i]
				idxCompletedSpans[spanIdx] = append(idxCompletedSpans[spanIdx], sp)
			}
			tracker.UpdateMergeProgress(ctx, func(_ context.Context, currentProgress *MergeProgress) {
				for idx, completedSpans := range idxCompletedSpans {
					currentProgress.TodoSpans[idx] = roachpb.SubtractSpans(currentProgress.TodoSpans[idx], completedSpans)
				}
			})
			if sc.testingKnobs.AlwaysUpdateIndexBackfillDetails {
				if err := tracker.FlushCheckpoint(ctx); err != nil {
					return err
				}
			}
		}
		return nil
	}

	stop := periodicFlusher.StartPeriodicUpdates(ctx, tracker)
	defer func() { _ = stop() }()

	run, err := planner.plan(ctx, tableDesc, progress.TodoSpans, progress.AddedIndexes,
		progress.TemporaryIndexes, metaFn)
	if err != nil {
		return err
	}

	if err := run(ctx); err != nil {
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

func extractMergeProgress(
	job *jobs.Job, tableDesc catalog.TableDescriptor, addedIndexes, temporaryIndexes []descpb.IndexID,
) (*MergeProgress, error) {
	resumeSpanList := job.Details().(jobspb.SchemaChangeDetails).ResumeSpanList
	progress := MergeProgress{}
	progress.TemporaryIndexes = temporaryIndexes
	progress.AddedIndexes = addedIndexes

	const noIdx = -1
	findMutIdx := func(id descpb.IndexID) int {
		for mutIdx, mut := range tableDesc.AllMutations() {
			if mut.AsIndex() != nil && mut.AsIndex().GetID() == id {
				return mutIdx
			}
		}

		return noIdx
	}

	for _, tempIdx := range temporaryIndexes {
		mutIdx := findMutIdx(tempIdx)
		if mutIdx == noIdx {
			return nil, errors.AssertionFailedf("no corresponding mutation for temporary index %d", tempIdx)
		}

		progress.TodoSpans = append(progress.TodoSpans, resumeSpanList[mutIdx].ResumeSpans)
		progress.MutationIdx = append(progress.MutationIdx, mutIdx)
	}

	return &progress, nil
}
