// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type truncateNode struct {
	zeroInputPlanNode
	n *tree.Truncate
}

// Truncate deletes all rows from a table.
// Privileges: DROP on table.
//
//	Notes: postgres requires TRUNCATE.
//	       mysql requires DROP (for mysql >= 5.1.16, DELETE before that).
func (p *planner) Truncate(ctx context.Context, n *tree.Truncate) (planNode, error) {
	return &truncateNode{n: n}, nil
}

func (t *truncateNode) startExec(params runParams) error {
	p := params.p
	n := t.n
	ctx := params.ctx

	// Since truncation may cascade to a given table any number of times, start by
	// building the unique set (ID->name) of tables to truncate.
	toTruncate := make(map[descpb.ID]string, len(n.Tables))
	// toTraverse is the list of tables whose references need to be traversed
	// while constructing the list of tables that should be truncated.
	toTraverse := make([]tabledesc.Mutable, 0, len(n.Tables))

	for i := range n.Tables {
		tn := &n.Tables[i]
		_, tableDesc, err := p.ResolveMutableTableDescriptor(
			ctx, tn, true /*required*/, tree.ResolveRequireTableDesc)
		if err != nil {
			return err
		}

		if err := p.CheckPrivilege(ctx, tableDesc, privilege.DROP); err != nil {
			return err
		}

		toTruncate[tableDesc.ID] = tn.FQString()
		toTraverse = append(toTraverse, *tableDesc)
	}

	// Check that any referencing tables are contained in the set, or, if CASCADE
	// requested, add them all to the set.
	for len(toTraverse) > 0 {
		// Pick last element.
		idx := len(toTraverse) - 1
		tableDesc := toTraverse[idx]
		toTraverse = toTraverse[:idx]

		maybeEnqueue := func(tableID descpb.ID, msg string) error {
			// Check if we're already truncating the referencing table.
			if _, ok := toTruncate[tableID]; ok {
				return nil
			}
			other, err := p.Descriptors().MutableByID(p.txn).Table(ctx, tableID)
			if err != nil {
				return err
			}

			if n.DropBehavior != tree.DropCascade {
				return errors.Errorf("%q is %s table %q", tableDesc.Name, msg, other.Name)
			}
			if err := p.CheckPrivilege(ctx, other, privilege.DROP); err != nil {
				return err
			}
			otherName, err := p.getQualifiedTableName(ctx, other)
			if err != nil {
				return err
			}
			toTruncate[other.ID] = otherName.FQString()
			toTraverse = append(toTraverse, *other)
			return nil
		}

		for i := range tableDesc.InboundFKs {
			fk := &tableDesc.InboundFKs[i]
			if err := maybeEnqueue(fk.OriginTableID, "referenced by foreign key from"); err != nil {
				return err
			}
		}
	}

	// Mark this query as non-cancellable if autocommitting.
	if err := p.cancelChecker.Check(); err != nil {
		return err
	}

	for id, name := range toTruncate {
		if err := p.truncateTable(ctx, id, tree.AsStringWithFQNames(t.n, params.Ann())); err != nil {
			return err
		}

		// Log a Truncate Table event for this table.
		if err := params.p.logEvent(ctx,
			id,
			&eventpb.TruncateTable{
				TableName: name,
			}); err != nil {
			return err
		}
	}

	return nil
}

func (t *truncateNode) Next(runParams) (bool, error) { return false, nil }
func (t *truncateNode) Values() tree.Datums          { return tree.Datums{} }
func (t *truncateNode) Close(context.Context)        {}

// PreservedSplitCountMultiple is the setting that configures the number of
// split points that we re-create on a table after a truncate, or that we
// create in an index backfill . It's scaled by the number of nodes in the cluster.
var PreservedSplitCountMultiple = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	// Note: The internal name cannot change.
	"sql.truncate.preserved_split_count_multiple",
	"set to non-zero to cause TRUNCATE and index backfills to preserve range splits "+
		"from the table's indexes or copy them from the primary index. The multiple "+
		"given will be multiplied with the number of nodes in the cluster to produce "+
		"the number of preserved range splits. This can improve performance when "+
		"truncating or backlling an index from a table with significant write traffic.",
	4,
	settings.WithName("sql.schema.preserved_split_count_multiple"),
)

// truncateTable truncates the data of a table in a single transaction. It does
// so by dropping all existing indexes on the table and creating new ones without
// backfilling any data into the new indexes. The old indexes are cleaned up
// asynchronously by the SchemaChangeGCJob.
func (p *planner) truncateTable(ctx context.Context, id descpb.ID, jobDesc string) error {
	// Read the table descriptor because it might have changed
	// while another table in the truncation list was truncated.
	tableDesc, err := p.Descriptors().MutableByID(p.txn).Table(ctx, id)
	if err != nil {
		return err
	}

	if err := checkTableForDisallowedMutationsWithTruncate(tableDesc); err != nil {
		return err
	}

	// Exit early with an error if the table is undergoing a declarative schema
	// change, before we try to get job IDs and update job statuses later. See
	// createOrUpdateSchemaChangeJob.
	if catalog.HasConcurrentDeclarativeSchemaChange(tableDesc) {
		return scerrors.ConcurrentSchemaChangeError(tableDesc)
	}

	// Resolve all outstanding mutations. Make all new schema elements
	// public because the table is empty and doesn't need to be backfilled.
	//
	// We collect any temporary indexes regardless of their
	// direction so that they can be dropped as they are only used
	// for backfills.
	tempIndexMutations := []descpb.DescriptorMutation{}
	for _, m := range tableDesc.Mutations {
		if idx := m.GetIndex(); idx != nil && idx.UseDeletePreservingEncoding {
			tempIndexMutations = append(tempIndexMutations, m)
		} else {
			if err := tableDesc.MakeMutationComplete(m); err != nil {
				return err
			}
		}
	}

	tableDesc.Mutations = nil

	// Collect all of the old indexes and reset all of the index IDs.
	oldIndexes := make([]descpb.IndexDescriptor, len(tableDesc.ActiveIndexes()))
	for _, idx := range tableDesc.ActiveIndexes() {
		oldIndexes[idx.Ordinal()] = idx.IndexDescDeepCopy()
		newIndex := *idx.IndexDesc()
		newIndex.ID = descpb.IndexID(0)
		if idx.Primary() {
			tableDesc.SetPrimaryIndex(newIndex)
		} else {
			tableDesc.SetPublicNonPrimaryIndex(idx.Ordinal(), newIndex)
		}
	}

	// Create new ID's for all of the indexes in the table.
	{
		version := p.ExecCfg().Settings.Version.ActiveVersion(ctx)
		// Temporarily empty the mutation jobs slice otherwise the descriptor
		// validation performed by AllocateIDs will fail: the Mutations slice
		// has been emptied but MutationJobs only gets emptied later on.
		mutationJobs := tableDesc.MutationJobs
		tableDesc.MutationJobs = nil
		if err := tableDesc.AllocateIDs(ctx, version); err != nil {
			return err
		}
		tableDesc.MutationJobs = mutationJobs
	}

	// Construct a mapping from old index ID's to new index ID's.
	indexIDMapping := make(map[descpb.IndexID]descpb.IndexID, len(oldIndexes))
	for _, idx := range tableDesc.ActiveIndexes() {
		indexIDMapping[oldIndexes[idx.Ordinal()].ID] = idx.GetID()
	}

	// Create schema change GC jobs for all of the indexes.
	dropTime := timeutil.Now().UnixNano()
	droppedIndexes := make([]jobspb.SchemaChangeGCDetails_DroppedIndex, 0, len(oldIndexes))
	for i := range oldIndexes {
		idx := oldIndexes[i]
		droppedIndexes = append(droppedIndexes, jobspb.SchemaChangeGCDetails_DroppedIndex{
			IndexID:  idx.ID,
			DropTime: dropTime,
		})
	}
	// Also add the temporary indexes to the GC job. We set the
	// drop time to 1 since these can be GC'd immediately.
	minimumDropTime := int64(1)
	for _, m := range tempIndexMutations {
		droppedIndexes = append(droppedIndexes, jobspb.SchemaChangeGCDetails_DroppedIndex{
			IndexID:  m.GetIndex().ID,
			DropTime: minimumDropTime,
		})
	}

	details := jobspb.SchemaChangeGCDetails{
		Indexes:  droppedIndexes,
		ParentID: tableDesc.ID,
	}
	record := CreateGCJobRecord(jobDesc, p.User(), details)
	if _, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(
		ctx, record, p.ExecCfg().JobRegistry.MakeJobID(), p.InternalSQLTxn(),
	); err != nil {
		return err
	}

	oldIndexIDs := make([]descpb.IndexID, len(oldIndexes))
	for i := range oldIndexIDs {
		oldIndexIDs[i] = oldIndexes[i].ID
	}
	newIndexIDs := make([]descpb.IndexID, len(tableDesc.ActiveIndexes()))
	newIndexes := tableDesc.ActiveIndexes()
	for i := range newIndexIDs {
		newIndexIDs[i] = newIndexes[i].GetID()
	}

	// Move existing range split points in the pre-truncated table's indexes to
	// the new indexes we're creating, to avoid a thundering herd effect where
	// any existing traffic on the table will slam into a single range after the
	// truncate is completed.
	if err := p.copySplitPointsToNewIndexes(ctx, id, oldIndexIDs, newIndexIDs); err != nil {
		return err
	}

	// Move any zone configs on indexes over to the new set of indexes.
	swapInfo := &descpb.PrimaryKeySwap{
		OldPrimaryIndexId: oldIndexIDs[0],
		OldIndexes:        oldIndexIDs[1:],
		NewPrimaryIndexId: newIndexIDs[0],
		NewIndexes:        newIndexIDs[1:],
	}
	if err := maybeUpdateZoneConfigsForPKChange(
		ctx, p.InternalSQLTxn(), p.ExecCfg(), p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
		tableDesc, swapInfo, true, /* forceSwap */
	); err != nil {
		return err
	}

	// Reassign any index comments.
	if err := p.reassignIndexComments(ctx, tableDesc, indexIDMapping); err != nil {
		return err
	}

	// Mark any mutation jobs as successful and then clear the record of
	// their existence. Old code did not do this and relied on jobs finishing
	// and removing themselves. That turns out to be hazardous because those
	// jobs may now fail and then fail to revert.
	if err := p.markTableMutationJobsSuccessful(ctx, tableDesc); err != nil {
		return err
	}
	tableDesc.MutationJobs = nil

	return p.writeSchemaChange(ctx, tableDesc, descpb.InvalidMutationID, jobDesc)
}

// checkTableForDisallowedMutationsWithTruncate iterates the set of mutations
// in the descriptor and determines whether any should prevent the truncate
// from completing. Interactions between truncate and ongoing schema change
// jobs are complex. On some level, the truncate can allow the work of these
// mutations to be short-circuited. However, accounting for the various cross-
// descriptor side-effects (like removing back-references or cleaning up dropped
// indexes) feels hard to get exactly right in the existing model. Thus, it
// is safer to more generally just disallow TRUNCATE when there are things
// going on and then advise the user to allow jobs to continue or to cancel
// them.
//
// This also aligns with a future world where we will be diminishing the
// allowable concurrency. However, in today's schema change world, some
// cancellations require O(rows) work. Some of these operations we can safely
// just complete. This function iterates the set of mutations and decides
// whether any exist that are not safe to just complete.
func checkTableForDisallowedMutationsWithTruncate(desc *tabledesc.Mutable) error {

	// The mutations which are scary to complete are mutations which involve
	// back-references. That would be anything related to foreign keys and
	// dropping of a column using a user-defined type or sequence. We can permit
	// the addition or removal of columns so long as they do not involve
	// user-defined types.

	for i, m := range desc.AllMutations() {
		if idx := m.AsIndex(); idx != nil {
			// Do not allow dropping indexes.
			if !m.Adding() && !idx.IsTemporaryIndexForBackfill() {
				return unimplemented.Newf(
					"TRUNCATE concurrent with ongoing schema change",
					"cannot perform TRUNCATE on %q which has indexes being dropped", desc.GetName())
			}
		} else if col := m.AsColumn(); col != nil {
			if col.Dropped() && col.GetType().UserDefined() {
				return unimplemented.Newf(
					"TRUNCATE concurrent with ongoing schema change",
					"cannot perform TRUNCATE on %q which has a column (%q) being "+
						"dropped which depends on another object", desc.GetName(), col.GetName())
			}
		} else if c := m.AsConstraintWithoutIndex(); c != nil {
			var constraintType descpb.ConstraintToUpdate_ConstraintType
			if ck := c.AsCheck(); ck != nil {
				constraintType = descpb.ConstraintToUpdate_CHECK
				if ck.IsNotNullColumnConstraint() {
					constraintType = descpb.ConstraintToUpdate_NOT_NULL
				}
			} else if c.AsForeignKey() != nil {
				constraintType = descpb.ConstraintToUpdate_FOREIGN_KEY
			} else if c.AsUniqueWithoutIndex() != nil {
				constraintType = descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX
			} else {
				return errors.AssertionFailedf("cannot perform TRUNCATE due to "+
					"unknown constraint type %s on mutation %d in %v", c, i, desc)
			}
			return unimplemented.Newf(
				"TRUNCATE concurrent with ongoing schema change",
				"cannot perform TRUNCATE on %q which has an ongoing %s "+
					"constraint change", desc.GetName(), constraintType)
		} else if s := m.AsPrimaryKeySwap(); s != nil {
			return unimplemented.Newf(
				"TRUNCATE concurrent with ongoing schema change",
				"cannot perform TRUNCATE on %q which has an ongoing primary key "+
					"change", desc.GetName())
		} else if m.AsComputedColumnSwap() != nil {
			return unimplemented.Newf(
				"TRUNCATE concurrent with ongoing schema change",
				"cannot perform TRUNCATE on %q which has an ongoing column type "+
					"change", desc.GetName())
		} else if m.AsModifyRowLevelTTL() != nil {
			return unimplemented.Newf(
				"TRUNCATE concurrent with ongoing schema change",
				"cannot perform TRUNCATE on %q which has an ongoing row level TTL "+
					"change", desc.GetName())
		} else {
			return errors.AssertionFailedf("cannot perform TRUNCATE due to "+
				"concurrent unknown mutation of type %T for mutation %d in %v", m, i, desc)
		}
	}
	return nil
}

// copySplitPointsToNewIndexes copies any range split points from the indexes
// given by the oldIndexIDs slice to the indexes given by the newIndexIDs slice
// on the table given by the tableID.
// oldIndexIDs and newIndexIDs must be in the same order and be the same length.
func (p *planner) copySplitPointsToNewIndexes(
	ctx context.Context,
	tableID descpb.ID,
	oldIndexIDs []descpb.IndexID,
	newIndexIDs []descpb.IndexID,
) error {
	execCfg := p.ExecCfg()
	preservedSplitsMultiple := int(PreservedSplitCountMultiple.Get(execCfg.SV()))
	if preservedSplitsMultiple <= 0 {
		return nil
	}

	nNodes := execCfg.NodeDescs.GetNodeDescriptorCount()
	nSplits := preservedSplitsMultiple * nNodes

	log.Infof(ctx, "making %d new truncate split points (%d * %d)", nSplits, preservedSplitsMultiple, nNodes)

	// Re-split the new set of indexes along the same split points as the old
	// indexes.
	b := p.Txn().NewBatch()
	tablePrefix := execCfg.Codec.TablePrefix(uint32(tableID))

	// Fetch all of the range descriptors for this index.
	rangeDescIterator, err := execCfg.RangeDescIteratorFactory.NewIterator(ctx, roachpb.Span{
		Key:    tablePrefix,
		EndKey: tablePrefix.PrefixEnd(),
	})
	if err != nil {
		return err
	}

	// Shift the range split points from the old keyspace into the new keyspace,
	// filtering out any ranges that we can't translate.
	var splitPoints [][]byte
	for rangeDescIterator.Valid() {
		rangeDesc := rangeDescIterator.CurRangeDescriptor()
		rangeDescIterator.Next()
		// For every range's start key, translate the start key into the keyspace
		// of the replacement index. We'll split the replacement index along this
		// same boundary later.
		startKey := rangeDesc.StartKey

		restOfKey, foundTable, foundIndex, err := execCfg.Codec.DecodeIndexPrefix(roachpb.Key(startKey))
		if err != nil {
			// If we get an error here, it means that either our key didn't contain
			// an index ID (because it was the first range in a table) or the key
			// didn't contain a table ID (because it's still the first range in the
			// system that hasn't split off yet).
			// In this case, we can't translate this range into the new keyspace,
			// so we just have to continue along.
			continue
		}
		if foundTable != uint32(tableID) {
			// We found a split point that started somewhere else in the database,
			// so we can't translate it to the new keyspace. Don't bother with this
			// range.
			continue
		}
		var newIndexID descpb.IndexID
		var found bool
		for k := range oldIndexIDs {
			if oldIndexIDs[k] == descpb.IndexID(foundIndex) {
				newIndexID = newIndexIDs[k]
				found = true
			}
		}
		if !found {
			// We found a split point that is on an index that no longer exists.
			// This can happen if the table was truncated more than once in a row,
			// and there are old split points sitting around in the ranges. In this
			// case, we can't translate the range into the new keyspace, so we don't
			// bother with this range.
			continue
		}

		newStartKey := append(execCfg.Codec.IndexPrefix(uint32(tableID), uint32(newIndexID)), restOfKey...)
		splitPoints = append(splitPoints, newStartKey)
	}

	if len(splitPoints) == 0 {
		// No split points to carry over. We can leave early.
		return nil
	}

	// Finally, downsample the split points - choose just nSplits of them to keep.
	step := float64(len(splitPoints)) / float64(nSplits)
	if step < 1 {
		step = 1
	}
	expirationTime := kvserverbase.SplitByLoadMergeDelay.Get(execCfg.SV()).Nanoseconds()
	for i := 0; i < nSplits; i++ {
		// Evenly space out the ranges that we select from the ranges that are
		// returned.
		idx := int(step * float64(i))
		if idx >= len(splitPoints) {
			break
		}
		sp := splitPoints[idx]

		// Jitter the expiration time by 20% up or down from the default.
		maxJitter := expirationTime / 5
		jitter := rand.Int63n(maxJitter*2) - maxJitter
		expirationTime += jitter

		log.Infof(ctx, "truncate sending split request for key %s", sp)
		b.AddRawRequest(&kvpb.AdminSplitRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: sp,
			},
			SplitKey:       sp,
			ExpirationTime: execCfg.Clock.Now().Add(expirationTime, 0),
		})
	}

	if err = p.txn.DB().Run(ctx, b); err != nil {
		return err
	}

	// Now scatter the ranges, after we've finished splitting them.
	b = p.Txn().NewBatch()
	b.AddRawRequest(&kvpb.AdminScatterRequest{
		// Scatter all of the data between the start key of the first new index, and
		// the PrefixEnd of the last new index.
		RequestHeader: kvpb.RequestHeader{
			Key:    execCfg.Codec.IndexPrefix(uint32(tableID), uint32(newIndexIDs[0])),
			EndKey: execCfg.Codec.IndexPrefix(uint32(tableID), uint32(newIndexIDs[len(newIndexIDs)-1])).PrefixEnd(),
		},
		RandomizeLeases: true,
	})

	return p.txn.DB().Run(ctx, b)
}

func (p *planner) reassignIndexComments(
	ctx context.Context, table *tabledesc.Mutable, indexIDMapping map[descpb.IndexID]descpb.IndexID,
) error {
	for old, new := range indexIDMapping {
		cmt, found := p.descCollection.GetComment(catalogkeys.MakeCommentKey(uint32(table.GetID()), uint32(old), catalogkeys.IndexCommentType))
		if !found {
			continue
		}
		if err := p.deleteComment(ctx, table.GetID(), uint32(old), catalogkeys.IndexCommentType); err != nil {
			return err
		}
		if err := p.updateComment(ctx, table.GetID(), uint32(new), catalogkeys.IndexCommentType, cmt); err != nil {
			return err
		}
	}
	return nil
}
