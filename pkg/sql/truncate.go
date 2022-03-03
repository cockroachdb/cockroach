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
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type truncateNode struct {
	n *tree.Truncate
}

// Truncate deletes all rows from a table.
// Privileges: DROP on table.
//   Notes: postgres requires TRUNCATE.
//          mysql requires DROP (for mysql >= 5.1.16, DELETE before that).
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
			other, err := p.Descriptors().GetMutableTableVersionByID(ctx, tableID, p.txn)
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
// split points that we re-create on a table after a truncate. It's scaled by
// the number of nodes in the cluster.
var PreservedSplitCountMultiple = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.truncate.preserved_split_count_multiple",
	"set to non-zero to cause TRUNCATE to preserve range splits from the "+
		"table's indexes. The multiple given will be multiplied with the number of "+
		"nodes in the cluster to produce the number of preserved range splits. This "+
		"can improve performance when truncating a table with significant write traffic.",
	4)

// truncateTable truncates the data of a table in a single transaction. It does
// so by dropping all existing indexes on the table and creating new ones without
// backfilling any data into the new indexes. The old indexes are cleaned up
// asynchronously by the SchemaChangeGCJob.
func (p *planner) truncateTable(ctx context.Context, id descpb.ID, jobDesc string) error {
	// Read the table descriptor because it might have changed
	// while another table in the truncation list was truncated.
	tableDesc, err := p.Descriptors().GetMutableTableVersionByID(ctx, id, p.txn)
	if err != nil {
		return err
	}

	if err := checkTableForDisallowedMutationsWithTruncate(tableDesc); err != nil {
		return err
	}

	// Exit early with an error if the table is undergoing a declarative schema
	// change, before we try to get job IDs and update job statuses later. See
	// createOrUpdateSchemaChangeJob.
	if tableDesc.GetDeclarativeSchemaChangerState() != nil {
		return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"cannot perform a schema change on table %q while it is undergoing a declarative schema change",
			tableDesc.GetName(),
		)
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
	tableDesc.GCMutations = nil

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
	version := p.ExecCfg().Settings.Version.ActiveVersion(ctx)
	if err := tableDesc.AllocateIDs(ctx, version); err != nil {
		return err
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
		ctx, record, p.ExecCfg().JobRegistry.MakeJobID(), p.txn); err != nil {
		return err
	}

	// TODO(Chengxiong): remove this block in 22.2
	st := p.EvalContext().Settings
	if !st.Version.IsActive(ctx, clusterversion.UnsplitRangesInAsyncGCJobs) {
		// Unsplit all manually split ranges in the table so they can be
		// automatically merged by the merge queue.
		if err := p.unsplitRangesForTable(ctx, tableDesc); err != nil {
			return err
		}
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
	if err := maybeUpdateZoneConfigsForPKChange(ctx, p.txn, p.ExecCfg(), tableDesc, swapInfo); err != nil {
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
		} else if c := m.AsConstraint(); c != nil {
			if c.IsCheck() || c.IsNotNull() || c.IsForeignKey() || c.IsUniqueWithoutIndex() {
				return unimplemented.Newf(
					"TRUNCATE concurrent with ongoing schema change",
					"cannot perform TRUNCATE on %q which has an ongoing %s "+
						"constraint change", desc.GetName(), c.ConstraintToUpdateDesc().ConstraintType)
			}
			return errors.AssertionFailedf("cannot perform TRUNCATE due to "+
				"unknown constraint type %v on mutation %d in %v", c.ConstraintToUpdateDesc().ConstraintType, i, desc)
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
		} else {
			return errors.AssertionFailedf("cannot perform TRUNCATE due to "+
				"concurrent unknown mutation of type %T for mutation %d in %v", m, i, desc)
		}
	}
	return nil
}

// ClearTableDataInChunks truncates the data of a table in chunks. It deletes a
// range of data for the table, which includes the PK and all indexes.
// The table has already been marked for deletion and has been purged from the
// descriptor cache on all nodes.
//
// TODO(vivek): No node is reading/writing data on the table at this stage,
// therefore the entire table can be deleted with no concern for conflicts (we
// can even eliminate the need to use a transaction for each chunk at a later
// stage if it proves inefficient).
func ClearTableDataInChunks(
	ctx context.Context,
	db *kv.DB,
	codec keys.SQLCodec,
	sv *settings.Values,
	tableDesc catalog.TableDescriptor,
	traceKV bool,
) error {
	const chunkSize = row.TableTruncateChunkSize
	var resume roachpb.Span
	alloc := &tree.DatumAlloc{}
	for rowIdx, done := 0, false; !done; rowIdx += chunkSize {
		resumeAt := resume
		if traceKV {
			log.VEventf(ctx, 2, "table %s truncate at row: %d, span: %s", tableDesc.GetName(), rowIdx, resume)
		}
		if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			rd := row.MakeDeleter(codec, tableDesc, nil /* requestedCols */, sv, true /* internal */, nil /* metrics */)
			td := tableDeleter{rd: rd, alloc: alloc}
			if err := td.init(ctx, txn, nil /* *tree.EvalContext */, sv); err != nil {
				return err
			}
			var err error
			resume, err = td.deleteAllRows(ctx, resumeAt, chunkSize, traceKV)
			return err
		}); err != nil {
			return err
		}
		done = resume.Key == nil
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
	if !p.EvalContext().Codec.ForSystemTenant() {
		// Can't do any of this direct manipulation of ranges in multi-tenant mode.
		return nil
	}

	preservedSplitsMultiple := int(PreservedSplitCountMultiple.Get(p.execCfg.SV()))
	if preservedSplitsMultiple <= 0 {
		return nil
	}
	row, err := p.execCfg.InternalExecutor.QueryRowEx(
		// Run as Root, since ordinary users can't select from this table.
		ctx, "count-active-nodes", nil, sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"SELECT count(*) FROM crdb_internal.kv_node_status")
	if err != nil || row == nil {
		return err
	}
	nNodes := int(tree.MustBeDInt(row[0]))
	nSplits := preservedSplitsMultiple * nNodes

	log.Infof(ctx, "making %d new truncate split points (%d * %d)", nSplits, preservedSplitsMultiple, nNodes)

	// Re-split the new set of indexes along the same split points as the old
	// indexes.
	var b kv.Batch
	tablePrefix := p.execCfg.Codec.TablePrefix(uint32(tableID))

	// Fetch all of the range descriptors for this index.
	ranges, err := kvclient.ScanMetaKVs(ctx, p.execCfg.DB.NewTxn(ctx, "truncate-copy-splits"), roachpb.Span{
		Key:    tablePrefix,
		EndKey: tablePrefix.PrefixEnd(),
	})
	if err != nil {
		return err
	}

	// Shift the range split points from the old keyspace into the new keyspace,
	// filtering out any ranges that we can't translate.
	var desc roachpb.RangeDescriptor
	splitPoints := make([][]byte, 0, len(ranges))
	for i := range ranges {
		if err := ranges[i].ValueProto(&desc); err != nil {
			return err
		}
		// For every range's start key, translate the start key into the keyspace
		// of the replacement index. We'll split the replacement index along this
		// same boundary later.
		startKey := desc.StartKey

		restOfKey, foundTable, foundIndex, err := p.execCfg.Codec.DecodeIndexPrefix(roachpb.Key(startKey))
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

		newStartKey := append(p.execCfg.Codec.IndexPrefix(uint32(tableID), uint32(newIndexID)), restOfKey...)
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
	expirationTime := kvserverbase.SplitByLoadMergeDelay.Get(p.execCfg.SV()).Nanoseconds()
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
		b.AddRawRequest(&roachpb.AdminSplitRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: sp,
			},
			SplitKey:       sp,
			ExpirationTime: p.execCfg.Clock.Now().Add(expirationTime, 0),
		})
	}

	if err = p.txn.DB().Run(ctx, &b); err != nil {
		return err
	}

	// Now scatter the ranges, after we've finished splitting them.
	b = kv.Batch{}
	b.AddRawRequest(&roachpb.AdminScatterRequest{
		// Scatter all of the data between the start key of the first new index, and
		// the PrefixEnd of the last new index.
		RequestHeader: roachpb.RequestHeader{
			Key:    p.execCfg.Codec.IndexPrefix(uint32(tableID), uint32(newIndexIDs[0])),
			EndKey: p.execCfg.Codec.IndexPrefix(uint32(tableID), uint32(newIndexIDs[len(newIndexIDs)-1])).PrefixEnd(),
		},
		RandomizeLeases: true,
	})

	return p.txn.DB().Run(ctx, &b)
}

func (p *planner) reassignIndexComments(
	ctx context.Context, table *tabledesc.Mutable, indexIDMapping map[descpb.IndexID]descpb.IndexID,
) error {
	// Check if there are any index comments that need to be updated.
	row, err := p.extendedEvalCtx.ExecCfg.InternalExecutor.QueryRowEx(
		ctx,
		"update-table-comments",
		p.txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		`SELECT count(*) FROM system.comments WHERE object_id = $1 AND type = $2`,
		table.ID,
		keys.IndexCommentType,
	)
	if err != nil {
		return err
	}
	if row == nil {
		return errors.New("failed to update table comments")
	}
	if int(tree.MustBeDInt(row[0])) > 0 {
		for old, new := range indexIDMapping {
			if _, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
				ctx,
				"update-table-comments",
				p.txn,
				sessiondata.InternalExecutorOverride{User: security.RootUserName()},
				`UPDATE system.comments SET sub_id=$1 WHERE sub_id=$2 AND object_id=$3 AND type=$4`,
				new,
				old,
				table.ID,
				keys.IndexCommentType,
			); err != nil {
				return err
			}
		}
	}
	return nil
}
