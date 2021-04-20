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

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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

	// Collect copies of each interleaved descriptor being truncated before any
	// modification has been done to them. We need this in order to truncate
	// the interleaved indexes in the GC job. We have to collect these descriptors
	// before the truncate modifications to know what are the correct index spans
	// to delete. Once changes have been made, the index spans where k/v data for
	// the table reside are no longer accessible from the table.
	interleaveCopies := make(map[descpb.ID]*descpb.TableDescriptor)
	maybeAddInterleave := func(desc catalog.TableDescriptor) {
		if !desc.IsInterleaved() {
			return
		}
		_, ok := interleaveCopies[desc.GetID()]
		if ok {
			return
		}
		interleaveCopies[desc.GetID()] = protoutil.Clone(desc.TableDesc()).(*descpb.TableDescriptor)
	}

	for i := range n.Tables {
		tn := &n.Tables[i]
		tableDesc, err := p.ResolveMutableTableDescriptor(
			ctx, tn, true /*required*/, tree.ResolveRequireTableDesc)
		if err != nil {
			return err
		}

		if err := p.CheckPrivilege(ctx, tableDesc, privilege.DROP); err != nil {
			return err
		}

		toTruncate[tableDesc.ID] = tn.FQString()
		toTraverse = append(toTraverse, *tableDesc)
		maybeAddInterleave(tableDesc)
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
			maybeAddInterleave(other)
			return nil
		}

		for i := range tableDesc.InboundFKs {
			fk := &tableDesc.InboundFKs[i]
			if err := maybeEnqueue(fk.OriginTableID, "referenced by foreign key from"); err != nil {
				return err
			}
		}
		for _, idx := range tableDesc.NonDropIndexes() {
			for i := 0; i < idx.NumInterleavedBy(); i++ {
				ref := idx.GetInterleavedBy(i)
				if err := maybeEnqueue(ref.Table, "interleaved by"); err != nil {
					return err
				}
			}
		}
	}

	// Mark this query as non-cancellable if autocommitting.
	if err := p.cancelChecker.Check(); err != nil {
		return err
	}

	for id, name := range toTruncate {
		if err := p.truncateTable(ctx, id, interleaveCopies, tree.AsStringWithFQNames(t.n, params.Ann())); err != nil {
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

// truncateTable truncates the data of a table in a single transaction. It does
// so by dropping all existing indexes on the table and creating new ones without
// backfilling any data into the new indexes. The old indexes are cleaned up
// asynchronously by the SchemaChangeGCJob. interleaveDescs is a set of
// interleaved TableDescriptors being truncated before any of the truncate
// mutations have been applied.
func (p *planner) truncateTable(
	ctx context.Context,
	id descpb.ID,
	interleaveDescs map[descpb.ID]*descpb.TableDescriptor,
	jobDesc string,
) error {
	// Read the table descriptor because it might have changed
	// while another table in the truncation list was truncated.
	tableDesc, err := p.Descriptors().GetMutableTableVersionByID(ctx, id, p.txn)
	if err != nil {
		return err
	}

	// Get all tables that might reference this one.
	allRefs, err := p.findAllReferencingInterleaves(ctx, tableDesc)
	if err != nil {
		return err
	}

	if err := checkTableForDisallowedMutationsWithTruncate(tableDesc); err != nil {
		return err
	}

	// Resolve all outstanding mutations. Make all new schema elements
	// public because the table is empty and doesn't need to be backfilled.
	for _, m := range tableDesc.Mutations {
		if err := tableDesc.MakeMutationComplete(m); err != nil {
			return err
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
	if err := tableDesc.AllocateIDs(ctx); err != nil {
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
	var droppedInterleaves []descpb.IndexDescriptor
	for i := range oldIndexes {
		idx := oldIndexes[i]
		if idx.IsInterleaved() {
			droppedInterleaves = append(droppedInterleaves, idx)
		} else {
			droppedIndexes = append(droppedIndexes, jobspb.SchemaChangeGCDetails_DroppedIndex{
				IndexID:  idx.ID,
				DropTime: dropTime,
			})
		}
	}

	details := jobspb.SchemaChangeGCDetails{
		Indexes:  droppedIndexes,
		ParentID: tableDesc.ID,
	}
	// If we have any interleaved indexes, add that information to the job record.
	if len(droppedInterleaves) > 0 {
		details.InterleavedTable = interleaveDescs[tableDesc.ID]
		details.InterleavedIndexes = droppedInterleaves
	}
	record := CreateGCJobRecord(jobDesc, p.User(), details)
	if _, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(
		ctx, record, p.ExecCfg().JobRegistry.MakeJobID(), p.txn); err != nil {
		return err
	}

	// Unsplit all manually split ranges in the table so they can be
	// automatically merged by the merge queue.
	if err := p.unsplitRangesForTable(ctx, tableDesc); err != nil {
		return err
	}

	// Reassign any referenced index ID's from other tables.
	if err := p.reassignInterleaveIndexReferences(ctx, allRefs, tableDesc.ID, indexIDMapping); err != nil {
		return err
	}
	// Reassign any self references.
	if err := p.reassignInterleaveIndexReferences(
		ctx,
		[]*tabledesc.Mutable{tableDesc},
		tableDesc.ID,
		indexIDMapping,
	); err != nil {
		return err
	}

	// Move any zone configs on indexes over to the new set of indexes.
	oldIndexIDs := make([]descpb.IndexID, len(oldIndexes)-1)
	for i := range oldIndexIDs {
		oldIndexIDs[i] = oldIndexes[i+1].ID
	}
	newIndexIDs := make([]descpb.IndexID, len(tableDesc.PublicNonPrimaryIndexes()))
	for i := range newIndexIDs {
		newIndexIDs[i] = tableDesc.PublicNonPrimaryIndexes()[i].GetID()
	}
	swapInfo := &descpb.PrimaryKeySwap{
		OldPrimaryIndexId: oldIndexes[0].ID,
		OldIndexes:        oldIndexIDs,
		NewPrimaryIndexId: tableDesc.GetPrimaryIndexID(),
		NewIndexes:        newIndexIDs,
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
			if !m.Adding() {
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
			switch ct := c.ConstraintToUpdateDesc().ConstraintType; ct {
			case descpb.ConstraintToUpdate_CHECK,
				descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX,
				descpb.ConstraintToUpdate_NOT_NULL,
				descpb.ConstraintToUpdate_FOREIGN_KEY:
				return unimplemented.Newf(
					"TRUNCATE concurrent with ongoing schema change",
					"cannot perform TRUNCATE on %q which has an ongoing %s "+
						"constraint change", desc.GetName(), ct)
			default:
				return errors.AssertionFailedf("cannot perform TRUNCATE due to "+
					"unknown constraint type %v on mutation %d in %v", ct, i, desc)
			}
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
	tableDesc catalog.TableDescriptor,
	traceKV bool,
) error {
	const chunkSize = row.TableTruncateChunkSize
	var resume roachpb.Span
	alloc := &rowenc.DatumAlloc{}
	for rowIdx, done := 0, false; !done; rowIdx += chunkSize {
		resumeAt := resume
		if traceKV {
			log.VEventf(ctx, 2, "table %s truncate at row: %d, span: %s", tableDesc.GetName(), rowIdx, resume)
		}
		if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			rd := row.MakeDeleter(codec, tableDesc, nil /* requestedCols */)
			td := tableDeleter{rd: rd, alloc: alloc}
			if err := td.init(ctx, txn, nil /* *tree.EvalContext */); err != nil {
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

// findAllReferencingInterleaves finds all tables that might interleave or
// be interleaved by the input table.
func (p *planner) findAllReferencingInterleaves(
	ctx context.Context, table *tabledesc.Mutable,
) ([]*tabledesc.Mutable, error) {
	refs, err := table.FindAllReferences()
	if err != nil {
		return nil, err
	}
	tables := make([]*tabledesc.Mutable, 0, len(refs))
	for id := range refs {
		if id == table.ID {
			continue
		}
		t, err := p.Descriptors().GetMutableTableVersionByID(ctx, id, p.txn)
		if err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}

	return tables, nil
}

// reassignInterleaveIndexReferences reassigns all index ID's present in
// interleave descriptor references according to indexIDMapping.
func (p *planner) reassignInterleaveIndexReferences(
	ctx context.Context,
	tables []*tabledesc.Mutable,
	truncatedID descpb.ID,
	indexIDMapping map[descpb.IndexID]descpb.IndexID,
) error {
	for _, table := range tables {
		changed := false
		if err := catalog.ForEachNonDropIndex(table, func(indexI catalog.Index) error {
			index := indexI.IndexDesc()
			for j, a := range index.Interleave.Ancestors {
				if a.TableID == truncatedID {
					index.Interleave.Ancestors[j].IndexID = indexIDMapping[index.Interleave.Ancestors[j].IndexID]
					changed = true
				}
			}
			for j, c := range index.InterleavedBy {
				if c.Table == truncatedID {
					index.InterleavedBy[j].Index = indexIDMapping[index.InterleavedBy[j].Index]
					changed = true
				}
			}
			return nil
		}); err != nil {
			return err
		}
		if changed {
			if err := p.writeSchemaChange(ctx, table, descpb.InvalidMutationID, "updating reference for truncated table"); err != nil {
				return err
			}
		}
	}
	return nil
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

// canClearRangeForDrop returns if an index can be deleted by deleting every
// key from a single span.
// This determines whether an index is dropped during a schema change, or if
// it is only deleted upon GC.
func canClearRangeForDrop(index *descpb.IndexDescriptor) bool {
	return !index.IsInterleaved()
}
