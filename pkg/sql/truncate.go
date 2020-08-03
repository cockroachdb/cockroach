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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// TableTruncateChunkSize is the maximum number of keys deleted per chunk
// during a table truncation.
const TableTruncateChunkSize = indexTruncateChunkSize

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
	toTraverse := make([]sqlbase.MutableTableDescriptor, 0, len(n.Tables))

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
		for _, idx := range tableDesc.AllNonDropIndexes() {
			for _, ref := range idx.InterleavedBy {
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
		if err := p.truncateTable(ctx, id, tree.AsStringWithFQNames(t.n, params.Ann())); err != nil {
			return err
		}

		// Log a Truncate Table event for this table.
		if err := MakeEventLogger(p.extendedEvalCtx.ExecCfg).InsertEventRecord(
			ctx,
			p.txn,
			EventLogTruncateTable,
			int32(id),
			int32(p.extendedEvalCtx.NodeID.SQLInstanceID()),
			struct {
				TableName string
				Statement string
				User      string
			}{name, n.String(), p.SessionData().User},
		); err != nil {
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
// asynchronously by the SchemaChangeGCJob.
func (p *planner) truncateTable(ctx context.Context, id descpb.ID, jobDesc string) error {
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

	// Collect all of the old indexes.
	oldIndexes := make([]descpb.IndexDescriptor, len(tableDesc.Indexes)+1)
	oldIndexes[0] = *protoutil.Clone(&tableDesc.PrimaryIndex).(*descpb.IndexDescriptor)
	for i := range tableDesc.Indexes {
		oldIndexes[i+1] = *protoutil.Clone(&tableDesc.Indexes[i]).(*descpb.IndexDescriptor)
	}

	// Reset all of the index IDs.
	tableDesc.PrimaryIndex.ID = descpb.IndexID(0)
	for i := range tableDesc.Indexes {
		tableDesc.Indexes[i].ID = descpb.IndexID(0)
	}
	// Create new ID's for all of the indexes in the table.
	if err := tableDesc.AllocateIDs(); err != nil {
		return err
	}

	// Construct a mapping from old index ID's to new index ID's.
	indexIDMapping := make(map[descpb.IndexID]descpb.IndexID, len(oldIndexes))
	indexIDMapping[oldIndexes[0].ID] = tableDesc.PrimaryIndex.ID
	for i := range tableDesc.Indexes {
		indexIDMapping[oldIndexes[i+1].ID] = tableDesc.Indexes[i].ID
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

	if err := p.writeSchemaChange(ctx, tableDesc, descpb.InvalidMutationID, jobDesc); err != nil {
		return err
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
		Indexes:            droppedIndexes,
		InterleavedIndexes: droppedInterleaves,
		ParentID:           tableDesc.ID,
	}
	record := CreateGCJobRecord(jobDesc, p.User(), details)
	if _, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(ctx, record, p.txn); err != nil {
		return err
	}

	// Reassign any referenced index ID's from other tables.
	if err := p.reassignInterleaveIndexReferences(ctx, allRefs, tableDesc.ID, indexIDMapping); err != nil {
		return err
	}

	// Reassign any index comments.
	if err := p.reassignIndexComments(ctx, tableDesc, indexIDMapping); err != nil {
		return err
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
	tableDesc *sqlbase.ImmutableTableDescriptor,
	traceKV bool,
) error {
	const chunkSize = TableTruncateChunkSize
	var resume roachpb.Span
	alloc := &sqlbase.DatumAlloc{}
	for rowIdx, done := 0, false; !done; rowIdx += chunkSize {
		resumeAt := resume
		if traceKV {
			log.VEventf(ctx, 2, "table %s truncate at row: %d, span: %s", tableDesc.Name, rowIdx, resume)
		}
		if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			rd := row.MakeDeleter(codec, tableDesc, nil)
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
	ctx context.Context, table *sqlbase.MutableTableDescriptor,
) ([]*sqlbase.MutableTableDescriptor, error) {
	refs, err := table.FindAllReferences()
	if err != nil {
		return nil, err
	}
	tables := make([]*sqlbase.MutableTableDescriptor, 0, len(refs))
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
	tables []*sqlbase.MutableTableDescriptor,
	truncatedID descpb.ID,
	indexIDMapping map[descpb.IndexID]descpb.IndexID,
) error {
	for _, table := range tables {
		changed := false
		if err := table.ForeachNonDropIndex(func(index *descpb.IndexDescriptor) error {
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
	ctx context.Context,
	table *sqlbase.MutableTableDescriptor,
	indexIDMapping map[descpb.IndexID]descpb.IndexID,
) error {
	// Check if there are any index comments that need to be updated.
	row, err := p.extendedEvalCtx.ExecCfg.InternalExecutor.QueryRowEx(
		ctx,
		"update-table-comments",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`SELECT count(*) FROM system.comments WHERE object_id = $1 AND type = $2`,
		table.ID,
		keys.IndexCommentType,
	)
	if err != nil {
		return err
	}
	if int(tree.MustBeDInt(row[0])) > 0 {
		for old, new := range indexIDMapping {
			if _, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
				ctx,
				"update-table-comments",
				p.txn,
				sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
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
