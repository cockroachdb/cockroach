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

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
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
	toTruncate := make(map[sqlbase.ID]string, len(n.Tables))
	// toTraverse is the list of tables whose references need to be traversed
	// while constructing the list of tables that should be truncated.
	toTraverse := make([]sqlbase.MutableTableDescriptor, 0, len(n.Tables))

	for i := range n.Tables {
		tn := &n.Tables[i]
		tableDesc, err := p.ResolveMutableTableDescriptor(
			ctx, tn, true /*required*/, ResolveRequireTableDesc)
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

		maybeEnqueue := func(tableID sqlbase.ID, msg string) error {
			// Check if we're already truncating the referencing table.
			if _, ok := toTruncate[tableID]; ok {
				return nil
			}
			other, err := p.Tables().getMutableTableVersionByID(ctx, tableID, p.txn)
			if err != nil {
				return err
			}

			if n.DropBehavior != tree.DropCascade {
				return errors.Errorf("%q is %s table %q", tableDesc.Name, msg, other.Name)
			}
			if err := p.CheckPrivilege(ctx, other, privilege.DROP); err != nil {
				return err
			}
			otherName, err := p.getQualifiedTableName(ctx, other.TableDesc())
			if err != nil {
				return err
			}
			toTruncate[other.ID] = otherName
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

	traceKV := p.extendedEvalCtx.Tracing.KVTracingEnabled()
	for id, name := range toTruncate {
		if err := p.truncateTable(ctx, id, tree.AsStringWithFQNames(t.n, params.Ann()), traceKV); err != nil {
			return err
		}

		// Log a Truncate Table event for this table.
		if err := MakeEventLogger(p.extendedEvalCtx.ExecCfg).InsertEventRecord(
			ctx,
			p.txn,
			EventLogTruncateTable,
			int32(id),
			int32(p.extendedEvalCtx.NodeID),
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

// truncateTable truncates the data of a table in a single transaction. It
// drops the table and recreates it with a new ID. The dropped table is
// GC-ed later through an asynchronous schema change.
func (p *planner) truncateTable(
	ctx context.Context, id sqlbase.ID, jobDesc string, traceKV bool,
) error {
	// Read the table descriptor because it might have changed
	// while another table in the truncation list was truncated.
	tableDesc, err := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
	if err != nil {
		return err
	}
	// tableDesc.DropJobID = dropJobID
	newTableDesc := sqlbase.NewMutableCreatedTableDescriptor(tableDesc.TableDescriptor)
	newTableDesc.ReplacementOf = sqlbase.TableDescriptor_Replacement{
		ID: id,
		// NB: Time is just used for debugging purposes. See the comment on the
		// field for more details.
		Time: p.txn.ReadTimestamp(),
	}
	newTableDesc.SetID(0)
	newTableDesc.Version = 1

	// Remove old name -> id map.
	// This is a violation of consistency because once the TRUNCATE commits
	// some nodes in the cluster can have cached the old name to id map
	// for the table and applying operations using the old table id.
	// This violation is needed because it is not uncommon for TRUNCATE
	// to be used along with other CRUD commands that follow it in the
	// same transaction. Commands that follow the TRUNCATE in the same
	// transaction will use the correct descriptor (through uncommittedTables)
	// See the comment about problem 3 related to draining names in
	// structured.proto
	//
	// TODO(vivek): Fix properly along with #12123.
	zoneKey := config.MakeZoneKey(uint32(tableDesc.ID))
	nameKey := sqlbase.MakePublicTableNameKey(ctx, p.ExecCfg().Settings, tableDesc.ParentID, tableDesc.GetName()).Key()
	key := sqlbase.MakePublicTableNameKey(ctx, p.ExecCfg().Settings, newTableDesc.ParentID, newTableDesc.Name).Key()

	b := &kv.Batch{}
	// Use CPut because we want to remove a specific name -> id map.
	if traceKV {
		log.VEventf(ctx, 2, "CPut %s -> nil", nameKey)
	}
	var existingIDVal roachpb.Value
	existingIDVal.SetInt(int64(tableDesc.ID))
	b.CPut(nameKey, nil, &existingIDVal)
	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

	// Drop table.
	if err := p.initiateDropTable(ctx, tableDesc, true /* queueJob */, jobDesc, false /* drainName */); err != nil {
		return err
	}

	newID, err := GenerateUniqueDescID(ctx, p.ExecCfg().DB)
	if err != nil {
		return err
	}

	// update all the references to this table.
	tables, err := p.findAllReferences(ctx, *tableDesc)
	if err != nil {
		return err
	}
	if changed, err := reassignReferencedTables(tables, tableDesc.ID, newID); err != nil {
		return err
	} else if changed {
		newTableDesc.State = sqlbase.TableDescriptor_ADD
	}

	for _, table := range tables {
		// TODO (lucy): Have more consistent/informative names for dependent jobs.
		if err := p.writeSchemaChange(
			ctx, table, sqlbase.InvalidMutationID, "updating reference for truncated table",
		); err != nil {
			return err
		}
	}

	// Reassign all self references.
	if changed, err := reassignReferencedTables(
		[]*sqlbase.MutableTableDescriptor{newTableDesc}, tableDesc.ID, newID,
	); err != nil {
		return err
	} else if changed {
		newTableDesc.State = sqlbase.TableDescriptor_ADD
	}

	// Resolve all outstanding mutations. Make all new schema elements
	// public because the table is empty and doesn't need to be backfilled.
	for _, m := range newTableDesc.Mutations {
		if err := newTableDesc.MakeMutationComplete(m); err != nil {
			return err
		}
	}
	newTableDesc.Mutations = nil
	newTableDesc.GCMutations = nil
	// NB: Set the modification time to a zero value so that it is interpreted
	// as the commit timestamp for the new descriptor. See the comment on
	// sqlbase.Descriptor.Table().
	newTableDesc.ModificationTime = hlc.Timestamp{}
	// TODO (lucy): Have more consistent/informative names for dependent jobs.
	if err := p.createDescriptorWithID(
		ctx, key, newID, newTableDesc, p.ExtendedEvalContext().Settings,
		"creating new descriptor for truncated table",
	); err != nil {
		return err
	}

	// Reassign comment.
	if err := reassignComment(ctx, p, tableDesc, newTableDesc); err != nil {
		return err
	}

	// Copy the zone config.
	b = &kv.Batch{}
	b.Get(zoneKey)
	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}
	val := b.Results[0].Rows[0].Value
	if val == nil {
		return nil
	}
	zoneCfg, err := val.GetBytes()
	if err != nil {
		return err
	}
	const insertZoneCfg = `INSERT INTO system.zones (id, config) VALUES ($1, $2)`
	_, err = p.ExtendedEvalContext().ExecCfg.InternalExecutor.Exec(
		ctx, "insert-zone", p.txn, insertZoneCfg, newID, zoneCfg)
	return err
}

// For all the references from a table
func (p *planner) findAllReferences(
	ctx context.Context, table sqlbase.MutableTableDescriptor,
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
		t, err := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
		if err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}

	return tables, nil
}

// reassign all the references from oldID to newID.
func reassignReferencedTables(
	tables []*sqlbase.MutableTableDescriptor, oldID, newID sqlbase.ID,
) (bool, error) {
	changed := false
	for _, table := range tables {
		if err := table.ForeachNonDropIndex(func(index *sqlbase.IndexDescriptor) error {
			for j, a := range index.Interleave.Ancestors {
				if a.TableID == oldID {
					index.Interleave.Ancestors[j].TableID = newID
					changed = true
				}
			}
			for j, c := range index.InterleavedBy {
				if c.Table == oldID {
					index.InterleavedBy[j].Table = newID
					changed = true
				}
			}
			return nil
		}); err != nil {
			return false, err
		}
		for i := range table.OutboundFKs {
			fk := &table.OutboundFKs[i]
			if fk.ReferencedTableID == oldID {
				fk.ReferencedTableID = newID
				changed = true
			}
		}
		for i := range table.InboundFKs {
			fk := &table.InboundFKs[i]
			if fk.OriginTableID == oldID {
				fk.OriginTableID = newID
				changed = true
			}
		}

		for i, dest := range table.DependsOn {
			if dest == oldID {
				table.DependsOn[i] = newID
				changed = true
			}
		}
		origRefs := table.DependedOnBy
		table.DependedOnBy = nil
		for _, ref := range origRefs {
			if ref.ID == oldID {
				ref.ID = newID
				changed = true
			}
			table.DependedOnBy = append(table.DependedOnBy, ref)
		}
	}
	return changed, nil
}

// reassignComment reassign comment on table.
func reassignComment(
	ctx context.Context, p *planner, oldTableDesc, newTableDesc *sqlbase.MutableTableDescriptor,
) error {
	comment, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryRowEx(
		ctx,
		"select-table-comment",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`SELECT comment FROM system.comments WHERE type=$1 AND object_id=$2`,
		keys.TableCommentType,
		oldTableDesc.ID)
	if err != nil {
		return err
	}

	if comment != nil {
		_, err = p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
			ctx,
			"set-table-comment",
			p.txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"UPSERT INTO system.comments VALUES ($1, $2, 0, $3)",
			keys.TableCommentType,
			newTableDesc.ID,
			comment[0])
		if err != nil {
			return err
		}

		_, err = p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
			ctx,
			"delete-comment",
			p.txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=0",
			keys.TableCommentType,
			oldTableDesc.ID)
		if err != nil {
			return err
		}
	}

	for i := range oldTableDesc.Columns {
		id := oldTableDesc.Columns[i].ID
		err = reassignColumnComment(ctx, p, oldTableDesc.ID, newTableDesc.ID, id)
		if err != nil {
			return err
		}
	}

	for _, indexDesc := range oldTableDesc.Indexes {
		err = reassignIndexComment(ctx, p, oldTableDesc.ID, newTableDesc.ID, indexDesc.ID)
		if err != nil {
			return err
		}
	}

	return nil
}

// reassignColumnComment reassign comment on column.
func reassignColumnComment(
	ctx context.Context, p *planner, oldID sqlbase.ID, newID sqlbase.ID, columnID sqlbase.ColumnID,
) error {
	comment, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryRowEx(
		ctx,
		"select-column-comment",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`SELECT comment FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=$3`,
		keys.ColumnCommentType,
		oldID,
		columnID)
	if err != nil {
		return err
	}

	if comment != nil {
		_, err = p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
			ctx,
			"set-column-comment",
			p.txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"UPSERT INTO system.comments VALUES ($1, $2, $3, $4)",
			keys.ColumnCommentType,
			newID,
			columnID,
			comment[0])
		if err != nil {
			return err
		}

		_, err = p.ExtendedEvalContext().ExecCfg.InternalExecutor.ExecEx(
			ctx,
			"delete-column-comment",
			p.txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"DELETE FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=$3",
			keys.ColumnCommentType,
			oldID,
			columnID)
		if err != nil {
			return err
		}
	}

	return nil
}

// reassignIndexComment reassigns a comment on an index.
func reassignIndexComment(
	ctx context.Context, p *planner, oldTableID, newTableID sqlbase.ID, indexID sqlbase.IndexID,
) error {
	comment, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryRowEx(
		ctx,
		"select-index-comment",
		p.txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		`SELECT comment FROM system.comments WHERE type=$1 AND object_id=$2 AND sub_id=$3`,
		keys.IndexCommentType,
		oldTableID,
		indexID)
	if err != nil {
		return err
	}

	if comment != nil {
		err = p.upsertIndexComment(
			ctx,
			newTableID,
			indexID,
			string(tree.MustBeDString(comment[0])))
		if err != nil {
			return err
		}

		err = p.removeIndexComment(ctx, oldTableID, indexID)
		if err != nil {
			return err
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
	ctx context.Context, tableDesc *sqlbase.TableDescriptor, db *kv.DB, traceKV bool,
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
			rd, err := row.MakeDeleter(
				ctx,
				txn,
				sqlbase.NewImmutableTableDescriptor(*tableDesc),
				nil,
				nil,
				row.SkipFKs,
				nil, /* *tree.EvalContext */
				alloc,
			)
			if err != nil {
				return err
			}
			td := tableDeleter{rd: rd, alloc: alloc}
			if err := td.init(ctx, txn, nil /* *tree.EvalContext */); err != nil {
				return err
			}
			resume, err = td.deleteAllRows(ctx, resumeAt, chunkSize, traceKV)
			return err
		}); err != nil {
			return err
		}
		done = resume.Key == nil
	}
	return nil
}

// canClearRangeForDrop returns if an index can be deleted by deleting every
// key from a single span.
// This determines whether an index is dropped during a schema change, or if
// it is only deleted upon GC.
func canClearRangeForDrop(index *sqlbase.IndexDescriptor) bool {
	return !index.IsInterleaved()
}
