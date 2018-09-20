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

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	toTraverse := make([]sqlbase.TableDescriptor, 0, len(n.Tables))

	// This is the list of descriptors of truncated tables in the statement. These tables will have a mutationID and
	// MutationJob related to the created job of the TRUNCATE statement.
	stmtTableDescs := make([]*sqlbase.TableDescriptor, 0, len(n.Tables))
	droppedTableDetails := make([]jobspb.DroppedTableDetails, 0, len(n.Tables))

	for _, name := range n.Tables {
		tn, err := name.Normalize()
		if err != nil {
			return err
		}
		tableDesc, err := p.ResolveMutableTableDescriptor(
			ctx, tn, true /*required*/, requireTableDesc)
		if err != nil {
			return err
		}

		if err := p.CheckPrivilege(ctx, tableDesc, privilege.DROP); err != nil {
			return err
		}

		toTruncate[tableDesc.ID] = tn.FQString()
		toTraverse = append(toTraverse, *tableDesc)

		stmtTableDescs = append(stmtTableDescs, tableDesc)
		droppedTableDetails = append(droppedTableDetails, jobspb.DroppedTableDetails{
			Name: tn.FQString(),
			ID:   tableDesc.ID,
		})
	}

	dropJobID, err := p.createDropTablesJob(ctx, stmtTableDescs, droppedTableDetails, tree.AsStringWithFlags(n,
		tree.FmtAlwaysQualifyTableNames), false /* drainNames */)
	if err != nil {
		return err
	}

	// Check that any referencing tables are contained in the set, or, if CASCADE
	// requested, add them all to the set.
	for len(toTraverse) > 0 {
		// Pick last element.
		idx := len(toTraverse) - 1
		tableDesc := toTraverse[idx]
		toTraverse = toTraverse[:idx]

		maybeEnqueue := func(ref sqlbase.ForeignKeyReference, msg string) error {
			// Check if we're already truncating the referencing table.
			if _, ok := toTruncate[ref.Table]; ok {
				return nil
			}
			other, err := sqlbase.GetTableDescFromID(ctx, p.txn, ref.Table)
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
			toTruncate[other.ID] = otherName
			toTraverse = append(toTraverse, *other)
			return nil
		}

		for _, idx := range tableDesc.AllNonDropIndexes() {
			for _, ref := range idx.ReferencedBy {
				if err := maybeEnqueue(ref, "referenced by foreign key from"); err != nil {
					return err
				}
			}

			for _, ref := range idx.InterleavedBy {
				if err := maybeEnqueue(ref, "interleaved by"); err != nil {
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
		if err := p.truncateTable(ctx, id, dropJobID, traceKV); err != nil {
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
	ctx context.Context, id sqlbase.ID, dropJobID int64, traceKV bool,
) error {
	// Read the table descriptor because it might have changed
	// while another table in the truncation list was truncated.
	tableDesc, err := sqlbase.GetTableDescFromID(ctx, p.txn, id)
	if err != nil {
		return err
	}
	tableDesc.DropJobID = dropJobID
	newTableDesc := *tableDesc
	newTableDesc.ReplacementOf = sqlbase.TableDescriptor_Replacement{
		ID: id, Time: p.txn.CommitTimestamp(),
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
	zoneKey, nameKey, _ := GetKeysForTableDescriptor(tableDesc)
	b := &client.Batch{}
	// Use CPut because we want to remove a specific name -> id map.
	if traceKV {
		log.VEventf(ctx, 2, "CPut %s -> nil", nameKey)
	}
	b.CPut(nameKey, nil, tableDesc.ID)
	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}

	// Drop table.
	if err := p.initiateDropTable(ctx, tableDesc, false /* drainName */); err != nil {
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
		if err := p.writeSchemaChange(ctx, table, sqlbase.InvalidMutationID); err != nil {
			return err
		}
	}

	// Reassign all self references.
	if changed, err := reassignReferencedTables(
		[]*sqlbase.TableDescriptor{&newTableDesc}, tableDesc.ID, newID,
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

	tKey := tableKey{parentID: newTableDesc.ParentID, name: newTableDesc.Name}
	key := tKey.Key()
	if err := p.createDescriptorWithID(
		ctx, key, newID, &newTableDesc, p.ExtendedEvalContext().Settings); err != nil {
		return err
	}

	p.Tables().addCreatedTable(newID)

	// Copy the zone config.
	b = &client.Batch{}
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
	ctx context.Context, table sqlbase.TableDescriptor,
) ([]*sqlbase.TableDescriptor, error) {
	refs, err := table.FindAllReferences()
	if err != nil {
		return nil, err
	}
	tables := make([]*sqlbase.TableDescriptor, 0, len(refs))
	for id := range refs {
		if id == table.ID {
			continue
		}
		t, err := sqlbase.GetTableDescFromID(ctx, p.txn, id)
		if err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}

	return tables, nil
}

// reassign all the references from oldID to newID.
func reassignReferencedTables(
	tables []*sqlbase.TableDescriptor, oldID, newID sqlbase.ID,
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

			if index.ForeignKey.IsSet() {
				if to := index.ForeignKey.Table; to == oldID {
					index.ForeignKey.Table = newID
					changed = true
				}
			}

			origRefs := index.ReferencedBy
			index.ReferencedBy = nil
			for _, ref := range origRefs {
				if ref.Table == oldID {
					ref.Table = newID
					changed = true
				}
				index.ReferencedBy = append(index.ReferencedBy, ref)
			}
			return nil
		}); err != nil {
			return false, err
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

// truncateTableInChunks truncates the data of a table in chunks. It deletes a
// range of data for the table, which includes the PK and all indexes.
// The table has already been marked for deletion and has been purged from the
// descriptor cache on all nodes.
//
// TODO(vivek): No node is reading/writing data on the table at this stage,
// therefore the entire table can be deleted with no concern for conflicts (we
// can even eliminate the need to use a transaction for each chunk at a later
// stage if it proves inefficient).
func truncateTableInChunks(
	ctx context.Context, tableDesc *sqlbase.TableDescriptor, db *client.DB, traceKV bool,
) error {
	const chunkSize = TableTruncateChunkSize
	var resume roachpb.Span
	alloc := &sqlbase.DatumAlloc{}
	for row, done := 0, false; !done; row += chunkSize {
		resumeAt := resume
		if traceKV {
			log.VEventf(ctx, 2, "table %s truncate at row: %d, span: %s", tableDesc.Name, row, resume)
		}
		if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
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
			resume, err = td.deleteAllRows(ctx, resumeAt, chunkSize, noAutoCommit, traceKV)
			return err
		}); err != nil {
			return err
		}
		done = resume.Key == nil
	}
	return nil
}
