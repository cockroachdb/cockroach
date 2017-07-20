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
//
// Author: XisiHuang (cockhuangxh@163.com)

package sql

import (
	"math"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TableTruncateChunkSize is the maximum number of keys deleted per chunk
// during a table truncation.
const TableTruncateChunkSize = indexTruncateChunkSize

// Truncate deletes all rows from a table.
// Privileges: DROP on table.
//   Notes: postgres requires TRUNCATE.
//          mysql requires DROP (for mysql >= 5.1.16, DELETE before that).
func (p *planner) Truncate(ctx context.Context, n *parser.Truncate) (planNode, error) {
	// Since truncation may cascade to a given table any number of times, start by
	// building the unique set (by ID) of tables to truncate.
	toTruncate := make(map[sqlbase.ID]*sqlbase.TableDescriptor, len(n.Tables))

	for _, name := range n.Tables {
		tn, err := name.NormalizeTableName()
		if err != nil {
			return nil, err
		}
		if err := tn.QualifyWithDatabase(p.session.Database); err != nil {
			return nil, err
		}

		tableDesc, err := p.session.tables.getTableVersion(ctx, p.txn, p.getVirtualTabler(), tn)
		if err != nil {
			return nil, err
		}
		// We don't support truncation on views, only real tables.
		if !tableDesc.IsTable() {
			return nil, errors.Errorf("cannot run TRUNCATE on view %q - views are not updateable", tn)
		}

		if err := p.CheckPrivilege(tableDesc, privilege.DROP); err != nil {
			return nil, err
		}
		toTruncate[tableDesc.ID] = tableDesc
	}

	// Check that any referencing tables are contained in the set, or, if CASCADE
	// requested, add them all to the set.
	for _, tableDesc := range toTruncate {
		for _, idx := range tableDesc.AllNonDropIndexes() {
			for _, ref := range idx.ReferencedBy {
				// Check if we're already truncating the referencing table.
				if _, ok := toTruncate[ref.Table]; ok {
					continue
				}

				other, err := p.session.tables.getTableVersionByID(ctx, p.txn, ref.Table)
				if err != nil {
					return nil, err
				}

				if n.DropBehavior != parser.DropCascade {
					return nil, errors.Errorf("%q is referenced by foreign key from table %q", tableDesc.Name, other.Name)
				}
				if err := p.CheckPrivilege(other, privilege.DROP); err != nil {
					return nil, err
				}
				toTruncate[other.ID] = other
			}
		}
	}

	// Mark this query as non-cancellable if autocommitting.
	if err := p.cancelChecker.Check(); err != nil {
		return nil, err
	}

	// TODO(knz): move truncate logic to Start/Next so it can be used with SHOW TRACE FOR.
	traceKV := p.session.Tracing.KVTracingEnabled()
	for _, tableDesc := range toTruncate {
		if err := truncateTable(p.session.Ctx(), tableDesc, p.txn, traceKV); err != nil {
			return nil, err
		}
	}

	return &emptyNode{}, nil
}

// truncateTable truncates the data of a table in a single transaction. It
// deletes a range of data for the table, which includes the PK and all
// indexes.
func truncateTable(
	ctx context.Context, tableDesc *sqlbase.TableDescriptor, txn *client.Txn, traceKV bool,
) error {
	alloc := &sqlbase.DatumAlloc{}
	rd, err := sqlbase.MakeRowDeleter(txn, tableDesc, nil, nil, false, alloc)
	if err != nil {
		return err
	}
	td := tableDeleter{rd: rd, alloc: alloc}
	if err := td.init(txn); err != nil {
		return err
	}
	_, err = td.deleteAllRows(ctx, roachpb.Span{}, math.MaxInt64, traceKV)
	return err
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
			rd, err := sqlbase.MakeRowDeleter(txn, tableDesc, nil, nil, false, alloc)
			if err != nil {
				return err
			}
			td := tableDeleter{rd: rd, alloc: alloc}
			if err := td.init(txn); err != nil {
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
