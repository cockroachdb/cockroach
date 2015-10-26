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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package sql

import (
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/log"
)

func (p *planner) makeBackfillBatch(tableName *parser.QualifiedName, tableDesc *TableDescriptor, indexDescs ...IndexDescriptor) (client.Batch, error) {
	b := client.Batch{}
	// Get all the rows affected.
	// TODO(vivek): Avoid going through Select.
	// TODO(tamird): Support partial indexes?
	row, err := p.Select(&parser.Select{
		Exprs: parser.SelectExprs{parser.StarSelectExpr()},
		From:  parser.TableExprs{&parser.AliasedTableExpr{Expr: tableName}},
	})
	if err != nil {
		return b, err
	}

	// Construct a map from column ID to the index the value appears at within a
	// row.
	colIDtoRowIndex := map[ColumnID]int{}
	for i, name := range row.Columns() {
		c, err := tableDesc.FindColumnByName(name)
		if err != nil {
			return b, err
		}
		colIDtoRowIndex[c.ID] = i
	}

	// TODO(tamird): This will fall down in production use. We need to do
	// something better (see #2036). In particular, this implementation
	// has the following problems:
	// - Very large tables will generate an enormous batch here. This
	// isn't really a problem in itself except that it will exacerbate
	// the other issue:
	// - Any non-quiescent table that this runs against will end up with
	// an inconsistent index. This is because as inserts/updates continue
	// to roll in behind this operation's read front, the written index
	// will become incomplete/stale before it's written.

	for row.Next() {
		rowVals := row.Values()

		for _, indexDesc := range indexDescs {
			secondaryIndexEntries, err := encodeSecondaryIndexes(
				tableDesc.ID, []IndexDescriptor{indexDesc}, colIDtoRowIndex, rowVals)
			if err != nil {
				return b, err
			}

			for _, secondaryIndexEntry := range secondaryIndexEntries {
				if log.V(2) {
					log.Infof("CPut %s -> %v", prettyKey(secondaryIndexEntry.key, 0),
						secondaryIndexEntry.value)
				}
				b.CPut(secondaryIndexEntry.key, secondaryIndexEntry.value, nil)
			}
		}
	}

	return b, row.Err()
}

// applyOneMutation applies the first queued mutation from the descriptor in the database.
func applyOneMutation(db *client.DB, desc TableDescriptor, dbName string) error {
	tableName := desc.Name
	err := db.Txn(func(txn *client.Txn) error {
		p := planner{txn: txn, user: security.RootUser}
		// Read the table descriptor from the database
		tableDesc := &TableDescriptor{}
		if err := p.getDescriptor(tableKey{desc.ParentID, tableName}, tableDesc); err != nil {
			return err
		}
		if len(tableDesc.Mutations) == 0 {
			return nil
		}
		mutation := tableDesc.Mutations[0]
		tableDesc.Mutations = tableDesc.Mutations[1:]
		if err := tableDesc.applyMutation(*mutation); err != nil {
			return err
		}
		descKey := MakeDescMetadataKey(tableDesc.GetID())

		switch m := mutation.Descriptor_.(type) {
		case *TableDescriptor_Mutation_AddIndex:
			indexDesc := m.AddIndex
			if log.V(2) {
				log.Infof("Attempt to Add index: %s", indexDesc.Name)
			}
			// `indexDesc` changed on us when we called `tableDesc.AllocateIDs()`.
			indexDesc = &tableDesc.Indexes[len(tableDesc.Indexes)-1]

			name := &parser.QualifiedName{Base: parser.Name(tableName)}
			if err := name.NormalizeTableName(dbName); err != nil {
				return err
			}

			b, err := p.makeBackfillBatch(name, tableDesc, *indexDesc)
			if err != nil {
				return err
			}

			b.Put(descKey, tableDesc)
			p.txn.SetSystemDBTrigger()

			if err := p.txn.Run(&b); err != nil {
				return convertBatchError(tableDesc, b, err)
			}
			if log.V(2) {
				log.Infof("Added index %s", indexDesc.Name)
			}
		case *TableDescriptor_Mutation_AddColumn:
			columnDesc := m.AddColumn
			if log.V(2) {
				log.Infof("Attempt to Add column: %s", columnDesc.Name)
			}
			name := &parser.QualifiedName{Base: parser.Name(tableName)}
			if err := name.NormalizeTableName(dbName); err != nil {
				return err
			}

			b, err := p.makeBackfillBatch(name, tableDesc)
			if err != nil {
				return err
			}

			b.Put(descKey, tableDesc)
			p.txn.SetSystemDBTrigger()

			if err := p.txn.Run(&b); err != nil {
				return convertBatchError(tableDesc, b, err)
			}
			if log.V(2) {
				log.Infof("Added column %s", columnDesc.Name)
			}
		case *TableDescriptor_Mutation_DropIndex:
			indexDesc := m.DropIndex
			if log.V(2) {
				log.Infof("Attempt to drop index %s", indexDesc.Name)
			}
			// delete index
			indexPrefix := MakeIndexKeyPrefix(tableDesc.ID, indexDesc.ID)

			// Drop the index.
			indexStartKey := roachpb.Key(indexPrefix)
			indexEndKey := indexStartKey.PrefixEnd()
			if log.V(2) {
				log.Infof("DelRange %s - %s", prettyKey(indexStartKey, 0), prettyKey(indexEndKey, 0))
			}
			b := client.Batch{}

			b.DelRange(indexStartKey, indexEndKey)

			if err := tableDesc.Validate(); err != nil {
				return err
			}
			b.Put(descKey, tableDesc)
			p.txn.SetSystemDBTrigger()

			if err := p.txn.Run(&b); err != nil {
				return err
			}
			if log.V(2) {
				log.Infof("Dropped index %s", indexDesc.Name)
			}
		case *TableDescriptor_Mutation_DropColumn:
			panic("Not Implemented")

		}
		return nil
	})
	if err != nil {
		// Irrecoverable error; delete all future mutations.
		// TODO(vivek): Figure out a better strategy here.
		log.Info(err)
		if err := db.Txn(func(txn *client.Txn) error {
			p := planner{txn: txn, user: security.RootUser}
			// Read the table descriptor from the database
			tableDesc := &TableDescriptor{}
			if err := p.getDescriptor(tableKey{desc.ParentID, tableName}, tableDesc); err != nil {
				return err
			}
			if len(tableDesc.Mutations) == 0 {
				return nil
			}

			tableDesc.Mutations = nil
			if err := txn.Put(MakeDescMetadataKey(tableDesc.GetID()), tableDesc); err != nil {
				return err
			}
			p.txn.SetSystemDBTrigger()

			err := p.txn.Commit()
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}
