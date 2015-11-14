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
	"sort"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/log"
)

func makeColIDtoRowIndex(row planNode, desc *TableDescriptor) (map[ColumnID]int, error) {
	columns := row.Columns()
	colIDtoRowIndex := make(map[ColumnID]int, len(columns))
	for i, name := range columns {
		col, err := desc.FindActiveColumnByName(name)
		if err != nil {
			return nil, err
		}
		colIDtoRowIndex[col.ID] = i
	}
	return colIDtoRowIndex, nil
}

var _ sort.Interface = columnsByID{}
var _ sort.Interface = indexesByID{}

type columnsByID []ColumnDescriptor

func (cds columnsByID) Len() int {
	return len(cds)
}
func (cds columnsByID) Less(i, j int) bool {
	return cds[i].ID < cds[j].ID
}
func (cds columnsByID) Swap(i, j int) {
	cds[i], cds[j] = cds[j], cds[i]
}

type indexesByID []IndexDescriptor

func (ids indexesByID) Len() int {
	return len(ids)
}
func (ids indexesByID) Less(i, j int) bool {
	return ids[i].ID < ids[j].ID
}
func (ids indexesByID) Swap(i, j int) {
	ids[i], ids[j] = ids[j], ids[i]
}

func (p *planner) backfillBatch(b *client.Batch, tableName *parser.QualifiedName, oldTableDesc, newTableDesc *TableDescriptor) error {
	table := &parser.AliasedTableExpr{Expr: tableName}
	var droppedColumnDescs []ColumnDescriptor
	var droppedIndexDescs []IndexDescriptor
	var newIndexDescs []IndexDescriptor
	for _, m := range oldTableDesc.Mutations {
		switch m.Direction {
		case DescriptorMutation_ADD:
			switch t := m.Descriptor_.(type) {
			case *DescriptorMutation_Column:
				// TODO(vivek): Add column to new columns and use it
				// to fill in default values.

			case *DescriptorMutation_Index:
				newIndexDescs = append(newIndexDescs, *t.Index)
			}

		case DescriptorMutation_DROP:
			switch t := m.Descriptor_.(type) {
			case *DescriptorMutation_Column:
				droppedColumnDescs = append(droppedColumnDescs, *t.Column)

			case *DescriptorMutation_Index:
				droppedIndexDescs = append(droppedIndexDescs, *t.Index)
			}
		}
	}

	if len(droppedColumnDescs) > 0 {
		var updateExprs parser.UpdateExprs
		for _, droppedColumnDesc := range droppedColumnDescs {
			updateExprs = append(updateExprs, &parser.UpdateExpr{
				Names: parser.QualifiedNames{&parser.QualifiedName{Base: parser.Name(droppedColumnDesc.Name)}},
				Expr:  parser.DNull,
			})
		}

		// Run `UPDATE <table> SET col1 = NULL, col2 = NULL, ...` to clear
		// the data stored in the columns being dropped.
		if _, err := p.Update(&parser.Update{
			Table: table,
			Exprs: updateExprs,
		}); err != nil {
			return err
		}
	}

	for _, indexDescriptor := range droppedIndexDescs {
		indexPrefix := MakeIndexKeyPrefix(newTableDesc.ID, indexDescriptor.ID)

		// Delete the index.
		indexStartKey := roachpb.Key(indexPrefix)
		indexEndKey := indexStartKey.PrefixEnd()
		if log.V(2) {
			log.Infof("DelRange %s - %s", prettyKey(indexStartKey, 0), prettyKey(indexEndKey, 0))
		}
		b.DelRange(indexStartKey, indexEndKey)
	}

	if len(newIndexDescs) > 0 {
		// Get all the rows affected.
		// TODO(vivek): Avoid going through Select.
		// TODO(tamird): Support partial indexes?
		rows, err := p.Select(&parser.Select{
			Exprs: parser.SelectExprs{parser.StarSelectExpr()},
			From:  parser.TableExprs{table},
		})
		if err != nil {
			return err
		}

		// Construct a map from column ID to the index the value appears at within a
		// row.
		colIDtoRowIndex, err := makeColIDtoRowIndex(rows, oldTableDesc)
		if err != nil {
			return err
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

		for rows.Next() {
			rowVals := rows.Values()

			for _, newIndexDesc := range newIndexDescs {
				secondaryIndexEntries, err := encodeSecondaryIndexes(
					oldTableDesc.ID, []IndexDescriptor{newIndexDesc}, colIDtoRowIndex, rowVals)
				if err != nil {
					return err
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

		return rows.Err()
	}

	return nil
}
