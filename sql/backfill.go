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
// Author: Tamir Duberstein (tamird@gmail.com)

package sql

import (
	"bytes"
	"sort"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/log"
)

func makeColIDtoRowIndex(row planNode, desc *TableDescriptor) (map[ColumnID]int, error) {
	columns := row.Columns()
	colIDtoRowIndex := make(map[ColumnID]int, len(columns))
	for i, column := range columns {
		col, err := desc.FindActiveColumnByName(column.name)
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

func (p *planner) backfillBatch(b *client.Batch, oldTableDesc *TableDescriptor, mutationID MutationID) error {
	var droppedColumnDescs []ColumnDescriptor
	var droppedIndexDescs []IndexDescriptor
	var newIndexDescs []IndexDescriptor
	// Collect the elements that are part of the mutation.
	for _, m := range oldTableDesc.Mutations {
		if m.MutationID != mutationID {
			// Mutations are applied in a FIFO order.
			break
		}
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

	// TODO(vivek): Break these backfill operations into chunks. All of them
	// will fail on big tables (see #3274).

	// Delete the entire dropped columns.
	// This used to use SQL UPDATE in the past to update the dropped
	// column to NULL; but a column in the process of being
	// dropped is placed in the table descriptor mutations, and
	// a SQL UPDATE of a column in mutations will fail.
	if len(droppedColumnDescs) > 0 {
		// Run a scan across the table using the primary key.
		start := roachpb.Key(MakeIndexKeyPrefix(oldTableDesc.ID, oldTableDesc.PrimaryIndex.ID))
		// Use a different batch to perform the scan.
		batch := &client.Batch{}
		batch.Scan(start, start.PrefixEnd(), 0)
		if err := p.txn.Run(batch); err != nil {
			return err
		}
		for _, result := range batch.Results {
			var sentinelKey roachpb.Key
			for _, kv := range result.Rows {
				if sentinelKey == nil || !bytes.HasPrefix(kv.Key, sentinelKey) {
					// Sentinel keys have a 0 suffix indicating 0 bytes of column
					// ID. Strip off that suffix to determine the prefix shared with the
					// other keys for the row.
					sentinelKey = stripColumnIDLength(kv.Key)
					for _, columnDesc := range droppedColumnDescs {
						// Delete the dropped column.
						colKey := keys.MakeColumnKey(sentinelKey, uint32(columnDesc.ID))
						if log.V(2) {
							log.Infof("Del %s", colKey)
						}
						b.Del(colKey)
					}
				}
			}
		}
	}

	for _, indexDescriptor := range droppedIndexDescs {
		indexPrefix := MakeIndexKeyPrefix(oldTableDesc.ID, indexDescriptor.ID)

		// Delete the index.
		indexStartKey := roachpb.Key(indexPrefix)
		indexEndKey := indexStartKey.PrefixEnd()
		if log.V(2) {
			log.Infof("DelRange %s - %s", indexStartKey, indexEndKey)
		}
		b.DelRange(indexStartKey, indexEndKey)
	}

	if len(newIndexDescs) > 0 {
		// Get all the rows affected.
		// TODO(vivek): Avoid going through Select.
		// TODO(tamird): Support partial indexes?
		// Use a scanNode with SELECT to pass in a TableDescriptor
		// to the SELECT without needing to use a parser.QualifiedName,
		// because we want to run schema changes from a gossip feed of
		// table IDs.
		scan := &scanNode{
			planner: p,
			txn:     p.txn,
			desc:    oldTableDesc,
		}
		scan.initDescDefaults()
		rows, err := p.selectWithScan(scan, &parser.Select{Exprs: oldTableDesc.allColumnsSelector()})
		if err != nil {
			return err
		}

		// Construct a map from column ID to the index the value appears at within a
		// row.
		colIDtoRowIndex, err := makeColIDtoRowIndex(rows, oldTableDesc)
		if err != nil {
			return err
		}

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
						log.Infof("CPut %s -> %v", secondaryIndexEntry.key,
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
