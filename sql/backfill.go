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
	"fmt"
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
		col, err := desc.FindActiveColumnByName(column.Name)
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

// backfillBatch runs the backfill for all the mutations that match the ID
// of the first mutation.
func (p *planner) backfillBatch(b *client.Batch, tableDesc *TableDescriptor) *roachpb.Error {
	var droppedColumnDescs []ColumnDescriptor
	var droppedIndexDescs []IndexDescriptor
	var newColumnDescs []ColumnDescriptor
	var newIndexDescs []IndexDescriptor
	// Mutations are applied in a FIFO order. Only apply the first set
	// of mutations.
	mutationID := tableDesc.Mutations[0].MutationID
	// Collect the elements that are part of the mutation.
	for _, m := range tableDesc.Mutations {
		if m.MutationID != mutationID {
			break
		}
		switch m.Direction {
		case DescriptorMutation_ADD:
			switch t := m.Descriptor_.(type) {
			case *DescriptorMutation_Column:
				newColumnDescs = append(newColumnDescs, *t.Column)

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

	defaultExprMap := make(map[ColumnDescriptor]parser.Expr)
	d, err := p.makeDefaultExprs(newColumnDescs)
	if err != nil {
		return roachpb.NewError(err)
	}
	if d != nil && len(d) != len(newColumnDescs) {
		panic(fmt.Sprintf("Number of default expressions %d !=  %d num of columns", len(d), len(newColumnDescs)))
	}

	if len(droppedColumnDescs) > 0 || len(newColumnDescs) > 0 {
		// Run a scan across the table using the primary key.
		start := roachpb.Key(MakeIndexKeyPrefix(tableDesc.ID, tableDesc.PrimaryIndex.ID))
		// Use a different batch to perform the scan.
		batch := &client.Batch{}
		batch.Scan(start, start.PrefixEnd(), 0)
		if pErr := p.txn.Run(batch); pErr != nil {
			return pErr
		}

		// Get number of rows.
		isEmpty := true
		for _, result := range batch.Results {
			if len(result.Rows) > 0 {
				isEmpty = false
				break
			}
		}

		// Add new columns and create default expressions.
		// TODO(seif): this is duplicating code from insert.go and we should eliminate it.
		for i, columnDesc := range newColumnDescs {
			if !isEmpty && columnDesc.DefaultExpr == nil && !columnDesc.Nullable {
				return roachpb.NewErrorf("column %s contains null values", columnDesc.Name)
			}
			tableDesc.AddColumn(columnDesc)
			// d will either be nil or have the length of newColumnDescs
			if d != nil {
				defaultExprMap[columnDesc] = d[i]
			}
		}

		for _, result := range batch.Results {
			var sentinelKey roachpb.Key
			for _, kv := range result.Rows {
				if sentinelKey == nil || !bytes.HasPrefix(kv.Key, sentinelKey) {
					// Sentinel keys have a 0 suffix indicating 0 bytes of column
					// ID. Strip off that suffix to determine the prefix shared with the
					// other keys for the row.
					sentinelKey = stripColumnIDLength(kv.Key)

					// Delete the entire dropped columns.
					// This used to use SQL UPDATE in the past to update the dropped
					// column to NULL; but a column in the process of being
					// dropped is placed in the table descriptor mutations, and
					// a SQL UPDATE of a column in mutations will fail.
					for _, columnDesc := range droppedColumnDescs {
						// Delete the dropped column.
						colKey := keys.MakeColumnKey(sentinelKey, uint32(columnDesc.ID))
						if log.V(2) {
							log.Infof("Del %s", colKey)
						}
						b.Del(colKey)
					}
					// Add columns.
					for col, expr := range defaultExprMap {
						colKey := keys.MakeColumnKey(sentinelKey, uint32(col.ID))
						d, err := expr.Eval(p.evalCtx)
						if err != nil {
							return roachpb.NewError(err)
						}
						val, err := marshalColumnValue(col, d, p.evalCtx.Args)
						if err != nil {
							return roachpb.NewError(err)
						}
						if log.V(2) {
							log.Infof("CPut %s -> %v", colKey, val)
						}
						b.CPut(colKey, val, nil)
					}
				}
			}
		}
	}

	for _, indexDescriptor := range droppedIndexDescs {
		indexPrefix := MakeIndexKeyPrefix(tableDesc.ID, indexDescriptor.ID)

		// Delete the index.
		indexStartKey := roachpb.Key(indexPrefix)
		indexEndKey := indexStartKey.PrefixEnd()
		if log.V(2) {
			log.Infof("DelRange %s - %s", indexStartKey, indexEndKey)
		}
		b.DelRange(indexStartKey, indexEndKey, false)
	}

	if len(newIndexDescs) > 0 {
		// Get all the rows affected.
		// TODO(tamird): Support partial indexes?
		// Use a scanNode with SELECT to pass in a TableDescriptor
		// to the SELECT without needing to use a parser.QualifiedName,
		// because we want to run schema changes from a gossip feed of
		// table IDs.
		scan := &scanNode{
			planner: p,
			txn:     p.txn,
			desc:    *tableDesc,
		}
		scan.initDescDefaults()
		rows := selectIndex(scan, nil, false)

		// Construct a map from column ID to the index the value appears at within a
		// row.
		colIDtoRowIndex, err := makeColIDtoRowIndex(rows, tableDesc)
		if err != nil {
			return roachpb.NewError(err)
		}

		for rows.Next() {
			rowVals := rows.Values()

			for _, newIndexDesc := range newIndexDescs {
				secondaryIndexEntries, err := encodeSecondaryIndexes(
					tableDesc.ID, []IndexDescriptor{newIndexDesc}, colIDtoRowIndex, rowVals)
				if err != nil {
					return roachpb.NewError(err)
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

		return rows.PErr()
	}

	return nil
}
