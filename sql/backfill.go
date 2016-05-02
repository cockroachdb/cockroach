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
	"github.com/cockroachdb/cockroach/util/timeutil"
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

func convertBackfillError(
	tableDesc *TableDescriptor, b *client.Batch, pErr *roachpb.Error,
) *roachpb.Error {
	// A backfill on a new schema element has failed and the batch contains
	// information useful in printing a sensible error. However
	// convertBatchError() will only work correctly if the schema elements are
	// "live" in the tableDesc. Apply the mutations belonging to the same
	// mutationID to make all the mutations live in tableDesc. Note: this
	// tableDesc is not written to the k:v store.
	mutationID := tableDesc.Mutations[0].MutationID
	for _, mutation := range tableDesc.Mutations {
		if mutation.MutationID != mutationID {
			// Mutations are applied in a FIFO order. Only apply the first set
			// of mutations if they have the mutation ID we're looking for.
			break
		}
		tableDesc.makeMutationComplete(mutation)
	}
	return convertBatchError(tableDesc, *b, pErr)
}

// runBackfill runs the backfill for the schema changer.
func (sc *SchemaChanger) runBackfill(lease *TableDescriptor_SchemaChangeLease) *roachpb.Error {
	l, pErr := sc.ExtendLease(*lease)
	if pErr != nil {
		return pErr
	}
	*lease = l

	// Mutations are applied in a FIFO order. Only apply the first set of
	// mutations. Collect the elements that are part of the mutation.
	var droppedColumnDescs []ColumnDescriptor
	var droppedIndexDescs []IndexDescriptor
	var addedColumnDescs []ColumnDescriptor
	var addedIndexDescs []IndexDescriptor
	if pErr := sc.db.Txn(func(txn *client.Txn) *roachpb.Error {
		tableDesc, pErr := getTableDescFromID(txn, sc.tableID)
		if pErr != nil {
			return pErr
		}

		for _, m := range tableDesc.Mutations {
			if m.MutationID != sc.mutationID {
				break
			}
			switch m.Direction {
			case DescriptorMutation_ADD:
				switch t := m.Descriptor_.(type) {
				case *DescriptorMutation_Column:
					addedColumnDescs = append(addedColumnDescs, *t.Column)
				case *DescriptorMutation_Index:
					addedIndexDescs = append(addedIndexDescs, *t.Index)
				default:
					return roachpb.NewErrorf("unsupported mutation: %+v", m)
				}

			case DescriptorMutation_DROP:
				switch t := m.Descriptor_.(type) {
				case *DescriptorMutation_Column:
					droppedColumnDescs = append(droppedColumnDescs, *t.Column)
				case *DescriptorMutation_Index:
					droppedIndexDescs = append(droppedIndexDescs, *t.Index)
				default:
					return roachpb.NewErrorf("unsupported mutation: %+v", m)
				}
			}
		}
		return nil
	}); pErr != nil {
		return pErr
	}

	// Add and drop columns.
	if pErr := sc.truncateAndBackfillColumns(
		lease, addedColumnDescs, droppedColumnDescs,
	); pErr != nil {
		return pErr
	}

	// Drop indexes.
	if pErr := sc.truncateIndexes(lease, droppedIndexDescs); pErr != nil {
		return pErr
	}

	// Add new indexes.
	if pErr := sc.backfillIndexes(lease, addedIndexDescs); pErr != nil {
		return pErr
	}

	return nil
}

// getTableSpan returns a span containing the start and end key for a table.
func (sc *SchemaChanger) getTableSpan() (span, *roachpb.Error) {
	var tableDesc *TableDescriptor
	if pErr := sc.db.Txn(func(txn *client.Txn) *roachpb.Error {
		var pErr *roachpb.Error
		tableDesc, pErr = getTableDescFromID(txn, sc.tableID)
		return pErr
	}); pErr != nil {
		return span{}, pErr
	}
	prefix := roachpb.Key(MakeIndexKeyPrefix(tableDesc.ID, tableDesc.PrimaryIndex.ID))
	return span{
		start: prefix,
		end:   prefix.PrefixEnd(),
	}, nil
}

// ColumnTruncateAndBackfillChunkSize is the maximum number of rows of keys
// processed per chunk during the column truncate or backfill.
//
// TODO(vivek): Run some experiments to set this value to something sensible
// or adjust it dynamically. Also add in a sleep after every chunk is
// processed to slow down the backfill and reduce its CPU usage.
const ColumnTruncateAndBackfillChunkSize = 600

func (sc *SchemaChanger) truncateAndBackfillColumns(
	lease *TableDescriptor_SchemaChangeLease,
	added []ColumnDescriptor,
	dropped []ColumnDescriptor,
) *roachpb.Error {
	evalCtx := parser.EvalContext{}
	// Set the eval context timestamps.
	pTime := timeutil.Now()
	evalCtx.SetTxnTimestamp(pTime)
	evalCtx.SetStmtTimestamp(pTime)
	defaultExprs, err := makeDefaultExprs(added, &parser.Parser{}, evalCtx)
	if err != nil {
		return roachpb.NewError(err)
	}
	marshalled := make([]roachpb.Value, len(defaultExprs))

	// Remember any new non nullable column with no default value.
	nonNullableColumn := ""
	for _, columnDesc := range added {
		if columnDesc.DefaultExpr == nil && !columnDesc.Nullable {
			nonNullableColumn = columnDesc.Name
		}
	}

	// Add or Drop a column.
	if len(dropped) > 0 || nonNullableColumn != "" || len(defaultExprs) > 0 {
		// First extend the schema change lease.
		l, pErr := sc.ExtendLease(*lease)
		if pErr != nil {
			return pErr
		}
		*lease = l

		// Initialize start and end to represent a span of keys.
		sp, err := sc.getTableSpan()
		if err != nil {
			return err
		}

		// Run through the entire table key space adding and deleting columns.
		for done := false; !done; {
			// First extend the schema change lease.
			l, pErr := sc.ExtendLease(*lease)
			if pErr != nil {
				return pErr
			}
			*lease = l

			var curSentinel roachpb.Key
			if pErr := sc.db.Txn(func(txn *client.Txn) *roachpb.Error {
				tableDesc, pErr := getTableDescFromID(txn, sc.tableID)
				if pErr != nil {
					return pErr
				}
				// Short circuit the backfill if the table has been deleted.
				if tableDesc.Deleted {
					done = true
					return nil
				}

				// Run a scan across the table using the primary key. Running
				// the scan and applying the changes in many transactions is
				// fine because the schema change is in the correct state to
				// handle intermediate OLTP commands which delete and add
				// values during the scan.
				b := &client.Batch{}
				b.Scan(sp.start, sp.end, ColumnTruncateAndBackfillChunkSize)
				if pErr := txn.Run(b); pErr != nil {
					return pErr
				}

				// Use a different batch to truncate/backfill columns.
				writeBatch := &client.Batch{}
				done = true
				for _, result := range b.Results {
					var sentinelKey roachpb.Key
					for _, kv := range result.Rows {
						// Still processing table.
						done = false
						if nonNullableColumn != "" {
							return roachpb.NewErrorf("column %s contains null values", nonNullableColumn)
						}

						if sentinelKey == nil || !bytes.HasPrefix(kv.Key, sentinelKey) {
							// Sentinel keys have a 0 suffix indicating 0
							// bytes of column ID. Strip off that suffix to
							// determine the prefix shared with the other keys
							// for the row.
							sentinelKey = stripColumnIDLength(kv.Key)
							// Store away key for the next table row as the
							// point from which to start from.
							curSentinel = sentinelKey

							// Delete the entire dropped columns. This used to
							// use SQL UPDATE in the past to update the
							// dropped column to NULL; but a column in the
							// process of being dropped is placed in the table
							// descriptor mutations, and a SQL UPDATE of a
							// column in mutations will fail.
							for _, columnDesc := range dropped {
								// Delete the dropped column.
								colKey := keys.MakeColumnKey(sentinelKey, uint32(columnDesc.ID))
								if log.V(2) {
									log.Infof("Del %s", colKey)
								}
								writeBatch.Del(colKey)
							}

							// Add the new columns and backfill the values.
							for i, expr := range defaultExprs {
								if expr == nil {
									continue
								}
								col := added[i]
								colKey := keys.MakeColumnKey(sentinelKey, uint32(col.ID))
								d, err := expr.Eval(evalCtx)
								if err != nil {
									return roachpb.NewError(err)
								}
								marshalled[i], err = marshalColumnValue(col, d)
								if err != nil {
									return roachpb.NewError(err)
								}

								if log.V(2) {
									log.Infof("Put %s -> %v", colKey, d)
								}
								// Insert default value into the column. If
								// this row was recently added the default
								// value might have already been populated,
								// because the ColumnDescriptor is in the
								// WRITE_ONLY state. Reinserting the default
								// value is not a big deal.
								//
								// Note: a column in the WRITE_ONLY state
								// cannot be populated directly through SQL. A
								// SQL INSERT cannot directly reference the
								// column, and the INSERT populates the column
								// with the default value.
								writeBatch.Put(colKey, &marshalled[i])
							}
						}
					}
				}
				if pErr := txn.Run(writeBatch); pErr != nil {
					return convertBackfillError(tableDesc, writeBatch, pErr)
				}
				return nil
			}); pErr != nil {
				return pErr
			}
			// Store away next starting point.
			sp.start = curSentinel.PrefixEnd()
		}
	}
	return nil
}

func (sc *SchemaChanger) truncateIndexes(
	lease *TableDescriptor_SchemaChangeLease,
	dropped []IndexDescriptor,
) *roachpb.Error {
	for _, desc := range dropped {
		// First extend the schema change lease.
		l, pErr := sc.ExtendLease(*lease)
		if pErr != nil {
			return pErr
		}
		*lease = l
		if pErr := sc.db.Txn(func(txn *client.Txn) *roachpb.Error {
			tableDesc, pErr := getTableDescFromID(txn, sc.tableID)
			if pErr != nil {
				return pErr
			}
			// Short circuit the truncation if the table has been deleted.
			if tableDesc.Deleted {
				return nil
			}

			indexPrefix := MakeIndexKeyPrefix(tableDesc.ID, desc.ID)

			// Delete the index.
			indexStartKey := roachpb.Key(indexPrefix)
			indexEndKey := indexStartKey.PrefixEnd()
			if log.V(2) {
				log.Infof("DelRange %s - %s", indexStartKey, indexEndKey)
			}
			b := &client.Batch{}
			b.DelRange(indexStartKey, indexEndKey, false)

			if pErr := txn.Run(b); pErr != nil {
				return pErr
			}
			return nil
		}); pErr != nil {
			return pErr
		}
	}
	return nil
}

// IndexBackfillChunkSize is the maximum number of rows processed per chunk
// during the index backfill.
//
// TODO(vivek) Run some experiments to set this value to something sensible or
// adjust it dynamically. Also add in a sleep after every chunk is processed,
// to slow down the backfill and not have it interfere with OLTP commands.
const IndexBackfillChunkSize = 100

func (sc *SchemaChanger) backfillIndexes(
	lease *TableDescriptor_SchemaChangeLease,
	added []IndexDescriptor,
) *roachpb.Error {
	if len(added) == 0 {
		return nil
	}
	// First extend the schema change lease.
	l, pErr := sc.ExtendLease(*lease)
	if pErr != nil {
		return pErr
	}
	*lease = l

	// Initialize start and end to represent a span of keys.
	sp, err := sc.getTableSpan()
	if err != nil {
		return err
	}

	// Backfill the index entries for all the rows.
	for done := false; !done; {
		// First extend the schema change lease.
		l, pErr := sc.ExtendLease(*lease)
		if pErr != nil {
			return pErr
		}
		*lease = l
		var nextKey roachpb.Key
		pErr = sc.db.Txn(func(txn *client.Txn) *roachpb.Error {
			tableDesc, pErr := getTableDescFromID(txn, sc.tableID)
			if pErr != nil {
				return pErr
			}
			// Short circuit the backfill if the table has been deleted.
			if tableDesc.Deleted {
				done = true
				return nil
			}

			// Get the next set of rows.
			// TODO(tamird): Support partial indexes?
			//
			// Use a scanNode with SELECT to pass in a TableDescriptor to the
			// SELECT without needing to use a parser.QualifiedName, because
			// we want to run schema changes from a gossip feed of table IDs.
			// Running the scan and applying the changes in many transactions
			// is fine because the schema change is in the correct state to
			// handle intermediate OLTP commands which delete and add values
			// during the scan.
			scan := &scanNode{
				planner: makePlanner(),
				txn:     txn,
				desc:    *tableDesc,
				spans:   []span{sp},
			}
			scan.initDescDefaults()
			rows, err := selectIndex(scan, nil, false)
			if err != nil {
				return roachpb.NewError(err)
			}
			// Construct a map from column ID to the index the value appears at within a
			// row.
			colIDtoRowIndex, err := makeColIDtoRowIndex(rows, tableDesc)
			if err != nil {
				return roachpb.NewError(err)
			}
			b := &client.Batch{}
			numRows := 0
			for ; numRows < IndexBackfillChunkSize && rows.Next(); numRows++ {
				rowVals := rows.Values()

				for _, desc := range added {
					secondaryIndexEntries, err := encodeSecondaryIndexes(
						tableDesc.ID, []IndexDescriptor{desc}, colIDtoRowIndex, rowVals)
					if err != nil {
						return roachpb.NewError(err)
					}
					for _, secondaryIndexEntry := range secondaryIndexEntries {
						if log.V(2) {
							log.Infof("InitPut %s -> %v", secondaryIndexEntry.key,
								secondaryIndexEntry.value)
						}
						b.InitPut(secondaryIndexEntry.key, secondaryIndexEntry.value)
					}
				}
			}
			if rows.PErr() != nil {
				return rows.PErr()
			}
			// Write the new index values.
			if pErr := txn.Run(b); pErr != nil {
				return convertBackfillError(tableDesc, b, pErr)
			}
			// Have we processed all the table rows?
			if numRows < IndexBackfillChunkSize {
				done = true
				return nil
			}
			// Update sp.start to the next possible key; sp.end remains the
			// same.
			nextKey = scan.fetcher.kv.Key
			return nil
		})
		if pErr != nil {
			return pErr
		}
		sp.start = nextKey
	}
	return nil
}
