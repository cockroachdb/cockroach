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

var descriptorChangedVersionError = roachpb.NewErrorf("table descriptor has changed version")

// getTableDescAtVersion attempts to read a descriptor from the database at a
// specific version. It returns descriptorChangedVersionError if the current
// version is not the passed in version.
func getTableDescAtVersion(
	txn *client.Txn, id ID, version DescriptorVersion,
) (*TableDescriptor, *roachpb.Error) {
	tableDesc, pErr := getTableDescFromID(txn, id)
	if pErr != nil {
		return nil, pErr
	}
	if version != tableDesc.Version {
		return nil, descriptorChangedVersionError
	}
	return tableDesc, nil
}

// runBackfill runs the backfill for the schema changer. It runs the entire
// backfill at a specific version of the table descriptor, and re-attempts to
// run the schema change when the version changes.
func (sc *SchemaChanger) runBackfill(lease *TableDescriptor_SchemaChangeLease) *roachpb.Error {
	for {
		pErr := sc.runBackfillAtLatestVersion(lease)
		if pErr == descriptorChangedVersionError {
			continue
		}
		return pErr
	}
}

// Run the backfill at the latest table descriptor version. It returns
// descriptorChangedVersionError when the table descriptor version changes
// while it is running the backfill.
func (sc *SchemaChanger) runBackfillAtLatestVersion(
	lease *TableDescriptor_SchemaChangeLease,
) *roachpb.Error {
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
	// Remember the version at the start of the backfill so we can ensure
	// later that the table descriptor hasn't changed during the backfill
	// process.
	var version DescriptorVersion
	if pErr := sc.db.Txn(func(txn *client.Txn) *roachpb.Error {
		tableDesc, pErr := getTableDescFromID(txn, sc.tableID)
		if pErr != nil {
			return pErr
		}
		version = tableDesc.Version
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
		return nil
	}); pErr != nil {
		return pErr
	}

	// TODO(vivek): Break these backfill operations into chunks. All of them
	// will fail on big tables (see #3274).

	// Add and drop columns.
	if pErr := sc.truncateAndBackfillColumns(
		lease, addedColumnDescs, droppedColumnDescs, version,
	); pErr != nil {
		return pErr
	}

	// Drop indexes.
	if pErr := sc.truncateIndexes(lease, droppedIndexDescs, version); pErr != nil {
		return pErr
	}

	// Add new indexes.
	if pErr := sc.backfillIndexes(lease, addedIndexDescs, version); pErr != nil {
		return pErr
	}

	return nil
}

func (sc *SchemaChanger) truncateAndBackfillColumns(
	lease *TableDescriptor_SchemaChangeLease,
	added []ColumnDescriptor,
	dropped []ColumnDescriptor,
	version DescriptorVersion,
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

		pErr = sc.db.Txn(func(txn *client.Txn) *roachpb.Error {
			tableDesc, pErr := getTableDescAtVersion(txn, sc.tableID, version)
			if pErr != nil {
				return pErr
			}

			// Run a scan across the table using the primary key.
			start := roachpb.Key(MakeIndexKeyPrefix(tableDesc.ID, tableDesc.PrimaryIndex.ID))
			b := &client.Batch{}
			b.Scan(start, start.PrefixEnd(), 0)
			if pErr := txn.Run(b); pErr != nil {
				return pErr
			}

			if nonNullableColumn != "" {
				for _, result := range b.Results {
					if len(result.Rows) > 0 {
						return roachpb.NewErrorf("column %s contains null values", nonNullableColumn)
					}
				}
			}

			// Use a different batch to truncate/backfill columns.
			writeBatch := &client.Batch{}
			for _, result := range b.Results {
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
							val, err := marshalColumnValue(col, d, evalCtx.Args)
							if err != nil {
								return roachpb.NewError(err)
							}

							if log.V(2) {
								log.Infof("Put %s -> %v", colKey, val)
							}
							// Insert default value into the column. If this row
							// was recently added the default value might have
							// already been populated, because the
							// ColumnDescriptor is in the WRITE_ONLY state.
							// Reinserting the default value is not a big deal.
							//
							// Note: a column in the WRITE_ONLY state cannot be
							// populated directly through SQL. A SQL INSERT cannot
							// directly reference the column, and the INSERT
							// populates the column with the default value.
							writeBatch.Put(colKey, val)
						}
					}
				}
			}
			if pErr := txn.Run(writeBatch); pErr != nil {
				return convertBackfillError(tableDesc, writeBatch, pErr)
			}
			return nil
		})
		return pErr
	}
	return nil
}

func (sc *SchemaChanger) truncateIndexes(
	lease *TableDescriptor_SchemaChangeLease,
	dropped []IndexDescriptor,
	version DescriptorVersion,
) *roachpb.Error {
	for _, desc := range dropped {
		// First extend the schema change lease.
		l, pErr := sc.ExtendLease(*lease)
		if pErr != nil {
			return pErr
		}
		*lease = l
		if pErr := sc.db.Txn(func(txn *client.Txn) *roachpb.Error {
			tableDesc, pErr := getTableDescAtVersion(txn, sc.tableID, version)
			if pErr != nil {
				return pErr
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

func (sc *SchemaChanger) backfillIndexes(
	lease *TableDescriptor_SchemaChangeLease,
	added []IndexDescriptor,
	version DescriptorVersion,
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
	pErr = sc.db.Txn(func(txn *client.Txn) *roachpb.Error {
		tableDesc, pErr := getTableDescAtVersion(txn, sc.tableID, version)
		if pErr != nil {
			return pErr
		}

		// Get all the rows affected.
		// TODO(tamird): Support partial indexes?
		// Use a scanNode with SELECT to pass in a TableDescriptor
		// to the SELECT without needing to use a parser.QualifiedName,
		// because we want to run schema changes from a gossip feed of
		// table IDs.
		scan := &scanNode{
			planner: makePlanner(),
			txn:     txn,
			desc:    *tableDesc,
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
		for rows.Next() {
			rowVals := rows.Values()

			for _, desc := range added {
				secondaryIndexEntries, err := encodeSecondaryIndexes(
					tableDesc.ID, []IndexDescriptor{desc}, colIDtoRowIndex, rowVals)
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
		if rows.PErr() != nil {
			return rows.PErr()
		}
		if pErr := txn.Run(b); pErr != nil {
			return convertBackfillError(tableDesc, b, pErr)
		}
		return nil
	})
	return pErr
}
