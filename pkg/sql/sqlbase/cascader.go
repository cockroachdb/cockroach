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

package sqlbase

import (
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// cascader is used to handle all referential integrity cascading actions.
type cascader struct {
	txn                *client.Txn
	tablesByID         TableLookupsByID                   // TablesDescriptors by Table ID
	indexRowFetchers   map[ID]map[IndexID]MultiRowFetcher // RowFetchers by Table ID and Index ID
	rowDeleters        map[ID]RowDeleter                  // RowDeleters by Table ID
	deleterRowFetchers map[ID]MultiRowFetcher             // RowFetchers for rowDeleters by Table ID
	rowsToCheck        map[ID][]tree.Datums               // Rows that have been deleted by Table ID
	alloc              *DatumAlloc
}

func makeCascader(txn *client.Txn, tablesByID TableLookupsByID, alloc *DatumAlloc) *cascader {
	return &cascader{
		txn:                txn,
		tablesByID:         tablesByID,
		indexRowFetchers:   make(map[ID]map[IndexID]MultiRowFetcher),
		rowDeleters:        make(map[ID]RowDeleter),
		deleterRowFetchers: make(map[ID]MultiRowFetcher),
		rowsToCheck:        make(map[ID][]tree.Datums),
		alloc:              alloc,
	}
}

// spanForIndexValues creates a span against an index to extract the primary
// keys needed for cascading.
func spanForIndexValues(
	table *TableDescriptor, index *IndexDescriptor, values tree.Datums,
) (roachpb.Span, error) {
	var key roachpb.Key
	//TODO(bram): consider caching both indexPrefix and indexColIDs
	indexPrefix := MakeIndexKeyPrefix(table, index.ID)
	// Create the indexColIDs by creating a map of the index columns. This is used
	// when creating the index span.
	indexColIDs := make(map[ColumnID]int, len(index.ColumnIDs))
	for i, colID := range index.ColumnIDs {
		indexColIDs[colID] = i
	}
	if values != nil {
		keyBytes, _, err := EncodePartialIndexKey(
			table, index, len(index.ColumnIDs), indexColIDs, values, indexPrefix)
		if err != nil {
			return roachpb.Span{}, err
		}
		key = roachpb.Key(keyBytes)
	} else {
		key = roachpb.Key(indexPrefix)
	}
	return roachpb.Span{Key: key, EndKey: key.PrefixEnd()}, nil
}

// batchRequestForIndexValues creates a batch request against an index to
// extract the primary keys needed for cascading.
func batchRequestForIndexValues(
	table *TableDescriptor, index *IndexDescriptor, values []tree.Datums,
) (roachpb.BatchRequest, error) {
	var req roachpb.BatchRequest
	for _, value := range values {
		span, err := spanForIndexValues(table, index, value)
		if err != nil {
			return roachpb.BatchRequest{}, err
		}
		req.Add(&roachpb.ScanRequest{Span: span})
	}
	return req, nil
}

// spanForPKValues creates a span against the primary index of a table and is
// used to fetch rows for cascading. This requires that the row deleter has
// already been created.
func spanForPKValues(
	table *TableDescriptor, fetchColIDtoRowIndex map[ColumnID]int, values tree.Datums,
) (roachpb.Span, error) {
	keyBytes, _, err := EncodePartialIndexKey(
		table,
		&table.PrimaryIndex,
		len(table.PrimaryIndex.ColumnIDs),
		fetchColIDtoRowIndex,
		values,
		MakeIndexKeyPrefix(table, table.PrimaryIndex.ID),
	)
	if err != nil {
		return roachpb.Span{}, err
	}
	key := roachpb.Key(keyBytes)
	return roachpb.Span{Key: key, EndKey: key.PrefixEnd()}, nil
}

// batchRequestForPKValues creates a batch request against the primary index of
// a table and is used to fetch rows for cascading.
func batchRequestForPKValues(
	table *TableDescriptor, fetchColIDtoRowIndex map[ColumnID]int, values []tree.Datums,
) (roachpb.BatchRequest, error) {
	var req roachpb.BatchRequest
	for _, value := range values {
		span, err := spanForPKValues(table, fetchColIDtoRowIndex, value)
		if err != nil {
			return roachpb.BatchRequest{}, err
		}
		req.Add(&roachpb.ScanRequest{Span: span})
	}
	return req, nil
}

// addIndexRowFetch will create or load a cached row fetcher on an index to
// fetch the primary keys of the rows that will be affected by a cascading
// action.
func (c *cascader) addIndexRowFetcher(
	table *TableDescriptor, index *IndexDescriptor,
) (MultiRowFetcher, error) {
	// Is there a cached row fetcher?
	rowFetchersForTable, exists := c.indexRowFetchers[table.ID]
	if exists {
		rowFetcher, exists := rowFetchersForTable[index.ID]
		if exists {
			return rowFetcher, nil
		}
	} else {
		c.indexRowFetchers[table.ID] = make(map[IndexID]MultiRowFetcher)
	}

	// Create a new row fetcher. Only the primary key columns are required.
	var colDesc []ColumnDescriptor
	for _, id := range table.PrimaryIndex.ColumnIDs {
		cDesc, err := table.FindColumnByID(id)
		if err != nil {
			return MultiRowFetcher{}, err
		}
		colDesc = append(colDesc, *cDesc)
	}
	var valNeededForCol util.FastIntSet
	valNeededForCol.AddRange(0, len(colDesc)-1)
	isSecondary := table.PrimaryIndex.ID != index.ID
	var rowFetcher MultiRowFetcher
	if err := rowFetcher.Init(
		false, /* reverse */
		false, /* returnRangeInfo */
		c.alloc,
		MultiRowFetcherTableArgs{
			Desc:             table,
			Index:            index,
			ColIdxMap:        ColIDtoRowIndexFromCols(colDesc),
			IsSecondaryIndex: isSecondary,
			Cols:             colDesc,
			ValNeededForCol:  valNeededForCol,
		},
	); err != nil {
		return MultiRowFetcher{}, err
	}
	// Cache the row fetcher.
	c.indexRowFetchers[table.ID][index.ID] = rowFetcher
	return rowFetcher, nil
}

// addRowDeleter creates the row deleter and primary index row fetcher.
func (c *cascader) addRowDeleter(table *TableDescriptor) (RowDeleter, MultiRowFetcher, error) {
	// Is there a cached row fetcher and deleter?
	if rowDeleter, exists := c.rowDeleters[table.ID]; exists {
		return rowDeleter, c.deleterRowFetchers[table.ID], nil
	}

	// Create the row deleter. The row deleter is needed prior to the row fetcher
	// as it will dictate what columns are required in the row fetcher.
	rowDeleter, err := MakeRowDeleter(
		c.txn,
		table,
		c.tablesByID,
		nil,  /* requestedCol */
		true, /* checkFKs */
		c.alloc,
	)
	if err != nil {
		return RowDeleter{}, MultiRowFetcher{}, err
	}

	// Create the row fetcher that will retrive the rows and columns needed for
	// deletion.
	var valNeededForCol util.FastIntSet
	valNeededForCol.AddRange(0, len(rowDeleter.FetchCols)-1)
	tableArgs := MultiRowFetcherTableArgs{
		Desc:             table,
		Index:            &table.PrimaryIndex,
		ColIdxMap:        rowDeleter.FetchColIDtoRowIndex,
		IsSecondaryIndex: false,
		Cols:             rowDeleter.FetchCols,
		ValNeededForCol:  valNeededForCol,
	}
	var rowFetcher MultiRowFetcher
	if err := rowFetcher.Init(
		false, /* reverse */
		false, /* returnRangeInfo */
		c.alloc,
		tableArgs,
	); err != nil {
		return RowDeleter{}, MultiRowFetcher{}, err
	}

	// Cache both the fetcher and deleter.
	c.rowDeleters[table.ID] = rowDeleter
	c.deleterRowFetchers[table.ID] = rowFetcher
	return rowDeleter, rowFetcher, nil
}

// deleteRow performs row deletions on a single table for all rows that match
// the values. Returns the values of the rows that were deleted. This deletion
// happens in a single batch.
func (c *cascader) deleteRow(
	ctx context.Context,
	table *TableDescriptor,
	index *IndexDescriptor,
	values []tree.Datums,
	traceKV bool,
) ([]tree.Datums, error) {
	// Create the span to search for index values.
	// TODO(bram): This initial index lookup can be skipped if the index is the
	// primary index.
	req, err := batchRequestForIndexValues(table, index, values)
	if err != nil {
		return nil, err
	}
	br, roachErr := c.txn.Send(ctx, req)
	if roachErr != nil {
		return nil, roachErr.GoError()
	}

	// Create or retrieve the index row fetcher.
	indexRowFetcher, err := c.addIndexRowFetcher(table, index)
	if err != nil {
		return nil, err
	}

	// Fetch all the primary keys that need to be deleted.
	var primaryKeysToDel []tree.Datums
	for _, resp := range br.Responses {
		fetcher := spanKVFetcher{
			kvs: resp.GetInner().(*roachpb.ScanResponse).Rows,
		}
		if err := indexRowFetcher.StartScanFrom(ctx, &fetcher); err != nil {
			return nil, err
		}
		for !indexRowFetcher.kvEnd {
			primaryKey, _, _, err := indexRowFetcher.NextRowDecoded(ctx)
			if err != nil {
				return nil, err
			}
			// Make a copy of the primary key because the datum struct is reused in
			// the row fetcher.
			primaryKey = append(tree.Datums(nil), primaryKey...)
			primaryKeysToDel = append(primaryKeysToDel, primaryKey)
			log.Warningf(ctx, "******** pks to delete: %+v", primaryKey)
		}
	}

	// Early exit if no rows need to be deleted.
	if len(primaryKeysToDel) == 0 {
		return nil, nil
	}

	// Create or retrieve the row deleter and primary index row fetcher.
	rowDeleter, pkRowFetcher, err := c.addRowDeleter(table)
	if err != nil {
		return nil, err
	}

	// Create a batch request to get all the spans of the primary keys that need
	// to be deleted.
	pkLookupReq, err := batchRequestForPKValues(table, rowDeleter.FetchColIDtoRowIndex, primaryKeysToDel)
	if err != nil {
		return nil, err
	}
	pkResp, roachErr := c.txn.Send(ctx, pkLookupReq)
	if roachErr != nil {
		return nil, roachErr.GoError()
	}

	// Fetch the rows for deletion.
	var rowsToDelete []tree.Datums
	for _, resp := range pkResp.Responses {
		fetcher := spanKVFetcher{
			kvs: resp.GetInner().(*roachpb.ScanResponse).Rows,
		}
		if err := pkRowFetcher.StartScanFrom(ctx, &fetcher); err != nil {
			return nil, err
		}
		for !pkRowFetcher.kvEnd {
			rowToDelete, _, _, err := pkRowFetcher.NextRowDecoded(ctx)
			if err != nil {
				return nil, err
			}
			// Make a copy of the rowToDelete because the datum struct is reused in
			// the row fetcher.
			rowToDelete = append(tree.Datums(nil), rowToDelete...)
			rowsToDelete = append(rowsToDelete, rowToDelete)
			log.Warningf(ctx, "******** row to delete: %+v", rowToDelete)
		}
	}

	// Delete the rows in a new batch.
	// TODO(bram): Can we move this batch out of this function?  Might not work
	// when dealing with updates.
	deleteBatch := c.txn.NewBatch()
	for _, row := range rowsToDelete {
		if err := rowDeleter.deleteRowNoCascade(ctx, deleteBatch, row, traceKV); err != nil {
			return nil, err
		}
	}
	if err := c.txn.Run(ctx, deleteBatch); err != nil {
		return nil, err
	}

	// Add the values to be checked for consistency after all cascading changes
	// have finished.
	c.rowsToCheck[table.ID] = append(c.rowsToCheck[table.ID], rowsToDelete...)
	return rowsToDelete, nil
}

type cascadeQueueElement struct {
	table  *TableDescriptor
	values []tree.Datums
}

// cascadeQueue is used for a breadth first walk of the referential integrity
// graph.
type cascadeQueue []cascadeQueueElement

func (q *cascadeQueue) enqueue(elem cascadeQueueElement) {
	*q = append((*q), elem)
}

func (q *cascadeQueue) dequeue() (cascadeQueueElement, bool) {
	if len(*q) == 0 {
		return cascadeQueueElement{}, false
	}
	elem := (*q)[0]
	*q = (*q)[1:]
	return elem, true
}

// cascadeAll performs all required cascading operations, then checks all the
// remaining indexes to ensure that no orphans were created.
func (c *cascader) cascadeAll(
	ctx context.Context, table *TableDescriptor, originalValues tree.Datums, traceKV bool,
) error {
	// Perform all the required cascading operations.
	var cascadeQ cascadeQueue
	cascadeQ.enqueue(cascadeQueueElement{table, []tree.Datums{originalValues}})
	for {
		elem, exists := cascadeQ.dequeue()
		if !exists {
			break
		}
		for _, idx := range elem.table.AllNonDropIndexes() {
			for _, ref := range idx.ReferencedBy {
				referencedTable, ok := c.tablesByID[ref.Table]
				if !ok {
					return errors.Errorf("Could not find table:%d in table descriptor map", ref.Table)
				}
				if referencedTable.IsAdding {
					// We can assume that a table being added but not yet public is empty,
					// and thus does not need to be checked for cascading.
					continue
				}
				index, err := referencedTable.Table.FindIndexByID(ref.Index)
				if err != nil {
					return err
				}
				if index.ForeignKey.OnDelete == ForeignKeyReference_CASCADE {
					returnedValues, err := c.deleteRow(ctx, referencedTable.Table, index, elem.values, traceKV)
					if err != nil {
						return err
					}
					if len(returnedValues) > 0 {
						// If a row was deleted, add the table to the queue.
						cascadeQ.enqueue(cascadeQueueElement{
							table:  referencedTable.Table,
							values: returnedValues,
						})
					}
				}
			}
		}
	}

	// Check all values to ensure there are no orphans.
	for tableID, removedValues := range c.rowsToCheck {
		if len(removedValues) == 0 {
			continue
		}
		rowDeleter, exists := c.rowDeleters[tableID]
		if !exists {
			return errors.Errorf("Could not find row deleter for table %d", tableID)
		}
		for _, removedValue := range removedValues {
			if err := rowDeleter.Fks.checkAll(ctx, removedValue); err != nil {
				return err
			}
		}
	}

	return nil
}
