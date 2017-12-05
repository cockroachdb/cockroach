// Copyright 2017 The Cockroach Authors.
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
	"fmt"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// cascader is used to handle all referential integrity cascading actions.
type cascader struct {
	txn                *client.Txn
	tablesByID         TableLookupsByID                   // TablesDescriptors by Table ID
	indexRowFetchers   map[ID]map[IndexID]MultiRowFetcher // RowFetchers by Table ID and Index ID
	rowDeleters        map[ID]RowDeleter                  // RowDeleters by Table ID
	deleterRowFetchers map[ID]MultiRowFetcher             // RowFetchers for rowDeleters by Table ID
	rowsChanged        map[ID]*RowContainer               // Rows that have been altered by Table ID
	alloc              *DatumAlloc
}

func makeCascader(txn *client.Txn, tablesByID TableLookupsByID, alloc *DatumAlloc) *cascader {
	return &cascader{
		txn:                txn,
		tablesByID:         tablesByID,
		indexRowFetchers:   make(map[ID]map[IndexID]MultiRowFetcher),
		rowDeleters:        make(map[ID]RowDeleter),
		deleterRowFetchers: make(map[ID]MultiRowFetcher),
		rowsChanged:        make(map[ID]*RowContainer),
		alloc:              alloc,
	}
}

func (c *cascader) close(ctx context.Context) {
	for _, container := range c.rowsChanged {
		container.Close(ctx)
	}
}

// spanForIndexValues creates a span against an index to extract the primary
// keys needed for cascading.
func spanForIndexValues(
	table *TableDescriptor,
	index *IndexDescriptor,
	prefixLen int,
	indexColIDs map[ColumnID]int,
	values []tree.Datum,
	keyPrefix []byte,
) (roachpb.Span, error) {
	keyBytes, _, err := EncodePartialIndexKey(table, index, prefixLen, indexColIDs, values, keyPrefix)
	if err != nil {
		return roachpb.Span{}, err
	}
	key := roachpb.Key(keyBytes)
	if index.ID == table.PrimaryIndex.ID {
		return roachpb.Span{Key: key, EndKey: encoding.EncodeInterleavedSentinel(key)}, nil
	}
	return roachpb.Span{Key: key, EndKey: key.PrefixEnd()}, nil
}

// batchRequestForIndexValues creates a batch request against an index to
// extract the primary keys needed for cascading.
func batchRequestForIndexValues(
	ctx context.Context,
	referencedIndex *IndexDescriptor,
	referencingTable *TableDescriptor,
	referencingIndex *IndexDescriptor,
	values cascadeQueueElement,
) (roachpb.BatchRequest, error) {

	//TODO(bram): consider caching some of these values
	keyPrefix := MakeIndexKeyPrefix(referencingTable, referencingIndex.ID)
	prefixLen := len(referencingIndex.ColumnIDs)
	if len(referencedIndex.ColumnIDs) < prefixLen {
		prefixLen = len(referencedIndex.ColumnIDs)
	}
	indexColIDs := make(map[ColumnID]int, len(referencedIndex.ColumnIDs))
	for i, referencedColID := range referencedIndex.ColumnIDs[:prefixLen] {
		if found, ok := values.colIDtoRowIndex[referencedColID]; ok {
			indexColIDs[referencingIndex.ColumnIDs[i]] = found
		} else {
			return roachpb.BatchRequest{}, errors.Errorf(
				"missing value for column %q in multi-part foreign key", referencedIndex.ColumnNames[i],
			)
		}
	}

	var req roachpb.BatchRequest
	for i := values.startIndex; i < values.endIndex; i++ {
		span, err := spanForIndexValues(
			referencingTable, referencingIndex, prefixLen, indexColIDs, values.values.At(i), keyPrefix,
		)
		if err != nil {
			return roachpb.BatchRequest{}, err
		}
		req.Add(&roachpb.ScanRequest{Span: span})
	}
	return req, nil
}

// spanForPKValues creates a span against the primary index of a table and is
// used to fetch rows for cascading.
func spanForPKValues(
	table *TableDescriptor, fetchColIDtoRowIndex map[ColumnID]int, values tree.Datums,
) (roachpb.Span, error) {
	return spanForIndexValues(
		table,
		&table.PrimaryIndex,
		len(table.PrimaryIndex.ColumnIDs),
		fetchColIDtoRowIndex,
		values,
		MakeIndexKeyPrefix(table, table.PrimaryIndex.ID),
	)
}

// batchRequestForPKValues creates a batch request against the primary index of
// a table and is used to fetch rows for cascading.
func batchRequestForPKValues(
	table *TableDescriptor, fetchColIDtoRowIndex map[ColumnID]int, values *RowContainer,
) (roachpb.BatchRequest, error) {
	var req roachpb.BatchRequest
	for i := 0; i < values.Len(); i++ {
		span, err := spanForPKValues(table, fetchColIDtoRowIndex, values.At(i))
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
		false, /* isCheck */
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
		false, /* isCheck */
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

// deleteRows performs row deletions on a single table for all rows that match
// the values. Returns the values of the rows that were deleted. This deletion
// happens in a single batch.
func (c *cascader) deleteRows(
	ctx context.Context,
	referencedIndex *IndexDescriptor,
	referencingTable *TableDescriptor,
	referencingIndex *IndexDescriptor,
	values cascadeQueueElement,
	mon *mon.BytesMonitor,
	traceKV bool,
) (*RowContainer, map[ColumnID]int, int, error) {
	// Create the span to search for index values.
	// TODO(bram): This initial index lookup can be skipped if the index is the
	// primary index.
	if traceKV {
		log.VEventf(ctx, 2,
			"cascading delete from refIndex:%s, into table:%s, using index:%s for values:%+v",
			referencedIndex.Name, referencingTable.Name, referencingIndex.Name, values.values.chunks,
		)
	}
	req, err := batchRequestForIndexValues(
		ctx, referencedIndex, referencingTable, referencingIndex, values,
	)
	if err != nil {
		return nil, nil, 0, err
	}
	br, roachErr := c.txn.Send(ctx, req)
	if roachErr != nil {
		return nil, nil, 0, roachErr.GoError()
	}

	// Create or retrieve the index row fetcher.
	indexRowFetcher, err := c.addIndexRowFetcher(referencingTable, referencingIndex)
	if err != nil {
		return nil, nil, 0, err
	}

	// Fetch all the primary keys that need to be deleted.
	// TODO(Bram): consider chunking this into n, primary keys, perhaps 100.
	pkColTypeInfo, err := makeColTypeInfo(referencingTable, indexRowFetcher.tables[0].colIdxMap)
	if err != nil {
		return nil, nil, 0, err
	}
	primaryKeysToDelete := NewRowContainer(mon.MakeBoundAccount(), pkColTypeInfo, values.values.Len())
	defer primaryKeysToDelete.Close(ctx)

	for _, resp := range br.Responses {
		fetcher := spanKVFetcher{
			kvs: resp.GetInner().(*roachpb.ScanResponse).Rows,
		}
		if err := indexRowFetcher.StartScanFrom(ctx, &fetcher); err != nil {
			return nil, nil, 0, err
		}
		for !indexRowFetcher.kvEnd {
			primaryKey, _, _, err := indexRowFetcher.NextRowDecoded(ctx)
			if err != nil {
				return nil, nil, 0, err
			}
			if _, err := primaryKeysToDelete.AddRow(ctx, primaryKey); err != nil {
				return nil, nil, 0, err
			}
		}
	}

	// Early exit if no rows need to be deleted.
	if primaryKeysToDelete.Len() == 0 {
		return nil, nil, 0, nil
	}

	// Create or retrieve the row deleter and primary index row fetcher.
	rowDeleter, pkRowFetcher, err := c.addRowDeleter(referencingTable)
	if err != nil {
		return nil, nil, 0, err
	}

	// Create a batch request to get all the spans of the primary keys that need
	// to be deleted.
	pkLookupReq, err := batchRequestForPKValues(
		referencingTable, rowDeleter.FetchColIDtoRowIndex, primaryKeysToDelete,
	)
	if err != nil {
		return nil, nil, 0, err
	}
	primaryKeysToDelete.Clear(ctx)
	pkResp, roachErr := c.txn.Send(ctx, pkLookupReq)
	if roachErr != nil {
		return nil, nil, 0, roachErr.GoError()
	}

	// Add the values to be checked for constraint violations after all cascading
	// changes have completed. Here either fetch or create the rowContainer for
	// the table. This rowContainer for the table is also used by the queue to
	// avoid having to double the memory used.
	if _, exists := c.rowsChanged[referencingTable.ID]; !exists {
		// Fetch the rows for deletion and store them in a container.
		colTypeInfo, err := makeColTypeInfo(referencingTable, rowDeleter.FetchColIDtoRowIndex)
		if err != nil {
			return nil, nil, 0, err
		}
		c.rowsChanged[referencingTable.ID] = NewRowContainer(
			mon.MakeBoundAccount(), colTypeInfo, primaryKeysToDelete.Len(),
		)
	}
	rowsChanged := c.rowsChanged[referencingTable.ID]
	deletedRowsStartIndex := rowsChanged.Len()

	// Delete all the rows in a new batch.
	deleteBatch := c.txn.NewBatch()

	for _, resp := range pkResp.Responses {
		fetcher := spanKVFetcher{
			kvs: resp.GetInner().(*roachpb.ScanResponse).Rows,
		}
		if err := pkRowFetcher.StartScanFrom(ctx, &fetcher); err != nil {
			return nil, nil, 0, err
		}
		for !pkRowFetcher.kvEnd {
			rowToDelete, _, _, err := pkRowFetcher.NextRowDecoded(ctx)
			if err != nil {
				return nil, nil, 0, err
			}

			// Add the row to be checked for consistency changes.
			if _, err := rowsChanged.AddRow(ctx, rowToDelete); err != nil {
				return nil, nil, 0, err
			}

			// Delete the row.
			if err := rowDeleter.deleteRowNoCascade(ctx, deleteBatch, rowToDelete, traceKV); err != nil {
				return nil, nil, 0, err
			}
		}
	}

	// Run the batch.
	if err := c.txn.Run(ctx, deleteBatch); err != nil {
		return nil, nil, 0, err
	}

	return rowsChanged, rowDeleter.FetchColIDtoRowIndex, deletedRowsStartIndex, nil
}

type cascadeQueueElement struct {
	table  *TableDescriptor
	values *RowContainer // This row container is actually defined elsewhere and
	// its memory is not managed by the queue.
	colIDtoRowIndex map[ColumnID]int
	startIndex      int // Start of the range of rows in the row container.
	endIndex        int // End of the range of rows (exclusive) in the row container.
}

// cascadeQueue is used for a breadth first walk of the referential integrity
// graph.
type cascadeQueue []cascadeQueueElement

// Enqueue adds a range of values in a row container to the queue. Note that
// it always assumes that all the values start at the startIndex and extend to
// all the rows following that index.
func (q *cascadeQueue) enqueue(
	ctx context.Context,
	table *TableDescriptor,
	rowContainer *RowContainer,
	colIDtoRowIndex map[ColumnID]int,
	startIndex int,
) error {
	*q = append(*q, cascadeQueueElement{
		table:           table,
		values:          rowContainer,
		colIDtoRowIndex: colIDtoRowIndex,
		startIndex:      startIndex,
		endIndex:        rowContainer.Len(),
	})
	return nil
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
	ctx context.Context,
	table *TableDescriptor,
	originalValues tree.Datums,
	colIDtoRowIndex map[ColumnID]int,
	mon *mon.BytesMonitor,
	traceKV bool,
) error {
	defer c.close(ctx)
	// Perform all the required cascading operations.
	var cascadeQ cascadeQueue

	// Enqueue the first values.
	colTypeInfo, err := makeColTypeInfo(table, colIDtoRowIndex)
	if err != nil {
		return err
	}
	originalRowContainer := NewRowContainer(mon.MakeBoundAccount(), colTypeInfo, len(originalValues))
	defer originalRowContainer.Close(ctx)
	if _, err := originalRowContainer.AddRow(ctx, originalValues); err != nil {
		return err
	}
	if err := cascadeQ.enqueue(ctx, table, originalRowContainer, colIDtoRowIndex, 0); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return NewQueryCanceledError()
		default:
		}
		elem, exists := cascadeQ.dequeue()
		if !exists {
			break
		}
		if traceKV {
			log.VEventf(ctx, 2, "cascading into %s for values:%s", elem.table.Name, elem.values.chunks)
		}
		for _, referencedIndex := range elem.table.AllNonDropIndexes() {
			for _, ref := range referencedIndex.ReferencedBy {
				referencingTable, ok := c.tablesByID[ref.Table]
				if !ok {
					return errors.Errorf("Could not find table:%d in table descriptor map", ref.Table)
				}
				if referencingTable.IsAdding {
					// We can assume that a table being added but not yet public is empty,
					// and thus does not need to be checked for cascading.
					continue
				}
				referencingIndex, err := referencingTable.Table.FindIndexByID(ref.Index)
				if err != nil {
					return err
				}
				if referencingIndex.ForeignKey.OnDelete == ForeignKeyReference_CASCADE {
					returnedValuesContainer, colIDtoRowIndex, startIndex, err := c.deleteRows(
						ctx,
						&referencedIndex,
						referencingTable.Table,
						referencingIndex,
						elem,
						mon,
						traceKV,
					)
					if err != nil {
						return err
					}
					if returnedValuesContainer != nil && returnedValuesContainer.Len() > startIndex {
						// If a row was deleted, add the table to the queue.
						if err := cascadeQ.enqueue(
							ctx, referencingTable.Table, returnedValuesContainer, colIDtoRowIndex, startIndex,
						); err != nil {
							return err
						}
					}
				}
			}
		}
	}

	// Check all values to ensure there are no orphans.
	for tableID, removedRowContainer := range c.rowsChanged {
		if removedRowContainer.Len() == 0 {
			continue
		}
		rowDeleter, exists := c.rowDeleters[tableID]
		if !exists {
			return pgerror.NewError(pgerror.CodeInternalError,
				fmt.Sprintf("could not find row deleter for table %d", tableID),
			)
		}
		for removedRowContainer.Len() > 0 {
			// TODO(bram): Can these be batched?
			if err := rowDeleter.Fks.checkAll(ctx, removedRowContainer.At(0)); err != nil {
				return err
			}
			removedRowContainer.PopFirst()
		}
	}

	return nil
}
