// Copyright 2018 The Cockroach Authors.
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
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type indexLookup struct {
	idx          *IndexDescriptor
	searchPrefix []byte
	prefixLen    int

	rowFetcher    RowFetcher
	colIDToRowIdx map[ColumnID]int
}

type indexLookupByID map[IndexID]indexLookup

type fkInfo struct {
	writeTableID  ID
	writeIdxID    IndexID
	searchTableID ID
	searchIndexID IndexID
}

type writeIdxInfo struct {
	idx           IndexDescriptor
	searchIdxInfo map[ID]indexLookupByID
}

type writeIdxs map[IndexID]writeIdxInfo

type writeTableInfo struct {
	table     TableDescriptor
	oldRows   *RowContainer
	newRows   *RowContainer
	writeIdxs writeIdxs
}

type writeTables map[ID]writeTableInfo

// FKHelper accumulates foreign key checks
type FKHelper struct {
	evalCtx *tree.EvalContext
	txn     *client.Txn

	writeTables writeTables
	//writeIdxs   map[IndexID]writeIdxInfo

	Tables          TableLookupsByID
	dir             FKCheck
	batch           roachpb.BatchRequest
	batchToORIdx    []int
	batchToNRIdx    []int
	batchToCheckIdx []fkInfo
	alloc           *DatumAlloc
}

func makeFKHelper(
	evalCtx *tree.EvalContext,
	txn *client.Txn,
	otherTables TableLookupsByID,
	writeTable TableDescriptor,
	colMap map[ColumnID]int,
	alloc *DatumAlloc,
	dir FKCheck,
) (FKHelper, error) {

	fkHelper := FKHelper{
		evalCtx:      evalCtx,
		txn:          txn,
		Tables:       otherTables,
		writeTables:  make(writeTables),
		dir:          dir,
		batchToORIdx: make([]int, 0),
		batchToNRIdx: make([]int, 0),
		alloc:        alloc,
	}
	if err := fkHelper.initializeTableInfo(writeTable, colMap, dir); err != nil {
		return FKHelper{}, nil
	}
	return fkHelper, nil
}

func (fk *FKHelper) addFKRefInfo(writeTableID ID, idx IndexDescriptor, otherTables TableLookupsByID, colMap map[ColumnID]int, alloc *DatumAlloc, ref ForeignKeyReference) error {
	writeIdxStruct := fk.writeTables[writeTableID].writeIdxs[idx.ID]
	writeIdx := writeIdxStruct.idx
	if _, ok := writeIdxStruct.searchIdxInfo[ref.Table]; !ok {
		writeIdxStruct.searchIdxInfo[ref.Table] = make(indexLookupByID)
	}
	searchTable := otherTables[ref.Table].Table
	searchPrefix := MakeIndexKeyPrefix(searchTable, ref.Index)
	searchIdx, err := searchTable.FindIndexByID(ref.Index)
	if err != nil {
		return err
	}

	prefixLen := len(searchIdx.ColumnIDs)
	if len(writeIdx.ColumnIDs) < prefixLen {
		prefixLen = len(writeIdx.ColumnIDs)
	}

	colIDToRowIdx := make(map[ColumnID]int, len(writeIdx.ColumnIDs))

	nulls := true
	for i, writeColID := range writeIdx.ColumnIDs[:prefixLen] {
		if found, ok := colMap[writeColID]; ok {
			colIDToRowIdx[searchIdx.ColumnIDs[i]] = found
			nulls = false
		} else if !nulls {
			return errors.Errorf("missing value for column %q in multi-part foreign key", writeIdx.ColumnNames[i])
		}
	}
	if nulls {
		return nil
	}

	tableArgs := RowFetcherTableArgs{
		Desc:             searchTable,
		Index:            searchIdx,
		ColIdxMap:        searchTable.ColumnIdxMap(),
		IsSecondaryIndex: searchIdx.ID != searchTable.PrimaryIndex.ID,
		Cols:             searchTable.Columns,
	}
	var rowFetcher RowFetcher
	if err := rowFetcher.Init(
		false, /* reverse */
		false, /* returnRangeInfo */
		false, /* isCheck */
		alloc,
		tableArgs,
	); err != nil {
		return err
	}

	writeIdxStruct.searchIdxInfo[ref.Table][searchIdx.ID] = indexLookup{
		idx:           searchIdx,
		searchPrefix:  searchPrefix,
		prefixLen:     prefixLen,
		rowFetcher:    rowFetcher,
		colIDToRowIdx: colIDToRowIdx,
	}

	fk.writeTables[writeTableID].writeIdxs[idx.ID] = writeIdxStruct

	return nil
}

func (fk *FKHelper) getSpans(row tree.Datums, tableID ID) ([]roachpb.Span, []fkInfo, error) {
	var spans []roachpb.Span
	var indexesInfo []fkInfo
	for _, tableInfo := range fk.writeTables {
		for writeIdxID, writeIdxInfo := range tableInfo.writeIdxs {
			for id, info := range writeIdxInfo.searchIdxInfo {
				for indexID, searchIdxLookup := range info {

					searchIdx := searchIdxLookup.idx

					nulls := true
					for _, colID := range searchIdx.ColumnIDs[:searchIdxLookup.prefixLen] {
						found, ok := searchIdxLookup.colIDToRowIdx[colID]
						if !ok {
							panic(fmt.Sprintf("fk ids (%v) missing column id %d", searchIdxLookup.colIDToRowIdx, colID))
						}
						if row[found] != tree.DNull {
							nulls = false
							break
						}
					}
					if nulls {
						continue
					}

					var key roachpb.Key
					if row != nil {
						keyBytes, _, err := EncodePartialIndexKey(fk.Tables[id].Table, searchIdxLookup.idx, searchIdxLookup.prefixLen, searchIdxLookup.colIDToRowIdx, row, searchIdxLookup.searchPrefix)
						if err != nil {
							return spans, indexesInfo, err
						}
						key = roachpb.Key(keyBytes)
					} else {
						key = roachpb.Key(searchIdxLookup.searchPrefix)
					}
					spans = append(spans, roachpb.Span{Key: key, EndKey: key.PrefixEnd()})
					indexesInfo = append(indexesInfo,
						fkInfo{
							writeTableID:  tableID,
							writeIdxID:    writeIdxID,
							searchTableID: id,
							searchIndexID: indexID,
						})
				}
			}
		}
	}
	return spans, indexesInfo, nil
}

func (fk *FKHelper) initializeTableInfo(writeTable TableDescriptor, colMap map[ColumnID]int, dir FKCheck) error {
	tableID := writeTable.ID
	if _, ok := fk.writeTables[tableID]; !ok {
		colInfo, err := makeColTypeInfo(&writeTable, colMap)
		if err != nil {
			return err
		}
		fk.writeTables[tableID] = writeTableInfo{
			table:     writeTable,
			oldRows:   NewRowContainer(fk.evalCtx.Mon.MakeBoundAccount(), colInfo, len(writeTable.Columns)),
			newRows:   NewRowContainer(fk.evalCtx.Mon.MakeBoundAccount(), colInfo, len(writeTable.Columns)),
			writeIdxs: make(writeIdxs),
		}
	}

	for _, idx := range writeTable.AllNonDropIndexes() {
		writeIdxInfo := fk.writeTables[writeTable.ID].writeIdxs[idx.ID]
		writeIdxInfo.idx = idx
		writeIdxInfo.searchIdxInfo = make(map[ID]indexLookupByID)
		fk.writeTables[writeTable.ID].writeIdxs[idx.ID] = writeIdxInfo
		if dir == CheckInserts || dir == CheckUpdates {
			if idx.ForeignKey.IsSet() {
				ref := idx.ForeignKey
				if err := fk.addFKRefInfo(writeTable.ID, idx, fk.Tables, colMap, fk.alloc, ref); err != nil {
					return err
				}
			}
		}
		if dir == CheckDeletes || dir == CheckUpdates {
			for _, ref := range idx.ReferencedBy {
				if fk.Tables[ref.Table].IsAdding {
					continue
				}
				if err := fk.addFKRefInfo(writeTable.ID, idx, fk.Tables, colMap, fk.alloc, ref); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// AddChecks prepares all the checks for a particular row.
func (fk *FKHelper) AddChecks(ctx context.Context, table TableDescriptor, colMap map[ColumnID]int, dir FKCheck, oldRow tree.Datums, newRow tree.Datums) error {

	tableID := table.ID
	if _, ok := fk.writeTables[tableID]; !ok {
		fk.initializeTableInfo(table, colMap, dir)
	}

	oldRowIdx := -1
	newRowIdx := -1

	if oldRow != nil {
		fk.writeTables[tableID].oldRows.AddRow(ctx, oldRow)
		oldRowIdx = fk.writeTables[tableID].oldRows.Len() - 1
	}
	if newRow != nil {
		fk.writeTables[tableID].newRows.AddRow(ctx, oldRow)
		newRowIdx = fk.writeTables[tableID].newRows.Len() - 1
	}
	spans, indexesInfo, err := fk.getSpans(oldRow, table.ID)
	if err != nil {
		return err
	}

	r := roachpb.RequestUnion{}
	for i, span := range spans {
		scan := roachpb.ScanRequest{
			RequestHeader: roachpb.RequestHeaderFromSpan(span),
		}
		r.MustSetInner(&scan)
		fk.batch.Requests = append(fk.batch.Requests, r)
		fk.batchToORIdx = append(fk.batchToORIdx, oldRowIdx)
		fk.batchToNRIdx = append(fk.batchToNRIdx, newRowIdx)
		fk.batchToCheckIdx = append(fk.batchToCheckIdx, indexesInfo[i])
	}

	return nil
}

func (fk *FKHelper) reset(ctx context.Context) {
	fk.batch.Reset()
	for _, tableInfo := range fk.writeTables {
		tableInfo.oldRows.Clear(ctx)
		tableInfo.newRows.Clear(ctx)
	}
	fk.batchToCheckIdx = fk.batchToCheckIdx[:0]
	fk.batchToORIdx = fk.batchToORIdx[:0]
	fk.batchToNRIdx = fk.batchToNRIdx[:0]
}

// Shutdown shuts down the fk helper completely
func (fk *FKHelper) Shutdown(ctx context.Context) {
	for _, tableInfo := range fk.writeTables {
		tableInfo.oldRows.Close(ctx)
		tableInfo.newRows.Close(ctx)
	}
}

// RunChecks runs all accumulated checks.
func (fk *FKHelper) RunChecks(ctx context.Context) error {
	if len(fk.batch.Requests) == 0 {
		return nil
	}

	defer fk.reset(ctx)

	br, err := fk.txn.Send(ctx, fk.batch)
	if err != nil {
		return err.GoError()
	}

	fetcher := SpanKVFetcher{}

	for i, resp := range br.Responses {
		fetcher.KVs = resp.GetInner().(*roachpb.ScanResponse).Rows
		writeTableID := fk.batchToCheckIdx[i].writeTableID
		writeIdxID := fk.batchToCheckIdx[i].writeIdxID
		tableID := fk.batchToCheckIdx[i].searchTableID
		indexID := fk.batchToCheckIdx[i].searchIndexID

		indexLookup := fk.writeTables[writeTableID].writeIdxs[writeIdxID].searchIdxInfo[tableID][indexID]
		writeIdx := fk.writeTables[writeTableID].writeIdxs[writeIdxID].idx

		searchIdx := indexLookup.idx
		rf := indexLookup.rowFetcher
		prefixLen := indexLookup.prefixLen
		colIDToRowIdx := indexLookup.colIDToRowIdx
		tableName := fk.Tables[tableID].Table.Name
		if err := rf.StartScanFrom(ctx, &fetcher); err != nil {
			return err
		}
		switch fk.dir {
		case CheckInserts:
			if rf.kvEnd {
				newRow := fk.writeTables[writeTableID].newRows.At(fk.batchToNRIdx[i])
				fkValues := make(tree.Datums, prefixLen)
				for valueIdx, colID := range searchIdx.ColumnIDs[:prefixLen] {
					fkValues[valueIdx] = newRow[colIDToRowIdx[colID]]
				}
				return pgerror.NewErrorf(pgerror.CodeForeignKeyViolationError,
					"foreign key violation: value %s not found in %s@%s %s (txn=%s)",
					fkValues, tableName, searchIdx.Name, searchIdx.ColumnNames[:prefixLen], fk.txn.Proto())
			}
		case CheckDeletes:
			if !rf.kvEnd {
				if fk.batchToORIdx[i] == -1 {
					return pgerror.NewErrorf(pgerror.CodeForeignKeyViolationError,
						"foreign key violation: non-empty columns %s referenced in table %q",
						fk.writeTables[writeTableID].writeIdxs[writeIdxID].idx.ColumnNames[:prefixLen], tableName)
				}
				oldRow := fk.writeTables[writeTableID].oldRows.At(fk.batchToORIdx[i])
				fkValues := make(tree.Datums, prefixLen)
				for valueIdx, colID := range searchIdx.ColumnIDs[:prefixLen] {
					fkValues[valueIdx] = oldRow[colIDToRowIdx[colID]]
				}
				return pgerror.NewErrorf(pgerror.CodeForeignKeyViolationError,
					"foreign key violation: values %v in columns %s referenced in table %q",
					fkValues, writeIdx.ColumnNames[:prefixLen], tableName)
			}

		default:
			log.Fatalf(ctx, "impossible case: FKHelper has dir=%v", fk.dir)
		}

	}

	return nil
}

// TablesNeededForFKsNew populates a map of TableLookupsByID for all the
// TableDescriptors that might be needed when performing FK checking for delete
// and/or insert operations. It uses the passed in lookup function to perform
// the actual lookup. The AnalyzeExpr function, if provided, is used to
// initialize the CheckHelper, and this requires that the TableLookupFunction
// and CheckPrivilegeFunction are provided and not just placeholder functions
// as well. If an operation may include a cascading operation then the
// CheckHelpers are required.
func TablesNeededForFKsNew(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	txn *client.Txn,
	table TableDescriptor,
	usage FKCheck,
	lookup TableLookupFunction,
	checkPrivilege CheckPrivilegeFunction,
	analyzeExpr AnalyzeExprFunction,
	colMap map[ColumnID]int,
	alloc *DatumAlloc,
) (FKHelper, error) {
	queue := tableLookupQueue{
		tableLookups:   make(TableLookupsByID),
		alreadyChecked: make(map[ID]map[FKCheck]struct{}),
		lookup:         lookup,
		checkPrivilege: checkPrivilege,
		analyzeExpr:    analyzeExpr,
	}
	// Add the passed in table descriptor to the table lookup.
	baseTableLookup := TableLookup{Table: &table}
	if err := baseTableLookup.addCheckHelper(ctx, analyzeExpr); err != nil {
		return FKHelper{}, err
	}
	queue.tableLookups[table.ID] = baseTableLookup
	if err := queue.enqueue(ctx, table.ID, usage); err != nil {
		return FKHelper{}, err
	}
	for {
		tableLookup, curUsage, exists := queue.dequeue()
		if !exists {
			fkHelper, err := makeFKHelper(
				evalCtx,
				txn,
				queue.tableLookups,
				table,
				colMap,
				alloc,
				usage,
			)
			if err != nil {
				return FKHelper{}, err
			}
			return fkHelper, nil
		}
		// If the table descriptor is nil it means that there was no actual lookup
		// performed. Meaning there is no need to walk any secondary relationships
		// and the table descriptor lookup will happen later.
		if tableLookup.IsAdding || tableLookup.Table == nil {
			continue
		}
		for _, idx := range tableLookup.Table.AllNonDropIndexes() {
			if curUsage == CheckInserts || curUsage == CheckUpdates {
				if idx.ForeignKey.IsSet() {
					if _, err := queue.getTable(ctx, idx.ForeignKey.Table); err != nil {
						return FKHelper{}, err
					}
				}
			}
			if curUsage == CheckDeletes || curUsage == CheckUpdates {
				for _, ref := range idx.ReferencedBy {
					// The table being referenced is required to know the relationship, so
					// fetch it here.
					referencedTableLookup, err := queue.getTable(ctx, ref.Table)
					if err != nil {
						return FKHelper{}, err
					}
					// Again here if the table descriptor is nil it means that there was
					// no actual lookup performed. Meaning there is no need to walk any
					// secondary relationships.
					if referencedTableLookup.IsAdding || referencedTableLookup.Table == nil {
						continue
					}
					referencedIdx, err := referencedTableLookup.Table.FindIndexByID(ref.Index)
					if err != nil {
						return FKHelper{}, err
					}
					if curUsage == CheckDeletes {
						var nextUsage FKCheck
						switch referencedIdx.ForeignKey.OnDelete {
						case ForeignKeyReference_CASCADE:
							nextUsage = CheckDeletes
						case ForeignKeyReference_SET_DEFAULT, ForeignKeyReference_SET_NULL:
							nextUsage = CheckUpdates
						default:
							// There is no need to check any other relationships.
							continue
						}
						if err := queue.enqueue(ctx, referencedTableLookup.Table.ID, nextUsage); err != nil {
							return FKHelper{}, err
						}
					} else {
						// curUsage == CheckUpdates
						if referencedIdx.ForeignKey.OnUpdate == ForeignKeyReference_CASCADE ||
							referencedIdx.ForeignKey.OnUpdate == ForeignKeyReference_SET_DEFAULT ||
							referencedIdx.ForeignKey.OnUpdate == ForeignKeyReference_SET_NULL {
							if err := queue.enqueue(
								ctx, referencedTableLookup.Table.ID, CheckUpdates,
							); err != nil {
								return FKHelper{}, err
							}
						}
					}
				}
			}
		}
	}
}
