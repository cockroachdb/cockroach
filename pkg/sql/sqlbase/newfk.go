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

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type tableSearchIdxInfo struct {
	searchIdxs     []*IndexDescriptor
	searchPrefixes [][]byte
	prefixLens     []int

	rowFetchers   map[IndexID]RowFetcher
	colIDToRowIdx map[IndexID]map[ColumnID]int
}

// FKHelper accumulates foreign key checks
type FKHelper struct {
	evalCtx *tree.EvalContext
	txn     *client.Txn

	writeIdx   IndexDescriptor
	writeTable TableDescriptor

	tableIdxInfo map[ID]tableSearchIdxInfo

	oldRows *RowContainer
	newRows *RowContainer

	tables       TableLookupsByID
	dir          FKCheck
	batch        roachpb.BatchRequest
	batchToORIdx []int
	batchToNRIdx []int
}

func makeFKHelper(
	evalCtx *tree.EvalContext,
	txn *client.Txn,
	otherTables TableLookupsByID,
	writeTable TableDescriptor,
	writeIdx IndexDescriptor,
	colMap map[ColumnID]int,
	alloc *DatumAlloc,
	dir FKCheck,
) (FKHelper, error) {

	fkHelper := FKHelper{
		evalCtx:      evalCtx,
		txn:          txn,
		tables:       otherTables,
		writeTable:   writeTable,
		writeIdx:     writeIdx,
		dir:          dir,
		batchToORIdx: make([]int, 0),
		batchToNRIdx: make([]int, 0),
	}

	colInfo, err := makeColTypeInfo(&writeTable, colMap)
	if err != nil {
		return FKHelper{}, err
	}
	fkHelper.oldRows.Init(evalCtx.Mon.MakeBoundAccount(), colInfo, 0)
	fkHelper.newRows.Init(evalCtx.Mon.MakeBoundAccount(), colInfo, 0)

	refs := make([]ForeignKeyReference, 0)

	for _, idx := range writeTable.AllNonDropIndexes() {
		switch dir {
		case CheckDeletes:
			for _, ref := range idx.ReferencedBy {
				if !otherTables[ref.Table].IsAdding {
					refs = append(refs, ref)
				}
			}
		case CheckInserts:
			if idx.ForeignKey.IsSet() {
				refs = append(refs, idx.ForeignKey)
			}
		case CheckUpdates:
			for _, ref := range idx.ReferencedBy {
				if !otherTables[ref.Table].IsAdding {
					refs = append(refs, ref)
				}
			}
			if idx.ForeignKey.IsSet() {
				refs = append(refs, idx.ForeignKey)
			}
		}
	}

	for _, ref := range refs {
		searchTable := otherTables[ref.Table].Table
		if _, ok := fkHelper.tableIdxInfo[ref.Table]; !ok {
			fkHelper.tableIdxInfo[ref.Table] = tableSearchIdxInfo{
				searchPrefixes: make([][]byte, 0),
				searchIdxs:     make([]*IndexDescriptor, 0),
				prefixLens:     make([]int, 0),
				rowFetchers:    make(map[IndexID]RowFetcher),
				colIDToRowIdx:  make(map[IndexID]map[ColumnID]int),
			}
		}
		tableIdxInfo := fkHelper.tableIdxInfo[ref.Table]
		searchPrefix := MakeIndexKeyPrefix(searchTable, ref.Index)
		searchIdx, err := searchTable.FindIndexByID(ref.Index)
		if err != nil {
			return FKHelper{}, nil
		}

		prefixLen := len(searchIdx.ColumnIDs)
		if len(writeIdx.ColumnIDs) < prefixLen {
			prefixLen = len(writeIdx.ColumnIDs)
		}

		tableIdxInfo.colIDToRowIdx[searchIdx.ID] = make(map[ColumnID]int, len(writeIdx.ColumnIDs))

		nulls := true
		for i, writeColID := range writeIdx.ColumnIDs[:prefixLen] {
			if found, ok := colMap[writeColID]; ok {
				tableIdxInfo.colIDToRowIdx[searchIdx.ID][searchIdx.ColumnIDs[i]] = found
				nulls = false
			} else if !nulls {
				return FKHelper{}, errors.Errorf("missing value for column %q in multi-part foreign key", writeIdx.ColumnNames[i])
			}
		}
		if nulls {
			continue
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
			return FKHelper{}, nil
		}

		tableIdxInfo.searchPrefixes = append(tableIdxInfo.searchPrefixes, searchPrefix)
		tableIdxInfo.searchIdxs = append(tableIdxInfo.searchIdxs, searchIdx)
		tableIdxInfo.prefixLens = append(tableIdxInfo.prefixLens, prefixLen)
		tableIdxInfo.rowFetchers[searchIdx.ID] = rowFetcher

		fkHelper.tableIdxInfo[ref.Table] = tableIdxInfo
	}
	return fkHelper, nil
}

func (fk *FKHelper) getSpans(row tree.Datums) ([]roachpb.Span, error) {
	var spans []roachpb.Span
	for id, info := range fk.tableIdxInfo {
		for i := 0; i < len(info.searchIdxs); i++ {
			var key roachpb.Key
			if row != nil {
				keyBytes, _, err := EncodePartialIndexKey(fk.tables[id].Table, info.searchIdxs[i], info.prefixLens[i], info.colIDToRowIdx[info.searchIdxs[i].ID], row, info.searchPrefixes[i])
				if err != nil {
					return spans, err
				}
				key = roachpb.Key(keyBytes)
			} else {
				key = roachpb.Key(info.searchPrefixes[i])
			}
			spans = append(spans, roachpb.Span{Key: key, EndKey: key.PrefixEnd()})
		}
	}
	return spans, nil
}

func (fk *FKHelper) addChecks(ctx context.Context, oldRow tree.Datums, newRow tree.Datums) error {

	oldRowIdx := -1
	newRowIdx := -1

	if oldRow != nil {
		fk.oldRows.AddRow(ctx, oldRow)
		oldRowIdx = fk.oldRows.Len() - 1
	}
	if newRow != nil {
		fk.newRows.AddRow(ctx, newRow)
		newRowIdx = fk.newRows.Len() - 1
	}
	spans, err := fk.getSpans(oldRow)
	if err != nil {
		return err
	}

	r := roachpb.RequestUnion{}
	for _, span := range spans {
		scan := roachpb.ScanRequest{
			RequestHeader: roachpb.RequestHeaderFromSpan(span),
		}
		r.MustSetInner(&scan)
		fk.batch.Requests = append(fk.batch.Requests, r)
		fk.batchToORIdx = append(fk.batchToORIdx, oldRowIdx)
		fk.batchToNRIdx = append(fk.batchToNRIdx, newRowIdx)
	}

	return nil
}

func (fk *FKHelper) runChecks() error {
	if len(fk.batch.Requests) == 0 {
		return nil
	}

	return nil
}
