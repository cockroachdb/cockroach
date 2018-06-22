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

	tables TableLookupsByID
	dir    FKCheck
	batch  roachpb.BatchRequest
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
		evalCtx:    evalCtx,
		txn:        txn,
		tables:     otherTables,
		writeTable: writeTable,
		writeIdx:   writeIdx,
		dir:        dir,
	}

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

Outer:
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
			continue Outer
		}

		tableIdxInfo.searchPrefixes = append(tableIdxInfo.searchPrefixes, searchPrefix)
		tableIdxInfo.searchIdxs = append(tableIdxInfo.searchIdxs, searchIdx)
		tableIdxInfo.prefixLens = append(tableIdxInfo.prefixLens, prefixLen)
		tableIdxInfo.rowFetchers[searchIdx.ID] = rowFetcher

		fkHelper.tableIdxInfo[ref.Table] = tableIdxInfo
	}
	return fkHelper, nil
}

func (fk *FKHelper) addCheck() {

}

func (fk *FKHelper) runChecks() {

}
