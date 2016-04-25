// Copyright 2016 The Cockroach Authors.
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
// Author: Daniel Harrison (daniel.harrison@gmail.com)

package sql

import (
	"bytes"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/log"
)

// tableWriter handles writing kvs and forming table rows.
//
// Usage:
//   pErr := tw.init(txn)
//   // Handle pErr.
//   for {
//      var values []parser.Datum
//      values = ...
//      row, pErr := tw.run()
//      // Handle pErr.
//   }
//   pErr := tw.finalize()
//   // Handle pErr.
//
// finalize will flush any remaining writes, but note that the transaction may
// be written to after a batch of calls to run.
type tableWriter interface {
	init(*client.Txn) *roachpb.Error
	run(parser.DTuple) (parser.DTuple, *roachpb.Error)
	finalize() *roachpb.Error
}

var _ tableWriter = (*tableInserter)(nil)
var _ tableWriter = (*tableUpdater)(nil)
var _ tableWriter = (*tableUpserter)(nil)
var _ tableWriter = (*tableDeleter)(nil)

// tableInserter handles writing kvs and forming table rows for inserts.
type tableInserter struct {
	ri         rowInserter
	autoCommit bool

	// Set by init.
	txn *client.Txn
	b   *client.Batch
}

func (ti *tableInserter) init(txn *client.Txn) *roachpb.Error {
	ti.txn = txn
	ti.b = txn.NewBatch()
	return nil
}

func (ti *tableInserter) run(values parser.DTuple) (parser.DTuple, *roachpb.Error) {
	return nil, ti.ri.insertRow(ti.b, values)
}

func (ti *tableInserter) finalize() *roachpb.Error {
	var pErr *roachpb.Error
	if ti.autoCommit {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		pErr = ti.txn.CommitInBatch(ti.b)
	} else {
		pErr = ti.txn.Run(ti.b)
	}

	if pErr != nil {
		pErr = convertBatchError(ti.ri.helper.tableDesc, *ti.b, pErr)
	}
	return pErr
}

// tableUpdater handles writing kvs and forming table rows for updates.
type tableUpdater struct {
	ru         rowUpdater
	autoCommit bool

	// Set by init.
	txn *client.Txn
	b   *client.Batch
}

func (tu *tableUpdater) init(txn *client.Txn) *roachpb.Error {
	tu.txn = txn
	tu.b = txn.NewBatch()
	return nil
}

func (tu *tableUpdater) run(values parser.DTuple) (parser.DTuple, *roachpb.Error) {
	oldValues := values[:len(tu.ru.fetchCols)]
	updateValues := values[len(tu.ru.fetchCols):]
	return tu.ru.updateRow(tu.b, oldValues, updateValues)
}

func (tu *tableUpdater) finalize() *roachpb.Error {
	var pErr *roachpb.Error
	if tu.autoCommit {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		pErr = tu.txn.CommitInBatch(tu.b)
	} else {
		return tu.txn.Run(tu.b)
	}

	if pErr != nil {
		pErr = convertBatchError(tu.ru.helper.tableDesc, *tu.b, pErr)
	}
	return pErr
}

// tableUpserter handles writing kvs and forming table rows for upserts.
type tableUpserter struct {
	ri         rowInserter
	ru         rowUpdater
	autoCommit bool

	// Set by init.
	txn                   *client.Txn
	tableDesc             *TableDescriptor
	updateColIDtoRowIndex map[ColumnID]int
	fetcher               rowFetcher

	// Batched up in run/flush.
	upsertRows   []parser.DTuple
	upsertRowPKs []roachpb.Key

	// For allocation avoidance.
	indexKeyPrefix []byte
	updateRow      parser.DTuple
}

func (tu *tableUpserter) init(txn *client.Txn) *roachpb.Error {
	tu.txn = txn

	tu.tableDesc = tu.ri.helper.tableDesc
	tu.indexKeyPrefix = MakeIndexKeyPrefix(tu.tableDesc.ID, tu.tableDesc.PrimaryIndex.ID)

	tu.updateColIDtoRowIndex = make(map[ColumnID]int)
	for i, updateCol := range tu.ru.updateCols {
		tu.updateColIDtoRowIndex[updateCol.ID] = i
	}
	tu.updateRow = make(parser.DTuple, len(tu.updateColIDtoRowIndex))

	valNeededForCol := make([]bool, len(tu.ru.fetchCols))
	for i := range valNeededForCol {
		valNeededForCol[i] = true
	}
	if err := tu.fetcher.init(
		tu.tableDesc, tu.ru.fetchColIDtoRowIndex, &tu.tableDesc.PrimaryIndex,
		false, false, valNeededForCol,
	); err != nil {
		return roachpb.NewError(err)
	}
	return nil
}

func (tu *tableUpserter) run(row parser.DTuple) (parser.DTuple, *roachpb.Error) {
	tu.upsertRows = append(tu.upsertRows, row)

	// TODO(dan): The presence of OnConflict currently implies the short form
	// (primary index and update the values being inserted). Implement the long
	// form.
	upsertRowPK, _, err := encodeIndexKey(
		&tu.tableDesc.PrimaryIndex, tu.ri.insertColIDtoRowIndex, row, tu.indexKeyPrefix)
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	tu.upsertRowPKs = append(tu.upsertRowPKs, upsertRowPK)

	// TODO(dan): If len(tu.upsertRows) > some threshold, call flush().
	return nil, nil
}

func (tu *tableUpserter) flush() *roachpb.Error {
	pkSpans := make(spans, 0, len(tu.upsertRowPKs))
	for _, upsertRowPK := range tu.upsertRowPKs {
		if upsertRowPK != nil {
			pkSpans = append(pkSpans, span{start: upsertRowPK, end: upsertRowPK.PrefixEnd()})
		}
	}

	pErr := tu.fetcher.startScan(tu.txn, pkSpans, int64(len(pkSpans)))
	if pErr != nil {
		return pErr
	}

	existingRows := make([]parser.DTuple, len(tu.upsertRowPKs))
	spanIdx := 0
	for i, upsertRowPK := range tu.upsertRowPKs {
		if spanIdx >= len(pkSpans) {
			continue
		}
		pkSpan := pkSpans[spanIdx]
		spanIdx++
		if !bytes.Equal(pkSpan.start, upsertRowPK) {
			continue
		}
		row, pErr := tu.fetcher.nextRow()
		if pErr != nil {
			return pErr
		}
		existingRows[i] = row
	}

	b := tu.txn.NewBatch()
	for i, upsertRow := range tu.upsertRows {
		existingRow := existingRows[i]
		if existingRow == nil {
			pErr = tu.ri.insertRow(b, upsertRow)
			if pErr != nil {
				return pErr
			}
		} else {
			for uCol, uIdx := range tu.updateColIDtoRowIndex {
				tu.updateRow[uIdx] = upsertRow[tu.ri.insertColIDtoRowIndex[uCol]]
			}
			_, pErr := tu.ru.updateRow(b, existingRow, tu.updateRow)
			if pErr != nil {
				return pErr
			}
		}
	}
	tu.upsertRows = nil
	tu.upsertRowPKs = nil

	pErr = tu.txn.Run(b)
	if pErr != nil {
		pErr = convertBatchError(tu.tableDesc, *b, pErr)
	}
	return pErr
}

func (tu *tableUpserter) finalize() *roachpb.Error {
	return tu.flush()
}

// tableDeleter handles writing kvs and forming table rows for deletes.
type tableDeleter struct {
	rd         rowDeleter
	autoCommit bool

	// Set by init.
	txn *client.Txn
	b   *client.Batch
}

func (td *tableDeleter) init(txn *client.Txn) *roachpb.Error {
	td.txn = txn
	td.b = txn.NewBatch()
	return nil
}

func (td *tableDeleter) run(values parser.DTuple) (parser.DTuple, *roachpb.Error) {
	return nil, td.rd.deleteRow(td.b, values)
}

func (td *tableDeleter) finalize() *roachpb.Error {
	if td.autoCommit {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		return td.txn.CommitInBatch(td.b)
	}
	return td.txn.Run(td.b)
}

// fastPathAvailable returns true if the fastDelete optimization can be used.
func (td *tableDeleter) fastPathAvailable() bool {
	if len(td.rd.helper.indexes) != 0 {
		if log.V(2) {
			log.Infof("delete forced to scan: values required to update %d secondary indexes", len(td.rd.helper.indexes))
		}
		return false
	}
	return true
}

// fastDelete adds to the batch the kv operations necessary to delete sql rows
// without knowing the values that are currently present. fastDelete calls
// finalize, so it should not be called after.
//
func (td *tableDeleter) fastDelete(
	scan *scanNode,
) (rowCount int, pErr *roachpb.Error) {
	for _, span := range scan.spans {
		if log.V(2) {
			log.Infof("Skipping scan and just deleting %s - %s", span.start, span.end)
		}
		td.b.DelRange(span.start, span.end, true)
	}

	pErr = td.finalize()
	if pErr != nil {
		return 0, pErr
	}

	for _, r := range td.b.Results {
		var prev []byte
		for _, i := range r.Keys {
			// If prefix is same, don't bother decoding key.
			if len(prev) > 0 && bytes.HasPrefix(i, prev) {
				continue
			}

			after, err := scan.fetcher.readIndexKey(i)
			if err != nil {
				return 0, roachpb.NewError(err)
			}
			k := i[:len(i)-len(after)]
			if !bytes.Equal(k, prev) {
				prev = k
				rowCount++
			}
		}
	}

	td.b = nil
	return rowCount, nil
}
