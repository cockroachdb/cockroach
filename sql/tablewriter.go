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
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/log"
)

// tableWriter handles writing kvs and forming table rows.
//
// Usage:
//   pErr := tw.init(txn)
//   // Handle pErr.
//   for {
//      values := ...
//      row, pErr := tw.row(values)
//      // Handle pErr.
//   }
//   pErr := tw.finalize()
//   // Handle pErr.
type tableWriter interface {

	// init provides the tableWriter with a Txn to write to and returns an error
	// if it was misconfigured.
	init(*client.Txn) error

	// row performs a sql row modification (tableInserter performs an insert,
	// etc). It batches up writes to the init'd txn and periodically sends them.
	// The returned DTuple is suitable for use with returningHelper.
	row(parser.DTuple) (parser.DTuple, error)

	// finalize flushes out any remaining writes. It is called after all calls to
	// row.
	finalize() error
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

func (ti *tableInserter) init(txn *client.Txn) error {
	ti.txn = txn
	ti.b = txn.NewBatch()
	return nil
}

func (ti *tableInserter) row(values parser.DTuple) (parser.DTuple, error) {
	return nil, ti.ri.insertRow(ti.b, values)
}

func (ti *tableInserter) finalize() error {
	var err error
	if ti.autoCommit {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		err = ti.txn.CommitInBatch(ti.b)
	} else {
		err = ti.txn.Run(ti.b)
	}

	if err != nil {
		for _, r := range ti.b.Results {
			if r.PErr != nil {
				return convertBatchError(ti.ri.helper.tableDesc, *ti.b, r.PErr)
			}
		}
		return err
	}
	return nil
}

// tableUpdater handles writing kvs and forming table rows for updates.
type tableUpdater struct {
	ru         rowUpdater
	autoCommit bool

	// Set by init.
	txn *client.Txn
	b   *client.Batch
}

func (tu *tableUpdater) init(txn *client.Txn) error {
	tu.txn = txn
	tu.b = txn.NewBatch()
	return nil
}

func (tu *tableUpdater) row(values parser.DTuple) (parser.DTuple, error) {
	oldValues := values[:len(tu.ru.fetchCols)]
	updateValues := values[len(tu.ru.fetchCols):]
	return tu.ru.updateRow(tu.b, oldValues, updateValues)
}

func (tu *tableUpdater) finalize() error {
	var err error
	if tu.autoCommit {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		err = tu.txn.CommitInBatch(tu.b)
	} else {
		err = tu.txn.Run(tu.b)
	}

	if err != nil {
		for _, r := range tu.b.Results {
			if r.PErr != nil {
				return convertBatchError(tu.ru.helper.tableDesc, *tu.b, r.PErr)
			}
		}
		return err
	}
	return nil
}

// tableUpserter handles writing kvs and forming table rows for upserts.
type tableUpserter struct {
	ri         rowInserter
	ru         rowUpdater
	autoCommit bool

	// Set by init.
	txn                   *client.Txn
	tableDesc             *sqlbase.TableDescriptor
	updateColIDtoRowIndex map[sqlbase.ColumnID]int
	fetcher               rowFetcher

	// Batched up in run/flush.
	upsertRows   []parser.DTuple
	upsertRowPKs []roachpb.Key

	// For allocation avoidance.
	indexKeyPrefix []byte
	updateRow      parser.DTuple
}

func (tu *tableUpserter) init(txn *client.Txn) error {
	tu.txn = txn

	tu.tableDesc = tu.ri.helper.tableDesc
	tu.indexKeyPrefix = sqlbase.MakeIndexKeyPrefix(tu.tableDesc.ID, tu.tableDesc.PrimaryIndex.ID)

	tu.updateColIDtoRowIndex = make(map[sqlbase.ColumnID]int)
	for i, updateCol := range tu.ru.updateCols {
		tu.updateColIDtoRowIndex[updateCol.ID] = i
	}
	tu.updateRow = make(parser.DTuple, len(tu.updateColIDtoRowIndex))

	valNeededForCol := make([]bool, len(tu.ru.fetchCols))
	for i := range valNeededForCol {
		valNeededForCol[i] = true
	}
	err := tu.fetcher.init(
		tu.tableDesc, tu.ru.fetchColIDtoRowIndex, &tu.tableDesc.PrimaryIndex,
		false, false, valNeededForCol)
	if err != nil {
		return err
	}
	return nil
}

func (tu *tableUpserter) row(row parser.DTuple) (parser.DTuple, error) {
	tu.upsertRows = append(tu.upsertRows, row)

	// TODO(dan): The presence of OnConflict currently implies the short form
	// (primary index and update the values being inserted). Implement the long
	// form.
	upsertRowPK, _, err := sqlbase.EncodeIndexKey(
		&tu.tableDesc.PrimaryIndex, tu.ri.insertColIDtoRowIndex, row, tu.indexKeyPrefix)
	if err != nil {
		return nil, err
	}
	tu.upsertRowPKs = append(tu.upsertRowPKs, upsertRowPK)

	// TODO(dan): If len(tu.upsertRows) > some threshold, call flush().
	return nil, nil
}

func (tu *tableUpserter) flush() error {
	pkSpans := make(spans, len(tu.upsertRowPKs))
	for i, upsertRowPK := range tu.upsertRowPKs {
		pkSpans[i] = span{start: upsertRowPK, end: upsertRowPK.PrefixEnd()}
	}

	if err := tu.fetcher.startScan(tu.txn, pkSpans, int64(len(pkSpans))); err != nil {
		return err
	}

	var upsertRowPKIdx int
	existingRows := make([]parser.DTuple, len(tu.upsertRowPKs))
	for {
		row, err := tu.fetcher.nextRow()
		if err != nil {
			return err
		}
		if row == nil {
			break
		}

		// TODO(dan): Can we do this without encoding an index key for every row we
		// get back?
		rowPK, _, err := sqlbase.EncodeIndexKey(
			&tu.tableDesc.PrimaryIndex, tu.ri.insertColIDtoRowIndex, row, tu.indexKeyPrefix)
		if err != nil {
			return err
		}

		for ; upsertRowPKIdx < len(tu.upsertRowPKs); upsertRowPKIdx++ {
			if bytes.Equal(tu.upsertRowPKs[upsertRowPKIdx], rowPK) {
				existingRows[upsertRowPKIdx] = row
				break
			}
		}
	}

	b := tu.txn.NewBatch()
	for i, upsertRow := range tu.upsertRows {
		existingRow := existingRows[i]
		if existingRow == nil {
			err := tu.ri.insertRow(b, upsertRow)
			if err != nil {
				return err
			}
		} else {
			for uCol, uIdx := range tu.updateColIDtoRowIndex {
				tu.updateRow[uIdx] = upsertRow[tu.ri.insertColIDtoRowIndex[uCol]]
			}
			_, err := tu.ru.updateRow(b, existingRow, tu.updateRow)
			if err != nil {
				return err
			}
		}
	}
	tu.upsertRows = nil
	tu.upsertRowPKs = nil

	if err := tu.txn.Run(b); err != nil {
		for _, r := range b.Results {
			if r.PErr != nil {
				return convertBatchError(tu.tableDesc, *b, r.PErr)
			}
		}
		return err
	}
	return nil
}

func (tu *tableUpserter) finalize() error {
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

func (td *tableDeleter) init(txn *client.Txn) error {
	td.txn = txn
	td.b = txn.NewBatch()
	return nil
}

func (td *tableDeleter) row(values parser.DTuple) (parser.DTuple, error) {
	return nil, td.rd.deleteRow(td.b, values)
}

func (td *tableDeleter) finalize() error {
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
func (td *tableDeleter) fastDelete(
	scan *scanNode,
) (rowCount int, err error) {
	for _, span := range scan.spans {
		if log.V(2) {
			log.Infof("Skipping scan and just deleting %s - %s", span.start, span.end)
		}
		td.b.DelRange(span.start, span.end, true)
	}

	err = td.finalize()
	if err != nil {
		return 0, err
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
				return 0, err
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
