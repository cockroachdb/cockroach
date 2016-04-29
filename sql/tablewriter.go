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
//      row, pErr := tw.run(values)
//      // Handle pErr.
//   }
//   pErr := tw.finalize()
//   // Handle pErr.
type tableWriter interface {

	// init provides the tableWriter with a Txn to write to and returns an error
	// if it was misconfigured.
	init(*client.Txn) *roachpb.Error

	// run performs a sql row modification (tableInserter performs an insert,
	// etc). It batch up writes to the init'd txn and periodically send them. The
	// returned DTuple is suitable for use with returningHelper.
	run(parser.DTuple) (parser.DTuple, *roachpb.Error)

	// finalize flushes out any remaining writes. It is called after all calls to
	// run.
	finalize() *roachpb.Error
}

var _ tableWriter = (*tableInserter)(nil)
var _ tableWriter = (*tableUpdater)(nil)
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
		pErr = tu.txn.Run(tu.b)
	}

	if pErr != nil {
		pErr = convertBatchError(tu.ru.helper.tableDesc, *tu.b, pErr)
	}
	return pErr
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
