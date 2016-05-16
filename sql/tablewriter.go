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
	"fmt"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/log"
)

// tableWriter handles writing kvs and forming table rows.
//
// Usage:
//   err := tw.init(txn)
//   // Handle err.
//   for {
//      values := ...
//      row, err := tw.row(values)
//      // Handle err.
//   }
//   err := tw.finalize()
//   // Handle err.
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
	return nil, ti.ri.insertRow(ti.b, values, false)
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
		return convertBatchError(ti.ri.helper.tableDesc, ti.b)
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
		return convertBatchError(tu.ru.helper.tableDesc, tu.b)
	}
	return nil
}

type tableUpsertEvaler interface {
	// TODO(dan): The tableUpsertEvaler interface separation was an attempt to
	// keep sql logic out of the mapping between table rows and kv operations.
	// Unfortunately, it was a misguided effort. tableUpserter's responsibilities
	// should really be defined as those needed in distributed sql leaf nodes,
	// which will necessarily include expr evaluation.

	// eval returns the values for the update case of an upsert, given the row
	// that would have been inserted and the existing (conflicting) values.
	eval(insertRow parser.DTuple, existingRow parser.DTuple) (parser.DTuple, error)

	isIdentityEvaler() bool
}

// tableUpserter handles writing kvs and forming table rows for upserts.
//
// There are two distinct "modes" that tableUpserter can use, one of which is
// selected during `init`. In the general mode, rows are batched up from calls
// to `row` and upserted using `flush`, which uses 1 or 2 `client.Batch`s from
// the init'd txn to fetch the existing (conflicting) values, followed by one
// more `client.Batch` with the appropriate inserts and updates. In this case,
// all necessary `client.Batch`s are created and run within the lifetime of
// `flush`.
//
// The other mode is the fast path. If certain conditions are met (no secondary
// indexes, all table values being inserted, update expressions of the form `SET
// a = excluded.a`) then the upsert can be done in one `client.Batch` and using
// only `Put`s. In this case, the single batch is created during `init`,
// operated on during `row`, and run during `finalize`. This is the same model
// as the other `tableFoo`s, which are more simple than upsert.
type tableUpserter struct {
	ri            rowInserter
	conflictIndex sqlbase.IndexDescriptor

	// These are set for ON CONFLICT DO UPDATE, but not for DO NOTHING
	updateCols []sqlbase.ColumnDescriptor
	evaler     tableUpsertEvaler

	// Set by init.
	txn                   *client.Txn
	tableDesc             *sqlbase.TableDescriptor
	ru                    rowUpdater
	updateColIDtoRowIndex map[sqlbase.ColumnID]int
	a                     sqlbase.DatumAlloc
	fetchColIDtoRowIndex  map[sqlbase.ColumnID]int
	fetcher               sqlbase.RowFetcher

	// Used for the fast path.
	fastPathBatch *client.Batch
	fastPathKeys  map[string]struct{}

	// Batched up in run/flush.
	insertRows []parser.DTuple

	// For allocation avoidance.
	indexKeyPrefix []byte
}

func (tu *tableUpserter) init(txn *client.Txn) error {
	tu.txn = txn
	tu.tableDesc = tu.ri.helper.tableDesc
	tu.indexKeyPrefix = sqlbase.MakeIndexKeyPrefix(tu.tableDesc.ID, tu.tableDesc.PrimaryIndex.ID)

	allColsIdentityExpr := len(tu.ri.insertCols) == len(tu.tableDesc.Columns) &&
		tu.evaler != nil && tu.evaler.isIdentityEvaler()
	if len(tu.tableDesc.Indexes) == 0 && allColsIdentityExpr {
		tu.fastPathBatch = tu.txn.NewBatch()
		tu.fastPathKeys = make(map[string]struct{})
		return nil
	}

	// TODO(dan): This could be made tighter, just the rows needed for the ON
	// CONFLICT exprs.
	requestedCols := tu.tableDesc.Columns

	var err error
	if len(tu.updateCols) == 0 {
		tu.fetchColIDtoRowIndex = colIDtoRowIndexFromCols(requestedCols)
	} else {
		tu.ru, err = makeRowUpdater(tu.tableDesc, tu.updateCols, requestedCols)
		if err != nil {
			return err
		}
		tu.fetchColIDtoRowIndex = tu.ru.fetchColIDtoRowIndex

		tu.updateColIDtoRowIndex = make(map[sqlbase.ColumnID]int)
		for i, updateCol := range tu.ru.updateCols {
			tu.updateColIDtoRowIndex[updateCol.ID] = i
		}
	}

	valNeededForCol := make([]bool, len(tu.tableDesc.Columns))
	for i := range valNeededForCol {
		if _, ok := tu.fetchColIDtoRowIndex[tu.tableDesc.Columns[i].ID]; ok {
			valNeededForCol[i] = true
		}
	}
	err = tu.fetcher.Init(
		tu.tableDesc, tu.fetchColIDtoRowIndex, &tu.tableDesc.PrimaryIndex, false, false,
		valNeededForCol)
	if err != nil {
		return err
	}

	return nil
}

func (tu *tableUpserter) row(row parser.DTuple) (parser.DTuple, error) {
	if tu.fastPathBatch != nil {
		primaryKey, _, err := sqlbase.EncodeIndexKey(
			&tu.tableDesc.PrimaryIndex, tu.ri.insertColIDtoRowIndex, row, tu.indexKeyPrefix)
		if err != nil {
			return nil, err
		}
		if _, ok := tu.fastPathKeys[string(primaryKey)]; ok {
			return nil, fmt.Errorf("UPSERT/ON CONFLICT DO UPDATE command cannot affect row a second time")
		}
		tu.fastPathKeys[string(primaryKey)] = struct{}{}
		err = tu.ri.insertRow(tu.fastPathBatch, row, true)
		return nil, err
	}

	tu.insertRows = append(tu.insertRows, row)
	// TODO(dan): If len(tu.insertRows) > some threshold, call flush().
	return nil, nil
}

// flush commits to tu.txn any rows batched up in tu.insertRows.
func (tu *tableUpserter) flush() error {
	defer func() {
		tu.insertRows = nil
	}()

	existingRows, err := tu.fetchExisting()
	if err != nil {
		return err
	}

	b := tu.txn.NewBatch()
	for i, insertRow := range tu.insertRows {
		existingRow := existingRows[i]

		if existingRow == nil {
			err := tu.ri.insertRow(b, insertRow, false)
			if err != nil {
				return err
			}
		} else {
			// If len(tu.updateCols) == 0, then we're in the DO NOTHING case.
			if len(tu.updateCols) > 0 {
				existingValues := existingRow[:len(tu.ru.fetchCols)]
				updateValues, err := tu.evaler.eval(insertRow, existingValues)
				if err != nil {
					return err
				}
				_, err = tu.ru.updateRow(b, existingValues, updateValues)
				if err != nil {
					return err
				}
			}
		}
	}

	if err := tu.txn.Run(b); err != nil {
		return convertBatchError(tu.tableDesc, b)
	}
	return nil
}

// upsertRowPKs returns the primary keys of any rows with potential upsert
// conflicts.
func (tu *tableUpserter) upsertRowPKs() ([]roachpb.Key, error) {
	upsertRowPKs := make([]roachpb.Key, len(tu.insertRows))

	if tu.conflictIndex.ID == tu.tableDesc.PrimaryIndex.ID {
		// If the conflict index is the primary index, we can compute them directly.
		// In this case, the slice will be filled, but not all rows will have
		// conflicts.
		for i, insertRow := range tu.insertRows {
			upsertRowPK, _, err := sqlbase.EncodeIndexKey(
				&tu.conflictIndex, tu.ri.insertColIDtoRowIndex, insertRow, tu.indexKeyPrefix)
			if err != nil {
				return nil, err
			}
			upsertRowPKs[i] = upsertRowPK
		}
		return upsertRowPKs, nil
	}

	// Otherwise, compute the keys for the conflict index and look them up. The
	// primary keys can be constructed from the entries that come back. In this
	// case, some spots in the slice will be nil (indicating no conflict) and the
	// others will be conflicting rows.
	b := tu.txn.NewBatch()
	for _, insertRow := range tu.insertRows {
		entry, err := sqlbase.EncodeSecondaryIndex(
			tu.tableDesc.ID, tu.conflictIndex, tu.ri.insertColIDtoRowIndex, insertRow)
		if err != nil {
			return nil, err
		}
		if log.V(2) {
			log.Infof("Get %s\n", entry.Key)
		}
		b.Get(entry.Key)
	}

	if err := tu.txn.Run(b); err != nil {
		return nil, err
	}
	for i, result := range b.Results {
		// if len(result.Rows) == 0, then no conflict for this row, so leave
		// upsertRowPKs[i] as nil.
		if len(result.Rows) == 1 {
			if result.Rows[0].Value == nil {
				upsertRowPKs[i] = nil
			} else {
				upsertRowPK, err := sqlbase.ExtractIndexKey(&tu.a, tu.tableDesc, result.Rows[0])
				if err != nil {
					return nil, err
				}
				upsertRowPKs[i] = upsertRowPK
			}
		} else if len(result.Rows) > 1 {
			panic(fmt.Errorf(
				"Expected <= 1 but got %d conflicts for row %s", len(result.Rows), tu.insertRows[i]))
		}
	}

	return upsertRowPKs, nil
}

// fetchExisting returns any existing rows in the table that conflict with the
// ones in tu.insertRows. The returned slice is the same length as tu.insertRows
// and a nil entry indicates no conflict.
func (tu *tableUpserter) fetchExisting() ([]parser.DTuple, error) {
	primaryKeys, err := tu.upsertRowPKs()
	if err != nil {
		return nil, err
	}

	pkSpans := make(sqlbase.Spans, 0, len(primaryKeys))
	rowIdxForPrimaryKey := make(map[string]int, len(primaryKeys))
	for i, primaryKey := range primaryKeys {
		if primaryKey != nil {
			pkSpans = append(pkSpans, sqlbase.Span{Start: primaryKey, End: primaryKey.PrefixEnd()})
			if _, ok := rowIdxForPrimaryKey[string(primaryKey)]; ok {
				return nil, fmt.Errorf("UPSERT/ON CONFLICT DO UPDATE command cannot affect row a second time")
			}
			rowIdxForPrimaryKey[string(primaryKey)] = i
		}
	}
	if len(pkSpans) == 0 {
		// Every key was empty, so there's nothing to fetch.
		return make([]parser.DTuple, len(primaryKeys)), nil
	}

	if err := tu.fetcher.StartScan(tu.txn, pkSpans, int64(len(pkSpans))); err != nil {
		return nil, err
	}

	rows := make([]parser.DTuple, len(primaryKeys))
	for {
		row, err := tu.fetcher.NextRow()
		if err != nil {
			return nil, err
		}
		if row == nil {
			break // Done
		}

		rowPrimaryKey, _, err := sqlbase.EncodeIndexKey(
			&tu.tableDesc.PrimaryIndex, tu.fetchColIDtoRowIndex, row, tu.indexKeyPrefix)
		if err != nil {
			return nil, err
		}

		// The rows returned by rowFetcher are invalidated after the call to
		// NextRow, so we have to copy them to save them.
		rowCopy := make(parser.DTuple, len(row))
		copy(rowCopy, row)
		rows[rowIdxForPrimaryKey[string(rowPrimaryKey)]] = rowCopy
	}
	return rows, nil
}

func (tu *tableUpserter) finalize() error {
	if tu.fastPathBatch != nil {
		return tu.txn.Run(tu.fastPathBatch)
	}
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
			log.Infof("Skipping scan and just deleting %s - %s", span.Start, span.End)
		}
		td.b.DelRange(span.Start, span.End, true)
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

			after, err := scan.fetcher.ReadIndexKey(i)
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
