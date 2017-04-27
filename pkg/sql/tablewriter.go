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

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// expressionCarrier handles visiting sub-expressions.
type expressionCarrier interface {
	// walkExprs explores all sub-expressions held by this object, if
	// any.
	walkExprs(func(desc string, index int, expr parser.TypedExpr))
}

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
	expressionCarrier

	// init provides the tableWriter with a Txn to write to and returns an error
	// if it was misconfigured.
	init(*client.Txn) error

	// row performs a sql row modification (tableInserter performs an insert,
	// etc). It batches up writes to the init'd txn and periodically sends them.
	// The passed Datums is not used after `row` returns. The returned Datums is
	// suitable for use with returningHelper.
	row(context.Context, parser.Datums) (parser.Datums, error)

	// finalize flushes out any remaining writes. It is called after all calls to
	// row.
	finalize(ctx context.Context) error

	// spans collects the upper bound set of read and write spans that the
	// tableWriter will touch when executed. This is contractual, and the
	// tableWriter will not touch any keys outside of the spans reported here.
	//
	// TODO(nvanbenschoten): we are currently pretty pessimistic here, assuming
	// that table operations will touch the entire table. In cases where we
	// can determine ahead of time that this isn't true, we should true to
	// constrain these spans.
	spans() (reads, writes roachpb.Spans, err error)
}

var _ tableWriter = (*tableInserter)(nil)
var _ tableWriter = (*tableUpdater)(nil)
var _ tableWriter = (*tableUpserter)(nil)
var _ tableWriter = (*tableDeleter)(nil)

// tableInserter handles writing kvs and forming table rows for inserts.
type tableInserter struct {
	ri         sqlbase.RowInserter
	autoCommit bool

	// Set by init.
	txn *client.Txn
	b   *client.Batch
}

func (ti *tableInserter) walkExprs(_ func(desc string, index int, expr parser.TypedExpr)) {}

func (ti *tableInserter) init(txn *client.Txn) error {
	ti.txn = txn
	ti.b = txn.NewBatch()
	return nil
}

func (ti *tableInserter) row(ctx context.Context, values parser.Datums) (parser.Datums, error) {
	return nil, ti.ri.InsertRow(ctx, ti.b, values, false)
}

func (ti *tableInserter) finalize(ctx context.Context) error {
	var err error
	if ti.autoCommit {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		err = ti.txn.CommitInBatch(ctx, ti.b)
	} else {
		err = ti.txn.Run(ctx, ti.b)
	}

	if err != nil {
		return sqlbase.ConvertBatchError(ti.ri.Helper.TableDesc, ti.b)
	}
	return nil
}

func (ti *tableInserter) spans() (reads, writes roachpb.Spans, err error) {
	return collectTableWriterSpans(ti.ri.Helper.TableDesc, ti.ri.Fks)
}

// tableUpdater handles writing kvs and forming table rows for updates.
type tableUpdater struct {
	ru         sqlbase.RowUpdater
	autoCommit bool

	// Set by init.
	txn *client.Txn
	b   *client.Batch
}

func (tu *tableUpdater) walkExprs(_ func(desc string, index int, expr parser.TypedExpr)) {}

func (tu *tableUpdater) init(txn *client.Txn) error {
	tu.txn = txn
	tu.b = txn.NewBatch()
	return nil
}

func (tu *tableUpdater) row(ctx context.Context, values parser.Datums) (parser.Datums, error) {
	oldValues := values[:len(tu.ru.FetchCols)]
	updateValues := values[len(tu.ru.FetchCols):]
	return tu.ru.UpdateRow(ctx, tu.b, oldValues, updateValues)
}

func (tu *tableUpdater) finalize(ctx context.Context) error {
	var err error
	if tu.autoCommit {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		err = tu.txn.CommitInBatch(ctx, tu.b)
	} else {
		err = tu.txn.Run(ctx, tu.b)
	}

	if err != nil {
		return sqlbase.ConvertBatchError(tu.ru.Helper.TableDesc, tu.b)
	}
	return nil
}

func (tu *tableUpdater) spans() (reads, writes roachpb.Spans, err error) {
	return collectTableWriterSpans(tu.ru.Helper.TableDesc, tu.ru.Fks)
}

type tableUpsertEvaler interface {
	expressionCarrier

	// TODO(dan): The tableUpsertEvaler interface separation was an attempt to
	// keep sql logic out of the mapping between table rows and kv operations.
	// Unfortunately, it was a misguided effort. tableUpserter's responsibilities
	// should really be defined as those needed in distributed sql leaf nodes,
	// which will necessarily include expr evaluation.

	// eval returns the values for the update case of an upsert, given the row
	// that would have been inserted and the existing (conflicting) values.
	eval(insertRow parser.Datums, existingRow parser.Datums) (parser.Datums, error)
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
	ri            sqlbase.RowInserter
	autoCommit    bool
	conflictIndex sqlbase.IndexDescriptor
	isUpsertAlias bool

	// These are set for ON CONFLICT DO UPDATE, but not for DO NOTHING
	updateCols []sqlbase.ColumnDescriptor
	evaler     tableUpsertEvaler

	// Set by init.
	txn                   *client.Txn
	tableDesc             *sqlbase.TableDescriptor
	fkTables              sqlbase.TableLookupsByID // for fk checks in update case
	ru                    sqlbase.RowUpdater
	updateColIDtoRowIndex map[sqlbase.ColumnID]int
	a                     sqlbase.DatumAlloc
	fetchCols             []sqlbase.ColumnDescriptor
	fetchColIDtoRowIndex  map[sqlbase.ColumnID]int
	fetcher               sqlbase.RowFetcher

	// Used for the fast path.
	fastPathBatch *client.Batch
	fastPathKeys  map[string]struct{}

	// Batched up in run/flush.
	insertRows []parser.Datums

	// For allocation avoidance.
	indexKeyPrefix []byte
}

func (tu *tableUpserter) walkExprs(walk func(desc string, index int, expr parser.TypedExpr)) {
	if tu.evaler != nil {
		tu.evaler.walkExprs(walk)
	}
}

func (tu *tableUpserter) init(txn *client.Txn) error {
	tu.txn = txn
	tu.tableDesc = tu.ri.Helper.TableDesc
	tu.indexKeyPrefix = sqlbase.MakeIndexKeyPrefix(tu.tableDesc, tu.tableDesc.PrimaryIndex.ID)

	// TODO(dan): The fast path is currently only enabled when the UPSERT alias
	// is explicitly selected by the user. It's possible to fast path some
	// queries of the form INSERT ... ON CONFLICT, but the utility is low and
	// there are lots of edge cases (that caused real correctness bugs #13437
	// #13962). As a result, we've decided to remove this until after 1.0 and
	// re-enable it then. See #14482.
	enableFastPath := tu.isUpsertAlias &&
		// Tables with secondary indexes are not eligible for fast path (it
		// would be easy to add the new secondary index entry but we can't clean
		// up the old one without the previous values).
		len(tu.tableDesc.Indexes) == 0 &&
		// When adding or removing a column in a schema change (mutation), the user
		// can't specify it, which means we need to do a lookup and so we can't use
		// the fast path. When adding or removing an index, same result, so the fast
		// path is disabled during all mutations.
		len(tu.tableDesc.Mutations) == 0 &&
		// For the fast path, all columns must be specified in the insert.
		len(tu.ri.InsertCols) == len(tu.tableDesc.Columns)
	if enableFastPath {
		tu.fastPathBatch = tu.txn.NewBatch()
		tu.fastPathKeys = make(map[string]struct{})
		return nil
	}

	// TODO(dan): This could be made tighter, just the rows needed for the ON
	// CONFLICT exprs.
	requestedCols := tu.tableDesc.Columns

	if len(tu.updateCols) == 0 {
		tu.fetchCols = requestedCols
		tu.fetchColIDtoRowIndex = sqlbase.ColIDtoRowIndexFromCols(requestedCols)
	} else {
		var err error
		tu.ru, err = sqlbase.MakeRowUpdater(
			txn, tu.tableDesc, tu.fkTables, tu.updateCols, requestedCols, sqlbase.RowUpdaterDefault,
		)
		if err != nil {
			return err
		}
		// t.ru.fetchCols can also contain columns undergoing mutation.
		tu.fetchCols = tu.ru.FetchCols
		tu.fetchColIDtoRowIndex = tu.ru.FetchColIDtoRowIndex

		tu.updateColIDtoRowIndex = make(map[sqlbase.ColumnID]int)
		for i, updateCol := range tu.ru.UpdateCols {
			tu.updateColIDtoRowIndex[updateCol.ID] = i
		}
	}

	valNeededForCol := make([]bool, len(tu.fetchCols))
	for i, col := range tu.fetchCols {
		if _, ok := tu.fetchColIDtoRowIndex[col.ID]; ok {
			valNeededForCol[i] = true
		}
	}

	return tu.fetcher.Init(
		tu.tableDesc, tu.fetchColIDtoRowIndex, &tu.tableDesc.PrimaryIndex,
		false /* reverse */, false, /* isSecondaryIndex */
		tu.fetchCols, valNeededForCol, false /*returnRangeInfo*/)
}

func (tu *tableUpserter) row(ctx context.Context, row parser.Datums) (parser.Datums, error) {
	if tu.fastPathBatch != nil {
		primaryKey, _, err := sqlbase.EncodeIndexKey(
			tu.tableDesc, &tu.tableDesc.PrimaryIndex, tu.ri.InsertColIDtoRowIndex, row, tu.indexKeyPrefix)
		if err != nil {
			return nil, err
		}
		if _, ok := tu.fastPathKeys[string(primaryKey)]; ok {
			return nil, fmt.Errorf("UPSERT/ON CONFLICT DO UPDATE command cannot affect row a second time")
		}
		tu.fastPathKeys[string(primaryKey)] = struct{}{}
		err = tu.ri.InsertRow(ctx, tu.fastPathBatch, row, true)
		return nil, err
	}

	// Copy the row because the `tableWriter` interface guarantees that it's not
	// used after this returns.
	tu.insertRows = append(tu.insertRows, append([]parser.Datum(nil), row...))
	// TODO(dan): If len(tu.insertRows) > some threshold, call flush().
	return nil, nil
}

// flush commits to tu.txn any rows batched up in tu.insertRows.
func (tu *tableUpserter) flush(ctx context.Context, finalize bool) error {
	defer func() {
		tu.insertRows = nil
	}()

	existingRows, err := tu.fetchExisting(ctx)
	if err != nil {
		return err
	}

	b := tu.txn.NewBatch()
	for i, insertRow := range tu.insertRows {
		existingRow := existingRows[i]

		if existingRow == nil {
			err := tu.ri.InsertRow(ctx, b, insertRow, false)
			if err != nil {
				return err
			}
		} else {
			// If len(tu.updateCols) == 0, then we're in the DO NOTHING case.
			if len(tu.updateCols) > 0 {
				existingValues := existingRow[:len(tu.ru.FetchCols)]
				updateValues, err := tu.evaler.eval(insertRow, existingValues)
				if err != nil {
					return err
				}
				_, err = tu.ru.UpdateRow(ctx, b, existingValues, updateValues)
				if err != nil {
					return err
				}
			}
		}
	}

	if finalize && tu.autoCommit {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		err = tu.txn.CommitInBatch(ctx, b)
	} else {
		err = tu.txn.Run(ctx, b)
	}
	if err != nil {
		return sqlbase.ConvertBatchError(tu.tableDesc, b)
	}
	return nil
}

// upsertRowPKs returns the primary keys of any rows with potential upsert
// conflicts.
func (tu *tableUpserter) upsertRowPKs(ctx context.Context) ([]roachpb.Key, error) {
	upsertRowPKs := make([]roachpb.Key, len(tu.insertRows))

	if tu.conflictIndex.ID == tu.tableDesc.PrimaryIndex.ID {
		// If the conflict index is the primary index, we can compute them directly.
		// In this case, the slice will be filled, but not all rows will have
		// conflicts.
		for i, insertRow := range tu.insertRows {
			upsertRowPK, _, err := sqlbase.EncodeIndexKey(
				tu.tableDesc, &tu.conflictIndex, tu.ri.InsertColIDtoRowIndex, insertRow, tu.indexKeyPrefix)
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
			tu.tableDesc, &tu.conflictIndex, tu.ri.InsertColIDtoRowIndex, insertRow)
		if err != nil {
			return nil, err
		}
		if log.V(2) {
			log.Infof(ctx, "Get %s\n", entry.Key)
		}
		b.Get(entry.Key)
	}

	if err := tu.txn.Run(ctx, b); err != nil {
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
func (tu *tableUpserter) fetchExisting(ctx context.Context) ([]parser.Datums, error) {
	primaryKeys, err := tu.upsertRowPKs(ctx)
	if err != nil {
		return nil, err
	}

	pkSpans := make(roachpb.Spans, 0, len(primaryKeys))
	rowIdxForPrimaryKey := make(map[string]int, len(primaryKeys))
	for i, primaryKey := range primaryKeys {
		if primaryKey != nil {
			pkSpans = append(pkSpans, roachpb.Span{Key: primaryKey, EndKey: primaryKey.PrefixEnd()})
			if _, ok := rowIdxForPrimaryKey[string(primaryKey)]; ok {
				return nil, fmt.Errorf("UPSERT/ON CONFLICT DO UPDATE command cannot affect row a second time")
			}
			rowIdxForPrimaryKey[string(primaryKey)] = i
		}
	}
	if len(pkSpans) == 0 {
		// Every key was empty, so there's nothing to fetch.
		return make([]parser.Datums, len(primaryKeys)), nil
	}

	// We don't limit batches here because the spans are unordered.
	if err := tu.fetcher.StartScan(ctx, tu.txn, pkSpans, false /* no batch limits */, 0); err != nil {
		return nil, err
	}

	rows := make([]parser.Datums, len(primaryKeys))
	for {
		row, err := tu.fetcher.NextRowDecoded(ctx)
		if err != nil {
			return nil, err
		}
		if row == nil {
			break // Done
		}

		rowPrimaryKey, _, err := sqlbase.EncodeIndexKey(
			tu.tableDesc, &tu.tableDesc.PrimaryIndex, tu.fetchColIDtoRowIndex, row, tu.indexKeyPrefix)
		if err != nil {
			return nil, err
		}

		// The rows returned by rowFetcher are invalidated after the call to
		// NextRow, so we have to copy them to save them.
		rowCopy := make(parser.Datums, len(row))
		copy(rowCopy, row)
		rows[rowIdxForPrimaryKey[string(rowPrimaryKey)]] = rowCopy
	}
	return rows, nil
}

func (tu *tableUpserter) finalize(ctx context.Context) error {
	if tu.fastPathBatch != nil {
		if tu.autoCommit {
			// An auto-txn can commit the transaction with the batch. This is an
			// optimization to avoid an extra round-trip to the transaction
			// coordinator.
			return tu.txn.CommitInBatch(ctx, tu.fastPathBatch)
		}
		return tu.txn.Run(ctx, tu.fastPathBatch)
	}
	return tu.flush(ctx, true /* finalize */)
}

func (tu *tableUpserter) spans() (reads, writes roachpb.Spans, err error) {
	return collectTableWriterSpans(tu.ri.Helper.TableDesc, tu.ri.Fks)
}

// tableDeleter handles writing kvs and forming table rows for deletes.
type tableDeleter struct {
	rd         sqlbase.RowDeleter
	autoCommit bool

	// Set by init.
	txn *client.Txn
	b   *client.Batch
}

func (td *tableDeleter) walkExprs(_ func(desc string, index int, expr parser.TypedExpr)) {}

func (td *tableDeleter) init(txn *client.Txn) error {
	td.txn = txn
	td.b = txn.NewBatch()
	return nil
}

func (td *tableDeleter) row(ctx context.Context, values parser.Datums) (parser.Datums, error) {
	return nil, td.rd.DeleteRow(ctx, td.b, values)
}

func (td *tableDeleter) finalize(ctx context.Context) error {
	if td.autoCommit {
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		return td.txn.CommitInBatch(ctx, td.b)
	}
	return td.txn.Run(ctx, td.b)
}

// fastPathAvailable returns true if the fastDelete optimization can be used.
func (td *tableDeleter) fastPathAvailable(ctx context.Context) bool {
	if len(td.rd.Helper.Indexes) != 0 {
		if log.V(2) {
			log.Infof(ctx, "delete forced to scan: values required to update %d secondary indexes", len(td.rd.Helper.Indexes))
		}
		return false
	}
	if td.rd.Helper.TableDesc.IsInterleaved() {
		if log.V(2) {
			log.Info(ctx, "delete forced to scan: table is interleaved")
		}
		return false
	}
	if len(td.rd.Helper.TableDesc.PrimaryIndex.ReferencedBy) > 0 {
		if log.V(2) {
			log.Info(ctx, "delete forced to scan: table is referenced by foreign keys")
		}
		return false
	}
	return true
}

// fastDelete adds to the batch the kv operations necessary to delete sql rows
// without knowing the values that are currently present. fastDelete calls
// finalize, so it should not be called after.
func (td *tableDeleter) fastDelete(ctx context.Context, scan *scanNode) (rowCount int, err error) {
	for _, span := range scan.spans {
		if log.V(2) {
			log.Infof(ctx, "Skipping scan and just deleting %s - %s", span.Key, span.EndKey)
		}
		td.b.DelRange(span.Key, span.EndKey, true)
	}

	err = td.finalize(ctx)
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

			after, ok, err := scan.fetcher.ReadIndexKey(i)
			if err != nil {
				return 0, err
			}
			if !ok {
				return 0, errors.Errorf("key did not match descriptor")
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

// deleteAllRows runs the kv operations necessary to delete all sql rows in the
// table passed at construction. This may require a scan.
//
// resume is the resume-span which should be used for the table deletion when
// the table deletion is chunked. The first call to this method should use a
// zero resume-span. After a chunk is deleted a new resume-span is returned.
//
// limit is a limit on either the number of keys or table-rows (for
// interleaved tables) deleted in the operation.
func (td *tableDeleter) deleteAllRows(
	ctx context.Context, resume roachpb.Span, limit int64,
) (roachpb.Span, error) {
	if td.rd.Helper.TableDesc.IsInterleaved() {
		if log.V(2) {
			log.Info(ctx, "delete forced to scan: table is interleaved")
		}
		return td.deleteAllRowsScan(ctx, resume, limit)
	}
	return td.deleteAllRowsFast(ctx, resume, limit)
}

func (td *tableDeleter) deleteAllRowsFast(
	ctx context.Context, resume roachpb.Span, limit int64,
) (roachpb.Span, error) {
	if resume.Key == nil {
		tablePrefix := roachpb.Key(
			encoding.EncodeUvarintAscending(nil, uint64(td.rd.Helper.TableDesc.ID)),
		)
		// Delete rows and indexes starting with the table's prefix.
		resume = roachpb.Span{
			Key:    tablePrefix,
			EndKey: tablePrefix.PrefixEnd(),
		}
	}
	if log.V(2) {
		log.Infof(ctx, "DelRange %s - %s", resume.Key, resume.EndKey)
	}
	td.b.DelRange(resume.Key, resume.EndKey, false /* returnKeys */)
	td.b.Header.MaxSpanRequestKeys = limit
	if err := td.finalize(ctx); err != nil {
		return resume, err
	}
	if l := len(td.b.Results); l != 1 {
		panic(fmt.Sprintf("%d results returned", l))
	}
	return td.b.Results[0].ResumeSpan, nil
}

func (td *tableDeleter) deleteAllRowsScan(
	ctx context.Context, resume roachpb.Span, limit int64,
) (roachpb.Span, error) {
	if resume.Key == nil {
		resume = td.rd.Helper.TableDesc.PrimaryIndexSpan()
	}
	valNeededForCol := make([]bool, len(td.rd.Helper.TableDesc.Columns))
	for _, idx := range td.rd.FetchColIDtoRowIndex {
		valNeededForCol[idx] = true
	}

	var rf sqlbase.RowFetcher
	err := rf.Init(
		td.rd.Helper.TableDesc, td.rd.FetchColIDtoRowIndex, &td.rd.Helper.TableDesc.PrimaryIndex,
		false /*reverse*/, false, /*isSecondaryIndex*/
		td.rd.FetchCols, valNeededForCol, false /* returnRangeInfo */)
	if err != nil {
		return resume, err
	}
	if err := rf.StartScan(ctx, td.txn, roachpb.Spans{resume}, true /* limit batches */, 0); err != nil {
		return resume, err
	}

	for i := int64(0); i < limit; i++ {
		row, err := rf.NextRowDecoded(ctx)
		if err != nil {
			return resume, err
		}
		if row == nil {
			// Done deleting all rows.
			resume = roachpb.Span{}
			break
		}
		_, err = td.row(ctx, row)
		if err != nil {
			return resume, err
		}
	}
	if resume.Key != nil {
		// Update the resume start key for the next iteration.
		resume.Key = rf.Key()
	}
	return resume, td.finalize(ctx)
}

// deleteIndex runs the kv operations necessary to delete all kv entries in the
// given index. This may require a scan.
//
// resume is the resume-span which should be used for the index deletion
// when the index deletion is chunked. The first call to this method should
// use a zero resume-span. After a chunk of the index is deleted a new resume-
// span is returned.
//
// limit is a limit on the number of index entries deleted in the operation.
func (td *tableDeleter) deleteIndex(
	ctx context.Context, idx *sqlbase.IndexDescriptor, resume roachpb.Span, limit int64,
) (roachpb.Span, error) {
	if len(idx.Interleave.Ancestors) > 0 || len(idx.InterleavedBy) > 0 {
		if log.V(2) {
			log.Info(ctx, "delete forced to scan: table is interleaved")
		}
		return td.deleteIndexScan(ctx, idx, resume, limit)
	}
	return td.deleteIndexFast(ctx, idx, resume, limit)
}

func (td *tableDeleter) deleteIndexFast(
	ctx context.Context, idx *sqlbase.IndexDescriptor, resume roachpb.Span, limit int64,
) (roachpb.Span, error) {
	if resume.Key == nil {
		resume = td.rd.Helper.TableDesc.IndexSpan(idx.ID)
	}

	if log.V(2) {
		log.Infof(ctx, "DelRange %s - %s", resume.Key, resume.EndKey)
	}
	td.b.DelRange(resume.Key, resume.EndKey, false /* returnKeys */)
	td.b.Header.MaxSpanRequestKeys = limit
	if err := td.finalize(ctx); err != nil {
		return resume, err
	}
	if l := len(td.b.Results); l != 1 {
		panic(fmt.Sprintf("%d results returned, expected 1", l))
	}
	return td.b.Results[0].ResumeSpan, nil
}

func (td *tableDeleter) deleteIndexScan(
	ctx context.Context, idx *sqlbase.IndexDescriptor, resume roachpb.Span, limit int64,
) (roachpb.Span, error) {
	if resume.Key == nil {
		resume = td.rd.Helper.TableDesc.PrimaryIndexSpan()
	}
	valNeededForCol := make([]bool, len(td.rd.Helper.TableDesc.Columns))
	for _, idx := range td.rd.FetchColIDtoRowIndex {
		valNeededForCol[idx] = true
	}

	var rf sqlbase.RowFetcher
	err := rf.Init(
		td.rd.Helper.TableDesc, td.rd.FetchColIDtoRowIndex, &td.rd.Helper.TableDesc.PrimaryIndex,
		false /* reverse */, false, /*isSecondaryIndex */
		td.rd.FetchCols, valNeededForCol, false /* returnRangeInfo */)
	if err != nil {
		return resume, err
	}
	if err := rf.StartScan(ctx, td.txn, roachpb.Spans{resume}, true /* limit batches */, 0); err != nil {
		return resume, err
	}

	for i := int64(0); i < limit; i++ {
		row, err := rf.NextRowDecoded(ctx)
		if err != nil {
			return resume, err
		}
		if row == nil {
			// Done deleting all rows.
			resume = roachpb.Span{}
			break
		}
		if err := td.rd.DeleteIndexRow(ctx, td.b, idx, row); err != nil {
			return resume, err
		}
	}
	if resume.Key != nil {
		// Update the resume start key for the next iteration.
		resume.Key = rf.Key()
	}
	return resume, td.finalize(ctx)
}

func (td *tableDeleter) spans() (reads, writes roachpb.Spans, err error) {
	return collectTableWriterSpans(td.rd.Helper.TableDesc, td.rd.Fks)
}

func collectTableWriterSpans(
	desc *sqlbase.TableDescriptor, fks sqlbase.FkSpanCollector,
) (reads, writes roachpb.Spans, err error) {
	// We don't generally know which spans we will be modifying so we must be
	// conservative and assume anything in the table might change. See TODO on
	// tableWriter.spans for discussion on constraining spans wherever possible.
	tableSpans := desc.AllIndexSpans()
	fkReads, fkWrites := fks.CollectSpans()
	if len(fkWrites) > 0 {
		return nil, nil, errors.Errorf("unexpected foreign key span writes: %v", fkWrites)
	}
	return fkReads, tableSpans, nil
}
