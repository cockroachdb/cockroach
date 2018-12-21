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

package sql

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// tableDeleter handles writing kvs and forming table rows for deletes.
type tableDeleter struct {
	tableWriterBase

	rd    row.Deleter
	alloc *sqlbase.DatumAlloc
}

// desc is part of the tableWriter interface.
func (*tableDeleter) desc() string { return "deleter" }

// walkExprs is part of the tableWriter interface.
func (td *tableDeleter) walkExprs(_ func(desc string, index int, expr tree.TypedExpr)) {}

// init is part of the tableWriter interface.
func (td *tableDeleter) init(txn *client.Txn, _ *tree.EvalContext) error {
	td.tableWriterBase.init(txn)
	return nil
}

// flushAndStartNewBatch is part of the extendedTableWriter interface.
func (td *tableDeleter) flushAndStartNewBatch(ctx context.Context) error {
	return td.tableWriterBase.flushAndStartNewBatch(ctx, td.rd.Helper.TableDesc)
}

// finalize is part of the tableWriter interface.
func (td *tableDeleter) finalize(
	ctx context.Context, autoCommit autoCommitOpt, _ bool,
) (*sqlbase.RowContainer, error) {
	return nil, td.tableWriterBase.finalize(ctx, autoCommit, td.rd.Helper.TableDesc)
}

// atBatchEnd is part of the extendedTableWriter interface.
func (td *tableDeleter) atBatchEnd(_ context.Context, _ bool) error { return nil }

func (td *tableDeleter) row(ctx context.Context, values tree.Datums, traceKV bool) error {
	td.batchSize++
	return td.rd.DeleteRow(ctx, td.b, values, row.CheckFKs, traceKV)
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
func (td *tableDeleter) fastDelete(
	ctx context.Context, scan *scanNode, autoCommit autoCommitOpt, traceKV bool,
) (rowCount int, err error) {
	for _, span := range scan.spans {
		log.VEvent(ctx, 2, "fast delete: skipping scan")
		if traceKV {
			log.VEventf(ctx, 2, "DelRange %s - %s", span.Key, span.EndKey)
		}
		td.b.DelRange(span.Key, span.EndKey, true /* returnKeys */)
	}

	_, err = td.finalize(ctx, autoCommit, traceKV)
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

			after, ok, err := scan.run.fetcher.ReadIndexKey(i)
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
	ctx context.Context, resume roachpb.Span, limit int64, autoCommit autoCommitOpt, traceKV bool,
) (roachpb.Span, error) {
	if td.rd.Helper.TableDesc.IsInterleaved() {
		log.VEvent(ctx, 2, "delete forced to scan: table is interleaved")
		return td.deleteAllRowsScan(ctx, resume, limit, autoCommit, traceKV)
	}
	return td.deleteAllRowsFast(ctx, resume, limit, autoCommit, traceKV)
}

// deleteAllRowsFast uses the DelRange KV request to delete data quickly,
// relative to deleteAllRowsScan.
//
// Note that this method leaves a RocksDB deletion tombstone on every key in the
// table, resulting in substantial write amplification. When possible, the
// schema changer avoids using a tableDeleter entirely in favor of the
// ClearRange KV request, which uses RocksDB range deletion tombstones to avoid
// write amplification.
func (td *tableDeleter) deleteAllRowsFast(
	ctx context.Context, resume roachpb.Span, limit int64, autoCommit autoCommitOpt, traceKV bool,
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

	log.VEventf(ctx, 2, "DelRange %s - %s", resume.Key, resume.EndKey)
	td.b.DelRange(resume.Key, resume.EndKey, false /* returnKeys */)
	td.b.Header.MaxSpanRequestKeys = limit
	if _, err := td.finalize(ctx, autoCommit, traceKV); err != nil {
		return resume, err
	}
	if l := len(td.b.Results); l != 1 {
		panic(fmt.Sprintf("%d results returned", l))
	}
	return td.b.Results[0].ResumeSpan, nil
}

func (td *tableDeleter) deleteAllRowsScan(
	ctx context.Context, resume roachpb.Span, limit int64, autoCommit autoCommitOpt, traceKV bool,
) (roachpb.Span, error) {
	if resume.Key == nil {
		resume = td.rd.Helper.TableDesc.PrimaryIndexSpan()
	}

	var valNeededForCol util.FastIntSet
	for _, idx := range td.rd.FetchColIDtoRowIndex {
		valNeededForCol.Add(idx)
	}

	var rf row.Fetcher
	tableArgs := row.FetcherTableArgs{
		Desc:            td.rd.Helper.TableDesc,
		Index:           &td.rd.Helper.TableDesc.PrimaryIndex,
		ColIdxMap:       td.rd.FetchColIDtoRowIndex,
		Cols:            td.rd.FetchCols,
		ValNeededForCol: valNeededForCol,
	}
	if err := rf.Init(
		false /* reverse */, false /* returnRangeInfo */, false /* isCheck */, td.alloc, tableArgs,
	); err != nil {
		return resume, err
	}
	if err := rf.StartScan(ctx, td.txn, roachpb.Spans{resume}, true /* limit batches */, 0, traceKV); err != nil {
		return resume, err
	}

	for i := int64(0); i < limit; i++ {
		datums, _, _, err := rf.NextRowDecoded(ctx)
		if err != nil {
			return resume, err
		}
		if datums == nil {
			// Done deleting all rows.
			resume = roachpb.Span{}
			break
		}
		if err = td.row(ctx, datums, traceKV); err != nil {
			return resume, err
		}
	}
	if resume.Key != nil {
		// Update the resume start key for the next iteration.
		resume.Key = rf.Key()
	}
	_, err := td.finalize(ctx, autoCommit, traceKV)
	return resume, err
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
	ctx context.Context,
	idx *sqlbase.IndexDescriptor,
	resume roachpb.Span,
	limit int64,
	autoCommit autoCommitOpt,
	traceKV bool,
) (roachpb.Span, error) {
	if idx.IsInterleaved() {
		if log.V(2) {
			log.Info(ctx, "delete forced to scan: table is interleaved")
		}
		return td.deleteIndexScan(ctx, idx, resume, limit, autoCommit, traceKV)
	}
	return td.deleteIndexFast(ctx, idx, resume, limit, autoCommit, traceKV)
}

func (td *tableDeleter) deleteIndexFast(
	ctx context.Context,
	idx *sqlbase.IndexDescriptor,
	resume roachpb.Span,
	limit int64,
	autoCommit autoCommitOpt,
	traceKV bool,
) (roachpb.Span, error) {
	if resume.Key == nil {
		resume = td.rd.Helper.TableDesc.IndexSpan(idx.ID)
	}

	if traceKV {
		log.VEventf(ctx, 2, "DelRange %s - %s", resume.Key, resume.EndKey)
	}
	td.b.DelRange(resume.Key, resume.EndKey, false /* returnKeys */)
	td.b.Header.MaxSpanRequestKeys = limit
	if _, err := td.finalize(ctx, autoCommit, traceKV); err != nil {
		return resume, err
	}
	if l := len(td.b.Results); l != 1 {
		panic(fmt.Sprintf("%d results returned, expected 1", l))
	}
	return td.b.Results[0].ResumeSpan, nil
}

func (td *tableDeleter) clearIndex(ctx context.Context, idx *sqlbase.IndexDescriptor) error {
	if idx.IsInterleaved() {
		return errors.Errorf("unexpected interleaved index %d", idx.ID)
	}

	sp := td.rd.Helper.TableDesc.IndexSpan(idx.ID)

	// ClearRange cannot be run in a transaction, so create a
	// non-transactional batch to send the request.
	b := &client.Batch{}
	b.AddRawRequest(&roachpb.ClearRangeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    sp.Key,
			EndKey: sp.EndKey,
		},
	})
	return td.txn.DB().Run(ctx, b)
}

func (td *tableDeleter) deleteIndexScan(
	ctx context.Context,
	idx *sqlbase.IndexDescriptor,
	resume roachpb.Span,
	limit int64,
	autoCommit autoCommitOpt,
	traceKV bool,
) (roachpb.Span, error) {
	if resume.Key == nil {
		resume = td.rd.Helper.TableDesc.PrimaryIndexSpan()
	}

	var valNeededForCol util.FastIntSet
	for _, idx := range td.rd.FetchColIDtoRowIndex {
		valNeededForCol.Add(idx)
	}

	var rf row.Fetcher
	tableArgs := row.FetcherTableArgs{
		Desc:            td.rd.Helper.TableDesc,
		Index:           &td.rd.Helper.TableDesc.PrimaryIndex,
		ColIdxMap:       td.rd.FetchColIDtoRowIndex,
		Cols:            td.rd.FetchCols,
		ValNeededForCol: valNeededForCol,
	}
	if err := rf.Init(
		false /* reverse */, false /* returnRangeInfo */, false /* isCheck */, td.alloc, tableArgs,
	); err != nil {
		return resume, err
	}
	if err := rf.StartScan(ctx, td.txn, roachpb.Spans{resume}, true /* limit batches */, 0, traceKV); err != nil {
		return resume, err
	}

	for i := int64(0); i < limit; i++ {
		datums, _, _, err := rf.NextRowDecoded(ctx)
		if err != nil {
			return resume, err
		}
		if datums == nil {
			// Done deleting all rows.
			resume = roachpb.Span{}
			break
		}
		if err := td.rd.DeleteIndexRow(ctx, td.b, idx, datums, traceKV); err != nil {
			return resume, err
		}
	}
	if resume.Key != nil {
		// Update the resume start key for the next iteration.
		resume.Key = rf.Key()
	}
	_, err := td.finalize(ctx, autoCommit, traceKV)
	return resume, err
}

func (td *tableDeleter) tableDesc() *sqlbase.ImmutableTableDescriptor {
	return td.rd.Helper.TableDesc
}

func (td *tableDeleter) fkSpanCollector() row.FkSpanCollector {
	return td.rd.Fks
}

func (td *tableDeleter) close(_ context.Context) {}
