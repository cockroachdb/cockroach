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

	rd    sqlbase.RowDeleter
	alloc *sqlbase.DatumAlloc
}

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

func (td *tableDeleter) row(
	ctx context.Context, values tree.Datums, traceKV bool,
) (tree.Datums, error) {
	td.batchSize++
	return nil, td.rd.DeleteRow(ctx, td.b, values, sqlbase.CheckFKs, traceKV)
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

// deleteAllRowsFast employs a ClearRange KV API call to delete the
// underlying data quickly. Unlike DeleteRange, ClearRange doesn't
// leave tombstone data on individual keys, instead using a more
// efficient ranged tombstone, preventing unnecessary write
// amplification.
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
	// If DropTime isn't set, assume this drop request is from a version
	// 1.1 server and invoke legacy code that uses DeleteRange and range GC.
	if td.tableDesc().DropTime == 0 {
		return td.legacyDeleteAllRowsFast(ctx, resume, limit, autoCommit, traceKV)
	}

	log.VEventf(ctx, 2, "ClearRange %s - %s", resume.Key, resume.EndKey)
	// ClearRange cannot be run in a transaction, so create a
	// non-transactional batch to send the request.
	b := &client.Batch{}
	// TODO(tschottdorf): this might need a cluster migration.
	b.AddRawRequest(&roachpb.ClearRangeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    resume.Key,
			EndKey: resume.EndKey,
		},
	})
	if err := td.txn.DB().Run(ctx, b); err != nil {
		return resume, err
	}
	if _, err := td.finalize(ctx, autoCommit, traceKV); err != nil {
		return resume, err
	}
	return roachpb.Span{}, nil
}

// legacyDeleteAllRowsFast handles cases where no GC deadline is set
// and so deletion must fall back to relying on DeleteRange and the
// eventual range GC cycle.
func (td *tableDeleter) legacyDeleteAllRowsFast(
	ctx context.Context, resume roachpb.Span, limit int64, autoCommit autoCommitOpt, traceKV bool,
) (roachpb.Span, error) {
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

	var rf sqlbase.RowFetcher
	tableArgs := sqlbase.RowFetcherTableArgs{
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
		_, err = td.row(ctx, datums, traceKV)
		if err != nil {
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
	if len(idx.Interleave.Ancestors) > 0 || len(idx.InterleavedBy) > 0 {
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
	// TODO(vivekmenezes): adapt index deletion to use the same GC
	// deadline / ClearRange fast path that table deletion uses.
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

	var rf sqlbase.RowFetcher
	tableArgs := sqlbase.RowFetcherTableArgs{
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

func (td *tableDeleter) tableDesc() *sqlbase.TableDescriptor {
	return td.rd.Helper.TableDesc
}

func (td *tableDeleter) fkSpanCollector() sqlbase.FkSpanCollector {
	return td.rd.Fks
}

func (td *tableDeleter) close(_ context.Context) {}
