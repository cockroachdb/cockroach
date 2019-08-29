// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
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
func (td *tableDeleter) finalize(ctx context.Context, _ bool) (*rowcontainer.RowContainer, error) {
	return nil, td.tableWriterBase.finalize(ctx, td.rd.Helper.TableDesc)
}

// atBatchEnd is part of the extendedTableWriter interface.
func (td *tableDeleter) atBatchEnd(_ context.Context, _ bool) error { return nil }

func (td *tableDeleter) row(ctx context.Context, values tree.Datums, traceKV bool) error {
	td.batchSize++
	return td.rd.DeleteRow(ctx, td.b, values, row.CheckFKs, traceKV)
}

// fastPathDeleteAvailable returns true if the fastDelete optimization can be used.
func fastPathDeleteAvailable(ctx context.Context, desc *ImmutableTableDescriptor) bool {
	indexes := desc.DeletableIndexes()
	if len(indexes) != 0 {
		if log.V(2) {
			log.Infof(ctx, "delete forced to scan: values required to update %d secondary indexes", len(indexes))
		}
		return false
	}
	if desc.IsInterleaved() {
		if log.V(2) {
			log.Info(ctx, "delete forced to scan: table is interleaved")
		}
		return false
	}
	if len(desc.InboundFKs) > 0 {
		if log.V(2) {
			log.Info(ctx, "delete forced to scan: table is referenced by foreign keys")
		}
		return false
	}
	return true
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
	ctx context.Context, resume roachpb.Span, limit int64, traceKV bool,
) (roachpb.Span, error) {
	if td.rd.Helper.TableDesc.IsInterleaved() {
		log.VEvent(ctx, 2, "delete forced to scan: table is interleaved")
		return td.deleteAllRowsScan(ctx, resume, limit, traceKV)
	}
	return td.deleteAllRowsFast(ctx, resume, limit, traceKV)
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
	ctx context.Context, resume roachpb.Span, limit int64, traceKV bool,
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
	if _, err := td.finalize(ctx, traceKV); err != nil {
		return resume, err
	}
	if l := len(td.b.Results); l != 1 {
		panic(fmt.Sprintf("%d results returned", l))
	}
	return td.b.Results[0].ResumeSpanAsValue(), nil
}

func (td *tableDeleter) deleteAllRowsScan(
	ctx context.Context, resume roachpb.Span, limit int64, traceKV bool,
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
	_, err := td.finalize(ctx, traceKV)
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
	ctx context.Context, idx *sqlbase.IndexDescriptor, resume roachpb.Span, limit int64, traceKV bool,
) (roachpb.Span, error) {
	if idx.IsInterleaved() {
		if log.V(2) {
			log.Info(ctx, "delete forced to scan: table is interleaved")
		}
		return td.deleteIndexScan(ctx, idx, resume, limit, traceKV)
	}
	return td.deleteIndexFast(ctx, idx, resume, limit, traceKV)
}

func (td *tableDeleter) deleteIndexFast(
	ctx context.Context, idx *sqlbase.IndexDescriptor, resume roachpb.Span, limit int64, traceKV bool,
) (roachpb.Span, error) {
	if resume.Key == nil {
		resume = td.rd.Helper.TableDesc.IndexSpan(idx.ID)
	}

	if traceKV {
		log.VEventf(ctx, 2, "DelRange %s - %s", resume.Key, resume.EndKey)
	}
	td.b.DelRange(resume.Key, resume.EndKey, false /* returnKeys */)
	td.b.Header.MaxSpanRequestKeys = limit
	if _, err := td.finalize(ctx, traceKV); err != nil {
		return resume, err
	}
	if l := len(td.b.Results); l != 1 {
		panic(fmt.Sprintf("%d results returned, expected 1", l))
	}
	return td.b.Results[0].ResumeSpanAsValue(), nil
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
	ctx context.Context, idx *sqlbase.IndexDescriptor, resume roachpb.Span, limit int64, traceKV bool,
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
	_, err := td.finalize(ctx, traceKV)
	return resume, err
}

func (td *tableDeleter) tableDesc() *sqlbase.ImmutableTableDescriptor {
	return td.rd.Helper.TableDesc
}

func (td *tableDeleter) close(_ context.Context) {}
