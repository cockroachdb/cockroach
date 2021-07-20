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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// tableDeleter handles writing kvs and forming table rows for deletes.
type tableDeleter struct {
	tableWriterBase

	rd    row.Deleter
	alloc *rowenc.DatumAlloc
}

var _ tableWriter = &tableDeleter{}

// desc is part of the tableWriter interface.
func (*tableDeleter) desc() string { return "deleter" }

// walkExprs is part of the tableWriter interface.
func (td *tableDeleter) walkExprs(_ func(desc string, index int, expr tree.TypedExpr)) {}

// init is part of the tableWriter interface.
func (td *tableDeleter) init(_ context.Context, txn *kv.Txn, evalCtx *tree.EvalContext) error {
	td.tableWriterBase.init(txn, td.tableDesc(), evalCtx)
	return nil
}

// row is part of the tableWriter interface.
func (td *tableDeleter) row(
	ctx context.Context, values tree.Datums, pm row.PartialIndexUpdateHelper, traceKV bool,
) error {
	td.currentBatchSize++
	return td.rd.DeleteRow(ctx, td.b, values, pm, traceKV)
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
	if td.tableDesc().IsInterleaved() {
		log.VEvent(ctx, 2, "delete forced to scan: table is interleaved")
		return td.deleteAllRowsScan(ctx, resume, limit, traceKV)
	}
	// TODO(pbardea): Is this ever called anymore?
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
		tablePrefix := td.rd.Helper.Codec.TablePrefix(uint32(td.tableDesc().GetID()))
		// Delete rows and indexes starting with the table's prefix.
		resume = roachpb.Span{
			Key:    tablePrefix,
			EndKey: tablePrefix.PrefixEnd(),
		}
	}

	log.VEventf(ctx, 2, "DelRange %s - %s", resume.Key, resume.EndKey)
	td.b.DelRange(resume.Key, resume.EndKey, false /* returnKeys */)
	td.b.Header.MaxSpanRequestKeys = limit
	if err := td.finalize(ctx); err != nil {
		return resume, err
	}
	if l := len(td.b.Results); l != 1 {
		panic(errors.AssertionFailedf("%d results returned", l))
	}
	return td.b.Results[0].ResumeSpanAsValue(), nil
}

func (td *tableDeleter) deleteAllRowsScan(
	ctx context.Context, resume roachpb.Span, limit int64, traceKV bool,
) (roachpb.Span, error) {
	if resume.Key == nil {
		resume = td.tableDesc().PrimaryIndexSpan(td.rd.Helper.Codec)
	}

	var valNeededForCol util.FastIntSet
	for i := range td.rd.FetchCols {
		col := td.rd.FetchCols[i].GetID()
		valNeededForCol.Add(td.rd.FetchColIDtoRowIndex.GetDefault(col))
	}

	var rf row.Fetcher
	tableArgs := row.FetcherTableArgs{
		Desc:            td.tableDesc(),
		Index:           td.tableDesc().GetPrimaryIndex(),
		ColIdxMap:       td.rd.FetchColIDtoRowIndex,
		Cols:            td.rd.FetchCols,
		ValNeededForCol: valNeededForCol,
	}
	if err := rf.Init(
		ctx,
		td.rd.Helper.Codec,
		false, /* reverse */
		// TODO(nvanbenschoten): it might make sense to use a FOR_UPDATE locking
		// strength here. Consider hooking this in to the same knob that will
		// control whether we perform locking implicitly during DELETEs.
		descpb.ScanLockingStrength_FOR_NONE,
		descpb.ScanLockingWaitPolicy_BLOCK,
		false, /* isCheck */
		td.alloc,
		// TODO(bulkio): this might need a memory monitor for the slow case of truncate.
		nil, /* memMonitor */
		tableArgs,
	); err != nil {
		return resume, err
	}
	if err := rf.StartScan(
		ctx, td.txn, roachpb.Spans{resume}, true /* limit batches */, 0, traceKV, td.forceProductionBatchSizes,
	); err != nil {
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
		// An empty PartialIndexUpdateHelper is passed here, meaning that DEL
		// operations will be issued for every partial index, regardless of
		// whether or not the row is indexed by the partial index.
		// TODO(mgartner): Try evaluating each partial index predicate
		// expression for each row and benchmark to determine if it is faster
		// than simply issuing DEL operations for entries that do not exist.
		var pm row.PartialIndexUpdateHelper
		if err = td.row(ctx, datums, pm, traceKV); err != nil {
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
	ctx context.Context, idx catalog.Index, resume roachpb.Span, limit int64, traceKV bool,
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
	ctx context.Context, idx catalog.Index, resume roachpb.Span, limit int64, traceKV bool,
) (roachpb.Span, error) {
	if resume.Key == nil {
		resume = td.tableDesc().IndexSpan(td.rd.Helper.Codec, idx.GetID())
	}

	if traceKV {
		log.VEventf(ctx, 2, "DelRange %s - %s", resume.Key, resume.EndKey)
	}
	td.b.DelRange(resume.Key, resume.EndKey, false /* returnKeys */)
	td.b.Header.MaxSpanRequestKeys = limit
	if err := td.finalize(ctx); err != nil {
		return resume, err
	}
	if l := len(td.b.Results); l != 1 {
		panic(errors.AssertionFailedf("%d results returned, expected 1", l))
	}
	return td.b.Results[0].ResumeSpanAsValue(), nil
}

func (td *tableDeleter) clearIndex(ctx context.Context, idx catalog.Index) error {
	if idx.IsInterleaved() {
		return errors.Errorf("unexpected interleaved index %d", idx.GetID())
	}

	sp := td.tableDesc().IndexSpan(td.rd.Helper.Codec, idx.GetID())

	// ClearRange cannot be run in a transaction, so create a
	// non-transactional batch to send the request.

	// TODO(sumeer): this is bypassing admission control, since it is not using
	// the currently instrumented shared code paths.
	b := &kv.Batch{}
	b.AddRawRequest(&roachpb.ClearRangeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    sp.Key,
			EndKey: sp.EndKey,
		},
	})
	return td.txn.DB().Run(ctx, b)
}

func (td *tableDeleter) deleteIndexScan(
	ctx context.Context, idx catalog.Index, resume roachpb.Span, limit int64, traceKV bool,
) (roachpb.Span, error) {
	if resume.Key == nil {
		resume = td.tableDesc().PrimaryIndexSpan(td.rd.Helper.Codec)
	}

	var valNeededForCol util.FastIntSet
	for i := range td.rd.FetchCols {
		col := td.rd.FetchCols[i].GetID()
		valNeededForCol.Add(td.rd.FetchColIDtoRowIndex.GetDefault(col))
	}

	var rf row.Fetcher
	tableArgs := row.FetcherTableArgs{
		Desc:            td.tableDesc(),
		Index:           td.tableDesc().GetPrimaryIndex(),
		ColIdxMap:       td.rd.FetchColIDtoRowIndex,
		Cols:            td.rd.FetchCols,
		ValNeededForCol: valNeededForCol,
	}
	if err := rf.Init(
		ctx,
		td.rd.Helper.Codec,
		false, /* reverse */
		// TODO(nvanbenschoten): it might make sense to use a FOR_UPDATE locking
		// strength here. Consider hooking this in to the same knob that will
		// control whether we perform locking implicitly during DELETEs.
		descpb.ScanLockingStrength_FOR_NONE,
		descpb.ScanLockingWaitPolicy_BLOCK,
		false, /* isCheck */
		td.alloc,
		// TODO(bulkio): this might need a memory monitor.
		nil, /* memMonitor */
		tableArgs,
	); err != nil {
		return resume, err
	}
	if err := rf.StartScan(
		ctx, td.txn, roachpb.Spans{resume}, true /* limit batches */, 0, traceKV, td.forceProductionBatchSizes,
	); err != nil {
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
	return resume, td.finalize(ctx)
}

func (td *tableDeleter) tableDesc() catalog.TableDescriptor {
	return td.rd.Helper.TableDesc
}
