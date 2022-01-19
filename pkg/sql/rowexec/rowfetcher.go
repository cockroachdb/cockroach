// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// rowFetcher is an interface used to abstract a row.Fetcher so that a stat
// collector wrapper can be plugged in.
type rowFetcher interface {
	StartScan(
		_ context.Context, _ *kv.Txn, _ roachpb.Spans, batchBytesLimit rowinfra.BytesLimit,
		rowLimitHint rowinfra.RowLimit, traceKV bool, forceProductionKVBatchSize bool,
	) error
	StartScanFrom(_ context.Context, _ row.KVBatchFetcher, traceKV bool) error
	StartInconsistentScan(
		_ context.Context,
		_ *kv.DB,
		initialTimestamp hlc.Timestamp,
		maxTimestampAge time.Duration,
		spans roachpb.Spans,
		batchBytesLimit rowinfra.BytesLimit,
		rowLimitHint rowinfra.RowLimit,
		traceKV bool,
		forceProductionKVBatchSize bool,
	) error

	NextRow(ctx context.Context) (rowenc.EncDatumRow, error)
	NextRowInto(
		ctx context.Context, destination rowenc.EncDatumRow, colIdxMap catalog.TableColMap,
	) (ok bool, err error)

	// PartialKey is not stat-related but needs to be supported.
	PartialKey(nCols int) (roachpb.Key, error)
	Reset()
	GetBytesRead() int64
	// Close releases any resources held by this fetcher.
	Close(ctx context.Context)
}

// makeRowFetcher creates and initializes a fetcher.
func makeRowFetcher(
	flowCtx *execinfra.FlowCtx,
	desc catalog.TableDescriptor,
	indexIdx int,
	columns []catalog.Column,
	reverseScan bool,
	mon *mon.BytesMonitor,
	alloc *tree.DatumAlloc,
	lockStrength descpb.ScanLockingStrength,
	lockWaitPolicy descpb.ScanLockingWaitPolicy,
) (*row.Fetcher, error) {
	fetcher := &row.Fetcher{}
	if indexIdx >= len(desc.ActiveIndexes()) {
		return nil, errors.Errorf("invalid indexIdx %d", indexIdx)
	}
	index := desc.ActiveIndexes()[indexIdx]
	isSecondaryIndex := !index.Primary()

	tableArgs := row.FetcherTableArgs{
		Desc:             desc,
		Index:            index,
		IsSecondaryIndex: isSecondaryIndex,
		Columns:          columns,
	}

	if err := fetcher.Init(
		flowCtx.EvalCtx.Context,
		flowCtx.Codec(),
		reverseScan,
		lockStrength,
		lockWaitPolicy,
		flowCtx.EvalCtx.SessionData().LockTimeout,
		alloc,
		mon,
		tableArgs,
	); err != nil {
		return nil, err
	}
	return fetcher, nil
}

// makeRowFetcherLegacy is a legacy version of the row fetcher which uses
// the valNeededForCol ordinal set to determine the fetcher columns.
func makeRowFetcherLegacy(
	flowCtx *execinfra.FlowCtx,
	desc catalog.TableDescriptor,
	indexIdx int,
	reverseScan bool,
	valNeededForCol util.FastIntSet,
	mon *mon.BytesMonitor,
	alloc *tree.DatumAlloc,
	lockStrength descpb.ScanLockingStrength,
	lockWaitPolicy descpb.ScanLockingWaitPolicy,
	withSystemColumns bool,
	invertedColumn catalog.Column,
) (*row.Fetcher, error) {
	cols := make([]catalog.Column, 0, len(desc.AllColumns()))
	for i, col := range desc.ReadableColumns() {
		if valNeededForCol.Contains(i) {
			if invertedColumn != nil && col.GetID() == invertedColumn.GetID() {
				col = invertedColumn
			}
			cols = append(cols, col)
		}
	}
	if withSystemColumns {
		start := len(desc.ReadableColumns())
		for i, col := range desc.SystemColumns() {
			if valNeededForCol.Contains(start + i) {
				cols = append(cols, col)
			}
		}
	}
	return makeRowFetcher(
		flowCtx,
		desc,
		indexIdx,
		cols,
		reverseScan,
		mon,
		alloc,
		lockStrength,
		lockWaitPolicy,
	)
}
