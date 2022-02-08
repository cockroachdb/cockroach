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
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
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
		qualityOfService sessiondatapb.QoSLevel,
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
) (*row.Fetcher, error) {
	colIDs := make([]descpb.ColumnID, 0, len(desc.AllColumns()))
	for i, col := range desc.ReadableColumns() {
		if valNeededForCol.Contains(i) {
			colIDs = append(colIDs, col.GetID())
		}
	}
	if withSystemColumns {
		start := len(desc.ReadableColumns())
		for i, col := range desc.SystemColumns() {
			if valNeededForCol.Contains(start + i) {
				colIDs = append(colIDs, col.GetID())
			}
		}
	}

	if indexIdx >= len(desc.ActiveIndexes()) {
		return nil, errors.Errorf("invalid indexIdx %d", indexIdx)
	}
	index := desc.ActiveIndexes()[indexIdx]

	var spec descpb.IndexFetchSpec
	if err := rowenc.InitIndexFetchSpec(&spec, flowCtx.Codec(), desc, index, colIDs); err != nil {
		return nil, err
	}

	fetcher := &row.Fetcher{}
	if err := fetcher.Init(
		flowCtx.EvalCtx.Context,
		reverseScan,
		lockStrength,
		lockWaitPolicy,
		flowCtx.EvalCtx.SessionData().LockTimeout,
		alloc,
		mon,
		&spec,
	); err != nil {
		return nil, err
	}
	return fetcher, nil
}
