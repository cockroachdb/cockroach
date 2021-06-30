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
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// rowFetcher is an interface used to abstract a row.Fetcher so that a stat
// collector wrapper can be plugged in.
type rowFetcher interface {
	StartScan(
		_ context.Context, _ *kv.Txn, _ roachpb.Spans, limitBatches bool,
		limitHint int64, traceKV bool, forceProductionKVBatchSize bool,
	) error
	StartInconsistentScan(
		_ context.Context,
		_ *kv.DB,
		initialTimestamp hlc.Timestamp,
		maxTimestampAge time.Duration,
		spans roachpb.Spans,
		limitBatches bool,
		limitHint int64,
		traceKV bool,
		forceProductionKVBatchSize bool,
	) error

	NextRow(ctx context.Context) (
		rowenc.EncDatumRow, catalog.TableDescriptor, catalog.Index, error)

	// PartialKey is not stat-related but needs to be supported.
	PartialKey(int) (roachpb.Key, error)
	Reset()
	GetBytesRead() int64
	NextRowWithErrors(context.Context) (rowenc.EncDatumRow, error)
	// Close releases any resources held by this fetcher.
	Close(ctx context.Context)
}

// initRowFetcher initializes the fetcher.
func initRowFetcher(
	flowCtx *execinfra.FlowCtx,
	fetcher *row.Fetcher,
	desc catalog.TableDescriptor,
	indexIdx int,
	colIdxMap catalog.TableColMap,
	reverseScan bool,
	valNeededForCol util.FastIntSet,
	isCheck bool,
	mon *mon.BytesMonitor,
	alloc *rowenc.DatumAlloc,
	scanVisibility execinfrapb.ScanVisibility,
	lockStrength descpb.ScanLockingStrength,
	lockWaitPolicy descpb.ScanLockingWaitPolicy,
	withSystemColumns bool,
	virtualColumn catalog.Column,
) (index catalog.Index, isSecondaryIndex bool, err error) {
	if indexIdx >= len(desc.ActiveIndexes()) {
		return nil, false, errors.Errorf("invalid indexIdx %d", indexIdx)
	}
	index = desc.ActiveIndexes()[indexIdx]
	isSecondaryIndex = !index.Primary()

	tableArgs := row.FetcherTableArgs{
		Desc:             desc,
		Index:            index,
		ColIdxMap:        colIdxMap,
		IsSecondaryIndex: isSecondaryIndex,
		ValNeededForCol:  valNeededForCol,
	}
	tableArgs.InitCols(desc, scanVisibility, withSystemColumns, virtualColumn)

	if err := fetcher.Init(
		flowCtx.EvalCtx.Context,
		flowCtx.Codec(),
		reverseScan,
		lockStrength,
		lockWaitPolicy,
		isCheck,
		alloc,
		mon,
		tableArgs,
	); err != nil {
		return nil, false, err
	}

	return index, isSecondaryIndex, nil
}
