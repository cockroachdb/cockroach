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
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// rowFetcher is an interface used to abstract a row fetcher so that a stat
// collector wrapper can be plugged in.
type rowFetcher interface {
	StartScan(
		_ context.Context, _ *kv.Txn, _ roachpb.Spans, limitBatches bool, limitHint int64, traceKV bool,
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
	) error

	NextRow(ctx context.Context) (
		sqlbase.EncDatumRow, *sqlbase.TableDescriptor, *sqlbase.IndexDescriptor, error)

	// PartialKey is not stat-related but needs to be supported.
	PartialKey(int) (roachpb.Key, error)
	Reset()
	GetBytesRead() int64
	GetRangesInfo() []roachpb.RangeInfo
	NextRowWithErrors(context.Context) (sqlbase.EncDatumRow, error)
}

// initRowFetcher initializes the fetcher.
func initRowFetcher(
	flowCtx *execinfra.FlowCtx,
	fetcher *row.Fetcher,
	desc *sqlbase.TableDescriptor,
	indexIdx int,
	colIdxMap map[sqlbase.ColumnID]int,
	reverseScan bool,
	valNeededForCol util.FastIntSet,
	isCheck bool,
	alloc *sqlbase.DatumAlloc,
	scanVisibility execinfrapb.ScanVisibility,
	lockStr sqlbase.ScanLockingStrength,
) (index *sqlbase.IndexDescriptor, isSecondaryIndex bool, err error) {
	immutDesc := sqlbase.NewImmutableTableDescriptor(*desc)
	index, isSecondaryIndex, err = immutDesc.FindIndexByIndexIdx(indexIdx)
	if err != nil {
		return nil, false, err
	}

	cols := immutDesc.Columns
	if scanVisibility == execinfra.ScanVisibilityPublicAndNotPublic {
		cols = immutDesc.ReadableColumns
	}
	tableArgs := row.FetcherTableArgs{
		Desc:             immutDesc,
		Index:            index,
		ColIdxMap:        colIdxMap,
		IsSecondaryIndex: isSecondaryIndex,
		Cols:             cols,
		ValNeededForCol:  valNeededForCol,
	}
	if err := fetcher.Init(
		flowCtx.Codec(),
		reverseScan,
		lockStr,
		true, /* returnRangeInfo */
		isCheck,
		alloc,
		tableArgs,
	); err != nil {
		return nil, false, err
	}

	return index, isSecondaryIndex, nil
}
