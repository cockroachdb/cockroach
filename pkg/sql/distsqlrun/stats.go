// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlrun

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// InputStatCollector wraps a RowSource and collects stats from it.
type InputStatCollector struct {
	RowSource
	InputStats
}

var _ RowSource = &InputStatCollector{}

// NewInputStatCollector creates a new InputStatCollector that wraps the given
// input.
func NewInputStatCollector(input RowSource) *InputStatCollector {
	return &InputStatCollector{RowSource: input}
}

// Next implements the RowSource interface. It calls Next on the embedded
// RowSource and collects stats.
func (isc *InputStatCollector) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	start := timeutil.Now()
	row, meta := isc.RowSource.Next()
	if row != nil {
		isc.NumRows++
	}
	isc.StallTime += timeutil.Since(start)
	return row, meta
}

const (
	rowsReadTagSuffix  = "input.rows"
	stallTimeTagSuffix = "stalltime"
	maxMemoryTagSuffix = "mem.max"
	maxDiskTagSuffix   = "disk.max"
	bytesReadTagSuffix = "bytes.read"
)

// Stats is a utility method that returns a map of the InputStats` stats to
// output to a trace as tags. The given prefix is prefixed to the keys.
func (is InputStats) Stats(prefix string) map[string]string {
	return map[string]string{
		prefix + rowsReadTagSuffix:  fmt.Sprintf("%d", is.NumRows),
		prefix + stallTimeTagSuffix: fmt.Sprintf("%v", is.RoundStallTime()),
	}
}

const (
	rowsReadQueryPlanSuffix  = "rows read"
	stallTimeQueryPlanSuffix = "stall time"
	maxMemoryQueryPlanSuffix = "max memory used"
	maxDiskQueryPlanSuffix   = "max disk used"
	bytesReadQueryPlanSuffix = "bytes read"
)

// StatsForQueryPlan is a utility method that returns a list of the InputStats'
// stats to output on a query plan. The given prefix is prefixed to each element
// in the returned list.
func (is InputStats) StatsForQueryPlan(prefix string) []string {
	return []string{
		fmt.Sprintf("%s%s: %d", prefix, rowsReadQueryPlanSuffix, is.NumRows),
		fmt.Sprintf("%s%s: %v", prefix, stallTimeQueryPlanSuffix, is.RoundStallTime()),
	}
}

// RoundStallTime returns the InputStats' StallTime rounded to the nearest
// time.Millisecond.
func (is InputStats) RoundStallTime() time.Duration {
	return is.StallTime.Round(time.Microsecond)
}

// rowFetcher is an interface used to abstract a row fetcher so that a stat
// collector wrapper can be plugged in.
type rowFetcher interface {
	RowSource
	StartScan(
		_ context.Context, _ *client.Txn, _ roachpb.Spans, limitBatches bool, limitHint int64, traceKV bool,
	) error
	StartInconsistentScan(
		_ context.Context,
		_ *client.DB,
		initialTimestamp hlc.Timestamp,
		maxTimestampAge time.Duration,
		spans roachpb.Spans,
		limitBatches bool,
		limitHint int64,
		traceKV bool,
	) error

	// PartialKey is not stat-related but needs to be supported.
	PartialKey(int) (roachpb.Key, error)
	Reset()
	GetBytesRead() int64
	GetRangesInfo() []roachpb.RangeInfo
	NextRowWithErrors(context.Context) (sqlbase.EncDatumRow, error)
}

// rowFetcherWrapper is used only by processors that need to wrap calls to
// Fetcher.NextRow() in a RowSource implementation.
type rowFetcherWrapper struct {
	ctx context.Context
	*row.Fetcher
}

var _ RowSource = &rowFetcherWrapper{}

// Start is part of the RowSource interface.
func (w *rowFetcherWrapper) Start(ctx context.Context) context.Context {
	w.ctx = ctx
	return ctx
}

// Next() calls NextRow() on the underlying Fetcher. If an error is encountered,
// it is returned via a ProducerMetadata.
func (w *rowFetcherWrapper) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	row, _, _, err := w.NextRow(w.ctx)
	if err != nil {
		return row, &distsqlpb.ProducerMetadata{Err: err}
	}
	return row, nil
}
func (w rowFetcherWrapper) OutputTypes() []types.T { return nil }
func (w rowFetcherWrapper) ConsumerDone()          {}
func (w rowFetcherWrapper) ConsumerClosed()        {}

type rowFetcherStatCollector struct {
	*rowFetcherWrapper
	inputStatCollector *InputStatCollector
	startScanStallTime time.Duration
}

var _ rowFetcher = &rowFetcherStatCollector{}

func newRowFetcherStatCollector(f *row.Fetcher) *rowFetcherStatCollector {
	fWrapper := &rowFetcherWrapper{Fetcher: f}
	return &rowFetcherStatCollector{
		rowFetcherWrapper:  fWrapper,
		inputStatCollector: NewInputStatCollector(fWrapper),
	}
}

func (c *rowFetcherStatCollector) Next() (sqlbase.EncDatumRow, *distsqlpb.ProducerMetadata) {
	return c.inputStatCollector.Next()
}

func (c *rowFetcherStatCollector) StartScan(
	ctx context.Context,
	txn *client.Txn,
	spans roachpb.Spans,
	limitBatches bool,
	limitHint int64,
	traceKV bool,
) error {
	start := timeutil.Now()
	err := c.rowFetcherWrapper.StartScan(ctx, txn, spans, limitBatches, limitHint, traceKV)
	c.startScanStallTime += timeutil.Since(start)
	return err
}

func (c *rowFetcherStatCollector) StartInconsistentScan(
	ctx context.Context,
	db *client.DB,
	initialTimestamp hlc.Timestamp,
	maxTimestampAge time.Duration,
	spans roachpb.Spans,
	limitBatches bool,
	limitHint int64,
	traceKV bool,
) error {
	start := timeutil.Now()
	err := c.rowFetcherWrapper.StartInconsistentScan(
		ctx, db, initialTimestamp, maxTimestampAge, spans, limitBatches, limitHint, traceKV,
	)
	c.startScanStallTime += timeutil.Since(start)
	return err
}
