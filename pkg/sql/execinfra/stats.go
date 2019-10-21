// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
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
func (isc *InputStatCollector) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
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
	// MaxMemoryTagSuffix is the tag suffix for the max memory used stat.
	MaxMemoryTagSuffix = "mem.max"
	// MaxDiskTagSuffix is the tag suffix for the max disk used stat.
	MaxDiskTagSuffix = "disk.max"
	// BytesReadTagSuffix is the tag suffix for the bytes read stat.
	BytesReadTagSuffix = "bytes.read"
)

// Stats is a utility method that returns a map of the InputStats` stats to
// output to a trace as tags. The given prefix is prefixed to the keys.
func (is InputStats) Stats(prefix string) map[string]string {
	return map[string]string{
		prefix + rowsReadTagSuffix:  fmt.Sprintf("%d", is.NumRows),
		prefix + stallTimeTagSuffix: is.RoundStallTime().String(),
	}
}

const (
	rowsReadQueryPlanSuffix  = "rows read"
	stallTimeQueryPlanSuffix = "stall time"
	// MaxMemoryQueryPlanSuffix is the tag suffix for the max memory used.
	MaxMemoryQueryPlanSuffix = "max memory used"
	// MaxDiskQueryPlanSuffix is the tag suffix for the max disk used.
	MaxDiskQueryPlanSuffix = "max disk used"
	// BytesReadQueryPlanSuffix is the tag suffix for the bytes read.
	BytesReadQueryPlanSuffix = "bytes read"
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

// RowFetcherWrapper is used only by processors that need to wrap calls to
// Fetcher.NextRow() in a RowSource implementation.
type RowFetcherWrapper struct {
	ctx context.Context
	*row.Fetcher
}

var _ RowSource = &RowFetcherWrapper{}

// Start is part of the RowSource interface.
func (w *RowFetcherWrapper) Start(ctx context.Context) context.Context {
	w.ctx = ctx
	return ctx
}

// Next calls NextRow() on the underlying Fetcher. If an error is encountered,
// it is returned via a ProducerMetadata.
func (w *RowFetcherWrapper) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	row, _, _, err := w.NextRow(w.ctx)
	if err != nil {
		return row, &execinfrapb.ProducerMetadata{Err: err}
	}
	return row, nil
}

// OutputTypes is part of the RowSource interface.
func (w RowFetcherWrapper) OutputTypes() []types.T { return nil }

// ConsumerDone is part of the RowSource interface.
func (w RowFetcherWrapper) ConsumerDone() {}

// ConsumerClosed is part of the RowSource interface.
func (w RowFetcherWrapper) ConsumerClosed() {}

// RowFetcherStatCollector is a RowFetcherWrapper that collects stats.
type RowFetcherStatCollector struct {
	*RowFetcherWrapper
	inputStatCollector *InputStatCollector
	startScanStallTime time.Duration
}

var _ RowFetcher = &RowFetcherStatCollector{}

// NewRowFetcherStatCollector returns a new RowFetcherStatCollector.
func NewRowFetcherStatCollector(f *row.Fetcher) *RowFetcherStatCollector {
	fWrapper := &RowFetcherWrapper{Fetcher: f}
	return &RowFetcherStatCollector{
		RowFetcherWrapper:  fWrapper,
		inputStatCollector: NewInputStatCollector(fWrapper),
	}
}

// Next is part of the RowSource interface.
func (c *RowFetcherStatCollector) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	return c.inputStatCollector.Next()
}

// StartScan is part of the RowFetcher interface.
func (c *RowFetcherStatCollector) StartScan(
	ctx context.Context,
	txn *client.Txn,
	spans roachpb.Spans,
	limitBatches bool,
	limitHint int64,
	traceKV bool,
) error {
	start := timeutil.Now()
	err := c.RowFetcherWrapper.StartScan(ctx, txn, spans, limitBatches, limitHint, traceKV)
	c.startScanStallTime += timeutil.Since(start)
	return err
}

// StartInconsistentScan is part of the RowFetcher interface.
func (c *RowFetcherStatCollector) StartInconsistentScan(
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
	err := c.RowFetcherWrapper.StartInconsistentScan(
		ctx, db, initialTimestamp, maxTimestampAge, spans, limitBatches, limitHint, traceKV,
	)
	c.startScanStallTime += timeutil.Since(start)
	return err
}
