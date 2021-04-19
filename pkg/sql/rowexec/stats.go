// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// inputStatCollector wraps an execinfra.RowSource and collects stats from it.
type inputStatCollector struct {
	execinfra.RowSource
	stats execinfrapb.InputStats
}

var _ execinfra.RowSource = &inputStatCollector{}
var _ execinfra.OpNode = &inputStatCollector{}

// newInputStatCollector creates a new inputStatCollector that wraps the given
// input.
func newInputStatCollector(input execinfra.RowSource) *inputStatCollector {
	res := &inputStatCollector{RowSource: input}
	res.stats.NumTuples.Set(0)
	return res
}

// ChildCount is part of the OpNode interface.
func (isc *inputStatCollector) ChildCount(verbose bool) int {
	if _, ok := isc.RowSource.(execinfra.OpNode); ok {
		return 1
	}
	return 0
}

// Child is part of the OpNode interface.
func (isc *inputStatCollector) Child(nth int, verbose bool) execinfra.OpNode {
	if nth == 0 {
		return isc.RowSource.(execinfra.OpNode)
	}
	panic(errors.AssertionFailedf("invalid index %d", nth))
}

// Next implements the RowSource interface. It calls Next on the embedded
// RowSource and collects stats.
func (isc *inputStatCollector) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	start := timeutil.Now()
	row, meta := isc.RowSource.Next()
	if row != nil {
		isc.stats.NumTuples.Add(1)
	}
	isc.stats.WaitTime.Add(timeutil.Since(start))
	return row, meta
}

// rowFetcherStatCollector is a wrapper on top of a row.Fetcher that collects stats.
//
// Only row.Fetcher methods that collect stats are overridden.
type rowFetcherStatCollector struct {
	*row.Fetcher
	// stats contains the collected stats.
	stats              execinfrapb.InputStats
	startScanStallTime time.Duration
}

var _ rowFetcher = &rowFetcherStatCollector{}

// newRowFetcherStatCollector returns a new rowFetcherStatCollector.
func newRowFetcherStatCollector(f *row.Fetcher) *rowFetcherStatCollector {
	res := &rowFetcherStatCollector{Fetcher: f}
	res.stats.NumTuples.Set(0)
	return res
}

// NextRow is part of the rowFetcher interface.
func (c *rowFetcherStatCollector) NextRow(
	ctx context.Context,
) (rowenc.EncDatumRow, catalog.TableDescriptor, catalog.Index, error) {
	start := timeutil.Now()
	row, t, i, err := c.Fetcher.NextRow(ctx)
	if row != nil {
		c.stats.NumTuples.Add(1)
	}
	c.stats.WaitTime.Add(timeutil.Since(start))
	return row, t, i, err
}

// StartScan is part of the rowFetcher interface.
func (c *rowFetcherStatCollector) StartScan(
	ctx context.Context,
	txn *kv.Txn,
	spans roachpb.Spans,
	limitBatches bool,
	limitHint int64,
	traceKV bool,
	forceProductionKVBatchSize bool,
) error {
	start := timeutil.Now()
	err := c.Fetcher.StartScan(ctx, txn, spans, limitBatches, limitHint, traceKV, forceProductionKVBatchSize)
	c.startScanStallTime += timeutil.Since(start)
	return err
}

// StartInconsistentScan is part of the rowFetcher interface.
func (c *rowFetcherStatCollector) StartInconsistentScan(
	ctx context.Context,
	db *kv.DB,
	initialTimestamp hlc.Timestamp,
	maxTimestampAge time.Duration,
	spans roachpb.Spans,
	limitBatches bool,
	limitHint int64,
	traceKV bool,
	forceProductionKVBatchSize bool,
) error {
	start := timeutil.Now()
	err := c.Fetcher.StartInconsistentScan(
		ctx, db, initialTimestamp, maxTimestampAge, spans, limitBatches, limitHint, traceKV, forceProductionKVBatchSize,
	)
	c.startScanStallTime += timeutil.Since(start)
	return err
}

// getInputStats is a utility function to check whether the given input is
// collecting stats, returning true and the stats if so. If false is returned,
// the input is not collecting stats.
func getInputStats(input execinfra.RowSource) (execinfrapb.InputStats, bool) {
	isc, ok := input.(*inputStatCollector)
	if !ok {
		return execinfrapb.InputStats{}, false
	}
	return isc.stats, true
}

// getFetcherInputStats is a utility function to check whether the given input
// is collecting row fetcher stats, returning true and the stats if so. If
// false is returned, the input is not collecting row fetcher stats.
func getFetcherInputStats(f rowFetcher) (execinfrapb.InputStats, bool) {
	rfsc, ok := f.(*rowFetcherStatCollector)
	if !ok {
		return execinfrapb.InputStats{}, false
	}
	// Add row fetcher start scan stall time to Next() stall time.
	rfsc.stats.WaitTime.Add(rfsc.startScanStallTime)
	return rfsc.stats, true
}
