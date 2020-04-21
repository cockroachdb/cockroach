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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// inputStatCollector wraps an execinfra.RowSource and collects stats from it.
type inputStatCollector struct {
	execinfra.RowSource
	InputStats
}

var _ execinfra.RowSource = &inputStatCollector{}
var _ execinfra.OpNode = &inputStatCollector{}

// newInputStatCollector creates a new inputStatCollector that wraps the given
// input.
func newInputStatCollector(input execinfra.RowSource) *inputStatCollector {
	return &inputStatCollector{RowSource: input}
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
	panic(fmt.Sprintf("invalid index %d", nth))
}

// Next implements the RowSource interface. It calls Next on the embedded
// RowSource and collects stats.
func (isc *inputStatCollector) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
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
	// bytesReadTagSuffix is the tag suffix for the bytes read stat.
	bytesReadTagSuffix = "bytes.read"
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
	// bytesReadQueryPlanSuffix is the tag suffix for the bytes read.
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

// rowFetcherStatCollector is a wrapper on top of a row.Fetcher that collects stats.
//
// Only row.Fetcher methods that collect stats are overridden.
type rowFetcherStatCollector struct {
	*row.Fetcher
	// stats contains the collected stats.
	stats              InputStats
	startScanStallTime time.Duration
}

var _ rowFetcher = &rowFetcherStatCollector{}

// newRowFetcherStatCollector returns a new rowFetcherStatCollector.
func newRowFetcherStatCollector(f *row.Fetcher) *rowFetcherStatCollector {
	return &rowFetcherStatCollector{Fetcher: f}
}

// NextRow is part of the rowFetcher interface.
func (c *rowFetcherStatCollector) NextRow(
	ctx context.Context,
) (sqlbase.EncDatumRow, *sqlbase.TableDescriptor, *sqlbase.IndexDescriptor, error) {
	start := timeutil.Now()
	row, t, i, err := c.Fetcher.NextRow(ctx)
	if row != nil {
		c.stats.NumRows++
	}
	c.stats.StallTime += timeutil.Since(start)
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
) error {
	start := timeutil.Now()
	err := c.Fetcher.StartScan(ctx, txn, spans, limitBatches, limitHint, traceKV)
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
) error {
	start := timeutil.Now()
	err := c.Fetcher.StartInconsistentScan(
		ctx, db, initialTimestamp, maxTimestampAge, spans, limitBatches, limitHint, traceKV,
	)
	c.startScanStallTime += timeutil.Since(start)
	return err
}

// getInputStats is a utility function to check whether the given input is
// collecting stats, returning true and the stats if so. If false is returned,
// the input is not collecting stats.
func getInputStats(flowCtx *execinfra.FlowCtx, input execinfra.RowSource) (InputStats, bool) {
	isc, ok := input.(*inputStatCollector)
	if !ok {
		return InputStats{}, false
	}
	return getStatsInner(flowCtx, isc.InputStats), true
}

func getStatsInner(flowCtx *execinfra.FlowCtx, stats InputStats) InputStats {
	if flowCtx.Cfg.TestingKnobs.DeterministicStats {
		stats.StallTime = 0
	}
	return stats
}

// getFetcherInputStats is a utility function to check whether the given input
// is collecting row fetcher stats, returning true and the stats if so. If
// false is returned, the input is not collecting row fetcher stats.
func getFetcherInputStats(flowCtx *execinfra.FlowCtx, f rowFetcher) (InputStats, bool) {
	rfsc, ok := f.(*rowFetcherStatCollector)
	if !ok {
		return InputStats{}, false
	}
	// Add row fetcher start scan stall time to Next() stall time.
	if !flowCtx.Cfg.TestingKnobs.DeterministicStats {
		rfsc.stats.StallTime += rfsc.startScanStallTime
	}
	return getStatsInner(flowCtx, rfsc.stats), true
}
