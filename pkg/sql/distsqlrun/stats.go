// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlrun

import (
	"fmt"

	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// StatSummarizer summarizes stats.
type StatSummarizer interface {
	SummarizeStats() string
}

// InputStatCollector wraps a RowSource and collects stats from it.
type InputStatCollector struct {
	RowSource
	InputStats
}

var _ RowSource = &InputStatCollector{}
var _ StatSummarizer = &InputStatCollector{}

// NewInputStatCollector creates a new InputStatCollector that wraps the given
// input described by name.
func NewInputStatCollector(input RowSource, name string) *InputStatCollector {
	return &InputStatCollector{RowSource: input, InputStats: InputStats{Name: name}}
}

// Next implements the RowSource interface. It calls Next on the embedded
// RowSource and collects stats.
func (isc *InputStatCollector) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	row, meta := isc.RowSource.Next()
	if row != nil {
		isc.NumRows++
	}
	return row, meta
}

// SummarizeStats implements the StatSummarizer interface.
func (is InputStats) SummarizeStats() string {
	return fmt.Sprintf("stat summary for %s: %d rows read", is.Name, is.NumRows)
}

// BytesAccountStatCollector wraps a mon.BytesAccount and collects stats from
// it.
type BytesAccountStatCollector struct {
	mon.BytesAccount
	BytesAccountStats
}

var _ mon.BytesAccount = &BytesAccountStatCollector{}
var _ StatSummarizer = &BytesAccountStatCollector{}

// NewBytesAccountStatCollector creates a new BytesAccountStatCollector that
// wraps the given mon.BytesAccount described by name.
func NewBytesAccountStatCollector(acc mon.BytesAccount, name string) *BytesAccountStatCollector {
	return &BytesAccountStatCollector{
		BytesAccount:      acc,
		BytesAccountStats: BytesAccountStats{Name: name},
	}
}

// maybeUpdateMaxUsed looks at the size of the mon.BytesAccount and updates the
// stats if it's a greater number than seen before.
func (bsc *BytesAccountStatCollector) maybeUpdateMaxUsed() {
	used := int32(bsc.BytesAccount.Used())
	if used > bsc.BytesAccountStats.MaxUsed {
		bsc.BytesAccountStats.MaxUsed = used
	}
}

// Grow implements the mon.BytesAccount interface. It calls Grow on the embedded
// mon.BytesAccount and collects stats.
// TODO(asubiotto): Not sure we really care about performance. This is the most
// correct way to get the max used bytes. An alternative is to update on
// Shrink(), Resize(), Clear(), Close(), and SummarizeStats(). This would also
// be correct but probably error-prone.
func (bsc *BytesAccountStatCollector) Grow(ctx context.Context, x int64) error {
	if err := bsc.BytesAccount.Grow(ctx, x); err != nil {
		return err
	}
	bsc.maybeUpdateMaxUsed()
	return nil
}

// Resize implements the mon.BytesAccount interface. It calls Resize on the
// embedded mon.BytesAccount and collects stats.
func (bsc *BytesAccountStatCollector) Resize(
	ctx context.Context, oldSize int64, newSize int64,
) error {
	if err := bsc.BytesAccount.Resize(ctx, oldSize, newSize); err != nil {
		return err
	}
	bsc.maybeUpdateMaxUsed()
	return nil
}

// SummarizeStats implements the StatSummarizer interface.
func (bs BytesAccountStats) SummarizeStats() string {
	return fmt.Sprintf(
		"stat summary for %s: %s used", bs.Name, humanizeutil.IBytes(int64(bs.MaxUsed)),
	)
}
