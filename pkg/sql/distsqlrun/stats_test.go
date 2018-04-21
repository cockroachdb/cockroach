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
	"testing"

	"context"

	"math"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// TestInputStatCollector verifies that an InputStatCollector correctly collects
// stats from an input.
func TestInputStatCollector(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numRows = 100

	isc := NewInputStatCollector(
		NewRowBuffer(oneIntCol, makeIntRows(numRows, 1), RowBufferArgs{}), "row buffer",
	)
	for row, meta := isc.Next(); row != nil || meta != nil; row, meta = isc.Next() {
	}
	if isc.NumRows != numRows {
		t.Fatalf("counted %d rows but expected %d", isc.NumRows, numRows)
	}
}

// TestBytesAccountStatCollector verifies that a BytesAccountStatCollector
// correctly reports stats from a mon.BytesAccount.
func TestBytesAccountStatCollector(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	m := mon.MakeUnlimitedMonitor(ctx, "test", mon.MemoryResource, nil, nil, math.MaxInt64, nil)
	bsc := NewBytesAccountStatCollector(
		m.NewBoundAccount(), "bytes account",
	)

	bsc.Grow(ctx, 120)
	bsc.Shrink(ctx, 20)
	bsc.Grow(ctx, 10)
	bsc.Resize(ctx, 10, 100)
	bsc.Clear(ctx)
	bsc.Grow(ctx, 50)
	bsc.Close(ctx)

	const expectedMaxUsed = 200

	if bsc.MaxUsed != expectedMaxUsed {
		t.Fatalf("counted %d bytes but expected %d", bsc.MaxUsed, expectedMaxUsed)
	}

}
