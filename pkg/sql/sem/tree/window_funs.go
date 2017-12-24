// Copyright 2017 The Cockroach Authors.
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

package tree

import "context"

// IndexedRow is a row with a corresponding index.
type IndexedRow struct {
	Idx int
	Row Datums
}

// WindowFrame is a view into a subset of data over which calculations are made.
type WindowFrame struct {
	// constant for all calls to WindowFunc.Add
	Rows        []IndexedRow
	ArgIdxStart int // the index which arguments to the window function begin
	ArgCount    int // the number of window function arguments

	// changes for each row (each call to WindowFunc.Add)
	RowIdx int // the current row index

	// changes for each peer group
	FirstPeerIdx int // the first index in the current peer group
	PeerRowCount int // the number of rows in the current peer group
}

// Rank returns the rank of this frame.
func (wf WindowFrame) Rank() int {
	return wf.RowIdx + 1
}

// RowCount returns the number of rows in this frame.
func (wf WindowFrame) RowCount() int {
	return len(wf.Rows)
}

// FrameSize returns the size of this frame.
// TODO(nvanbenschoten): This definition only holds while we don't support
// frame specification (RANGE or ROWS) in the OVER clause.
func (wf WindowFrame) FrameSize() int {
	return wf.FirstPeerIdx + wf.PeerRowCount
}

// FirstInPeerGroup returns if the current row is the first in its peer group.
func (wf WindowFrame) FirstInPeerGroup() bool {
	return wf.RowIdx == wf.FirstPeerIdx
}

// Args returns the current argument set in the window frame.
func (wf WindowFrame) Args() Datums {
	return wf.ArgsWithRowOffset(0)
}

// ArgsWithRowOffset returns the argumnent set at the given offset in the window frame.
func (wf WindowFrame) ArgsWithRowOffset(offset int) Datums {
	return wf.Rows[wf.RowIdx+offset].Row[wf.ArgIdxStart : wf.ArgIdxStart+wf.ArgCount]
}

// WindowFunc performs a computation on each row using data from a provided WindowFrame.
type WindowFunc interface {
	// Compute computes the window function for the provided window frame, given the
	// current state of WindowFunc. The method should be called sequentially for every
	// row in a partition in turn with the desired ordering of the WindowFunc. This is
	// because there is an implicit carried dependency between each row and all those
	// that have come before it (like in an AggregateFunc). As such, this approach does
	// not present any exploitable associativity/commutativity for optimization.
	Compute(context.Context, *EvalContext, WindowFrame) (Datum, error)

	// Close allows the window function to free any memory it requested during execution,
	// such as during the execution of an aggregation like CONCAT_AGG or ARRAY_AGG.
	Close(context.Context, *EvalContext)
}
