// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import "github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"

const columnOmitted = -1

var (
	// windowFnNeedsPeersInfo is a map from a window function to a boolean that
	// indicates whether the window function pays attention to the concept of
	// "peers" during its computation ("peers" are tuples within the same partition
	// - from PARTITION BY clause - that are not distinct on the columns in ORDER
	// BY clause). For most window functions, the result of computation should be
	// the same for "peers", so most window functions do need this information.
	windowFnNeedsPeersInfo map[execinfrapb.WindowerSpec_WindowFunc]bool

	// SupportedWindowFns contains all window functions supported by the
	// vectorized engine.
	SupportedWindowFns = []execinfrapb.WindowerSpec_WindowFunc{
		execinfrapb.WindowerSpec_ROW_NUMBER,
		execinfrapb.WindowerSpec_RANK,
		execinfrapb.WindowerSpec_DENSE_RANK,
		execinfrapb.WindowerSpec_PERCENT_RANK,
		execinfrapb.WindowerSpec_CUME_DIST,
	}
)

func init() {
	windowFnNeedsPeersInfo = make(map[execinfrapb.WindowerSpec_WindowFunc]bool)
	// row_number doesn't pay attention to the concept of "peers."
	windowFnNeedsPeersInfo[execinfrapb.WindowerSpec_ROW_NUMBER] = false
	windowFnNeedsPeersInfo[execinfrapb.WindowerSpec_RANK] = true
	windowFnNeedsPeersInfo[execinfrapb.WindowerSpec_DENSE_RANK] = true
	windowFnNeedsPeersInfo[execinfrapb.WindowerSpec_PERCENT_RANK] = true
	windowFnNeedsPeersInfo[execinfrapb.WindowerSpec_CUME_DIST] = true
}
