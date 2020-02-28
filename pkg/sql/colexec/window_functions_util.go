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

import (
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

const columnOmitted = -1

var (
	windowFnNeedsPeersInfo map[execinfrapb.WindowerSpec_WindowFunc]bool
	// WindowFnOutputType is a mapping from a window function to its output type.
	// Only window functions supported by the vectorized engine are present.
	WindowFnOutputType map[execinfrapb.WindowerSpec_WindowFunc]*types.T
)

func init() {
	windowFnNeedsPeersInfo = make(map[execinfrapb.WindowerSpec_WindowFunc]bool)
	windowFnNeedsPeersInfo[execinfrapb.WindowerSpec_ROW_NUMBER] = false
	windowFnNeedsPeersInfo[execinfrapb.WindowerSpec_RANK] = true
	windowFnNeedsPeersInfo[execinfrapb.WindowerSpec_DENSE_RANK] = true
	windowFnNeedsPeersInfo[execinfrapb.WindowerSpec_PERCENT_RANK] = true
	windowFnNeedsPeersInfo[execinfrapb.WindowerSpec_CUME_DIST] = true

	WindowFnOutputType = make(map[execinfrapb.WindowerSpec_WindowFunc]*types.T)
	WindowFnOutputType[execinfrapb.WindowerSpec_ROW_NUMBER] = types.Int
	WindowFnOutputType[execinfrapb.WindowerSpec_RANK] = types.Int
	WindowFnOutputType[execinfrapb.WindowerSpec_DENSE_RANK] = types.Int
	WindowFnOutputType[execinfrapb.WindowerSpec_PERCENT_RANK] = types.Float
	WindowFnOutputType[execinfrapb.WindowerSpec_CUME_DIST] = types.Float
}
