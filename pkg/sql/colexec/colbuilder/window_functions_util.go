// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colbuilder

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
)

// SupportedWindowFns contains all window functions supported by the
// vectorized engine.
var SupportedWindowFns = map[execinfrapb.WindowerSpec_WindowFunc]struct{}{
	execinfrapb.WindowerSpec_ROW_NUMBER:   {},
	execinfrapb.WindowerSpec_RANK:         {},
	execinfrapb.WindowerSpec_DENSE_RANK:   {},
	execinfrapb.WindowerSpec_PERCENT_RANK: {},
	execinfrapb.WindowerSpec_CUME_DIST:    {},
}

// windowFnNeedsPeersInfo returns whether a window function pays attention to
// the concept of "peers" during its computation ("peers" are tuples within the
// same partition - from PARTITION BY clause - that are not distinct on the
// columns in ORDER BY clause). For most window functions, the result of
// computation should be the same for "peers", so most window functions do need
// this information.
func windowFnNeedsPeersInfo(windowFn execinfrapb.WindowerSpec_WindowFunc) bool {
	switch windowFn {
	case execinfrapb.WindowerSpec_ROW_NUMBER:
		// row_number doesn't pay attention to the concept of "peers."
		return false
	case
		execinfrapb.WindowerSpec_RANK,
		execinfrapb.WindowerSpec_DENSE_RANK,
		execinfrapb.WindowerSpec_PERCENT_RANK,
		execinfrapb.WindowerSpec_CUME_DIST:
		return true
	default:
		colexecerror.InternalError(fmt.Sprintf("window function %s is not supported", windowFn.String()))
		// This code is unreachable, but the compiler cannot infer that.
		return false
	}
}
