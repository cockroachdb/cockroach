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

var windowFnNeedsPeersInfo map[execinfrapb.WindowerSpec_WindowFunc]bool

func init() {
	windowFnNeedsPeersInfo = make(map[execinfrapb.WindowerSpec_WindowFunc]bool)
	windowFnNeedsPeersInfo[execinfrapb.WindowerSpec_ROW_NUMBER] = false
	windowFnNeedsPeersInfo[execinfrapb.WindowerSpec_RANK] = true
	windowFnNeedsPeersInfo[execinfrapb.WindowerSpec_DENSE_RANK] = true
}
