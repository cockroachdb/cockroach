// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserverpb

import "math"

var maxRaftCommandFooterSize = (&RaftCommandFooter{
	MaxLeaseIndex: math.MaxUint64,
}).Size()

// MaxRaftCommandFooterSize returns the maximum possible size of an
// encoded RaftCommandFooter proto.
func MaxRaftCommandFooterSize() int {
	return maxRaftCommandFooterSize
}
