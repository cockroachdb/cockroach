// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserverpb

import "github.com/cockroachdb/cockroach/pkg/storage/enginepb"

// ToStats converts the receiver to an MVCCStats.
func (ms *MVCCPersistentStats) ToStats() enginepb.MVCCStats {
	return enginepb.MVCCStats(*ms)
}

// ToStatsPtr converts the receiver to a *MVCCStats.
func (ms *MVCCPersistentStats) ToStatsPtr() *enginepb.MVCCStats {
	return (*enginepb.MVCCStats)(ms)
}
