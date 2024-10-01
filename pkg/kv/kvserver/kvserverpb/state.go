// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
