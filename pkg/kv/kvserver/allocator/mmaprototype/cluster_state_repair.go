// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/logtags"
)

// repair examines the ranges on the local store and proposes changes to bring
// them into compliance with their span configs. For example, it adds replicas
// when under-replicated, removes replicas when over-replicated, replaces dead
// or decommissioning replicas, and finalizes atomic replication changes.
//
// TODO(mma): implement repair logic.
func (re *rebalanceEnv) repair(
	ctx context.Context, localStoreID roachpb.StoreID,
) []ExternalRangeChange {
	re.mmaid++
	ctx = logtags.AddTag(ctx, "mmaid", re.mmaid)
	log.KvDistribution.Infof(ctx, "repair not yet implemented")
	return nil
}
