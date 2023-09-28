// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package op

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// RelocateRangeOp contains the information for a relocate range operation.
type RelocateRangeOp struct {
	baseOp
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget
	key                           state.Key
	transferLeaseToFirstVoter     bool
}

// NewRelocateRangeOp returns a new NewRelocateRangeOp.
func NewRelocateRangeOp(
	tick time.Time,
	key roachpb.Key,
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
	transferLeaseToFirstVoter bool,
) *RelocateRangeOp {
	return &RelocateRangeOp{
		baseOp:                    newBaseOp(tick),
		voterTargets:              voterTargets,
		nonVoterTargets:           nonVoterTargets,
		key:                       state.ToKey(key),
		transferLeaseToFirstVoter: transferLeaseToFirstVoter,
	}
}

func (rro *RelocateRangeOp) error(err error) {
	augmentedErr := errors.Wrapf(err, "Unable to relocate key=%d,voters=%v,nonvoters=%v",
		rro.key, rro.voterTargets, rro.nonVoterTargets)
	rro.errs = append(rro.errs, augmentedErr)
}

// SimRelocateOneOptions holds the necessary information to call RelocateOne,
// to generate a suggested replication change.
type SimRelocateOneOptions struct {
	allocator allocatorimpl.Allocator
	storePool storepool.AllocatorStorePool
	state     state.State
}

// Allocator returns the allocator for the store this replica is on.
func (s *SimRelocateOneOptions) Allocator() allocatorimpl.Allocator {
	return s.allocator
}

// StorePool returns the store's configured store pool.
func (s *SimRelocateOneOptions) StorePool() storepool.AllocatorStorePool {
	return s.storePool
}

// LoadSpanConfig returns the span configuration for the range with start key.
func (s *SimRelocateOneOptions) LoadSpanConfig(
	ctx context.Context, startKey roachpb.RKey,
) (*roachpb.SpanConfig, error) {
	return s.state.RangeFor(state.ToKey(startKey.AsRawKey())).SpanConfig(), nil
}

// Leaseholder returns the descriptor of the replica which holds the lease on
// the range with start key.
func (s *SimRelocateOneOptions) Leaseholder(
	ctx context.Context, startKey roachpb.RKey,
) (roachpb.ReplicaDescriptor, error) {
	if desc, ok := s.state.LeaseHolderReplica(s.state.RangeFor(state.ToKey(startKey.AsRawKey())).RangeID()); ok {
		return desc.Descriptor(), nil
	}
	return roachpb.ReplicaDescriptor{}, errors.Errorf("Unable to find leaseholder for key %s.", startKey)
}

// LHRemovalAllowed returns true if the lease holder may be removed during
// a replication change.
func (s *SimRelocateOneOptions) LHRemovalAllowed(ctx context.Context) bool {
	return true
}
