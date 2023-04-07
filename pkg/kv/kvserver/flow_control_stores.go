// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// StoresForFlowControl is used to integrate with replication flow control. It
// exposes the underlying kvflowcontrol.Handles and is informed of (remote)
// stores we're no longer connected via the raft transport.
type StoresForFlowControl interface {
	kvflowcontrol.Handles
	RaftTransportDisconnectListener
}

// storesForFlowControl is a concrete implementation of the
// StoresForFlowControl interface, backed by a set of Stores.
type storesForFlowControl Stores

var _ StoresForFlowControl = &storesForFlowControl{}

// MakeStoresForFlowControl returns the canonical StoresForFlowControl
// implementation.
func MakeStoresForFlowControl(stores *Stores) StoresForFlowControl {
	return (*storesForFlowControl)(stores)
}

// Lookup is part of the StoresForFlowControl interface.
func (sh *storesForFlowControl) Lookup(
	rangeID roachpb.RangeID,
) (handle kvflowcontrol.Handle, found bool) {
	ls := (*Stores)(sh)
	if err := ls.VisitStores(func(s *Store) error {
		if h, ok := makeStoreForFlowControl(s).Lookup(rangeID); ok {
			handle = h
			found = true
		}
		return nil
	}); err != nil {
		ctx := ls.AnnotateCtx(context.Background())
		log.Errorf(ctx, "unexpected error: %s", err)
		return nil, false
	}
	return handle, found
}

// ResetStreams is part of the StoresForFlowControl interface.
func (sh *storesForFlowControl) ResetStreams(ctx context.Context) {
	ls := (*Stores)(sh)
	if err := ls.VisitStores(func(s *Store) error {
		makeStoreForFlowControl(s).ResetStreams(ctx)
		return nil
	}); err != nil {
		ctx = ls.AnnotateCtx(ctx)
		log.Errorf(ctx, "unexpected error: %s", err)
	}
}

// OnRaftTransportDisconnected is part of the StoresForFlowControl
// interface.
func (sh *storesForFlowControl) OnRaftTransportDisconnected(
	ctx context.Context, storeIDs ...roachpb.StoreID,
) {
	ls := (*Stores)(sh)
	if err := ls.VisitStores(func(s *Store) error {
		makeStoreForFlowControl(s).OnRaftTransportDisconnected(ctx, storeIDs...)
		return nil
	}); err != nil {
		ctx := ls.AnnotateCtx(context.Background())
		log.Errorf(ctx, "unexpected error: %s", err)
	}
}

// storeForFlowControl is a concrete implementation of the
// StoresForFlowControl interface, backed by a single Store.
type storeForFlowControl Store

var _ StoresForFlowControl = &storeForFlowControl{}

// makeStoreForFlowControl returns a new storeForFlowControl instance.
func makeStoreForFlowControl(store *Store) *storeForFlowControl {
	return (*storeForFlowControl)(store)
}

// Lookup is part of the StoresForFlowControl interface.
func (sh *storeForFlowControl) Lookup(
	rangeID roachpb.RangeID,
) (_ kvflowcontrol.Handle, found bool) {
	s := (*Store)(sh)
	repl := s.GetReplicaIfExists(rangeID)
	if repl == nil {
		return nil, false
	}
	return nil, false // TODO(irfansharif): Fill this in.
}

// ResetStreams is part of the StoresForFlowControl interface.
func (sh *storeForFlowControl) ResetStreams(ctx context.Context) {
	s := (*Store)(sh)
	s.VisitReplicas(func(r *Replica) (wantMore bool) {
		// TODO(irfansharif): Fill this in.
		return true
	})
}

// OnRaftTransportDisconnected is part of the StoresForFlowControl
// interface.
func (sh *storeForFlowControl) OnRaftTransportDisconnected(
	ctx context.Context, storeIDs ...roachpb.StoreID,
) {
	s := (*Store)(sh)
	s.mu.replicasByRangeID.Range(func(replica *Replica) {
		// TODO(irfansharif): Fill this in.
	})
}

// NoopStoresFlowControlIntegration is a no-op implementation of the
// StoresForFlowControl interface.
type NoopStoresFlowControlIntegration struct{}

var _ StoresForFlowControl = NoopStoresFlowControlIntegration{}

// Lookup is part of the StoresForFlowControl interface.
func (l NoopStoresFlowControlIntegration) Lookup(roachpb.RangeID) (kvflowcontrol.Handle, bool) {
	return nil, false
}

// ResetStreams is part of the StoresForFlowControl interface.
func (l NoopStoresFlowControlIntegration) ResetStreams(context.Context) {
}

// Inspect is part of the StoresForFlowControl interface.
func (l NoopStoresFlowControlIntegration) Inspect() []roachpb.RangeID {
	return nil
}

// OnRaftTransportDisconnected is part of the RaftTransportDisconnectListener
// interface.
func (NoopStoresFlowControlIntegration) OnRaftTransportDisconnected(
	context.Context, ...roachpb.StoreID,
) {
}
