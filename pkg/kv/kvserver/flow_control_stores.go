// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowhandle"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// StoresFlowControlIntegration is used to integrate with replication flow
// control. It exposes the underlying kvflowcontrol.Handles and is informed of
// (remote) stores we're no longer connected via the raft transport.
type StoresFlowControlIntegration interface {
	kvflowcontrol.Handles
	RaftTransportDisconnectedListener
}

// StoresForFlowControl is a concrete implementation of the
// StoresFlowControlIntegration interface, backed by a set of Stores.
type StoresForFlowControl Stores

var _ StoresFlowControlIntegration = &StoresForFlowControl{}

// MakeStoresForFlowControl returns a new StoresForFlowControl instance.
func MakeStoresForFlowControl(stores *Stores) *StoresForFlowControl {
	return (*StoresForFlowControl)(stores)
}

// Lookup is part of the StoresFlowControlIntegration interface.
func (sh *StoresForFlowControl) Lookup(
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

// ResetStreams is part of the StoresFlowControlIntegration interface.
func (sh *StoresForFlowControl) ResetStreams(ctx context.Context) {
	ls := (*Stores)(sh)
	if err := ls.VisitStores(func(s *Store) error {
		makeStoreForFlowControl(s).ResetStreams(ctx)
		return nil
	}); err != nil {
		ctx = ls.AnnotateCtx(ctx)
		log.Errorf(ctx, "unexpected error: %s", err)
	}
}

// onRaftTransportDisconnected is part of the StoresFlowControlIntegration
// interface.
func (sh *StoresForFlowControl) OnRaftTransportDisconnected(
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

// storeForFlowControlImpl is a concrete implementation of the
// StoresFlowControlIntegration interface, backed by a single Store.
type storeForFlowControlImpl Store

var _ StoresFlowControlIntegration = &storeForFlowControlImpl{}

// makeStoreForFlowControl returns a new storeForFlowControlImpl instance.
func makeStoreForFlowControl(store *Store) *storeForFlowControlImpl {
	return (*storeForFlowControlImpl)(store)
}

// Lookup is part of the StoresFlowControlIntegration interface.
func (sh *storeForFlowControlImpl) Lookup(
	rangeID roachpb.RangeID,
) (_ kvflowcontrol.Handle, found bool) {
	s := (*Store)(sh)
	repl := s.GetReplicaIfExists(rangeID)
	if repl == nil {
		return nil, false
	}

	repl.mu.Lock()
	defer repl.mu.Unlock()
	return repl.mu.replicaFlowControlIntegration.handle()
}

// ResetStreams is part of the StoresFlowControlIntegration interface.
func (sh *storeForFlowControlImpl) ResetStreams(ctx context.Context) {
	s := (*Store)(sh)
	s.VisitReplicas(func(r *Replica) (wantMore bool) {
		r.mu.Lock()
		defer r.mu.Unlock()
		handle, found := r.mu.replicaFlowControlIntegration.handle()
		if found {
			handle.ResetStreams(ctx)
		}
		return true
	})
}

// onRaftTransportDisconnected is part of the StoresFlowControlIntegration
// interface.
func (sh *storeForFlowControlImpl) OnRaftTransportDisconnected(
	ctx context.Context, storeIDs ...roachpb.StoreID,
) {
	s := (*Store)(sh)
	s.mu.replicasByRangeID.Range(func(replica *Replica) {
		replica.mu.Lock()
		defer replica.mu.Unlock()
		replica.mu.replicaFlowControlIntegration.OnRaftTransportDisconnected(ctx, storeIDs...)
	})
}

// storeFlowControlHandleFactory is a concrete implementation of
// kvflowcontrol.HandleFactory.
type storeFlowControlHandleFactory Store

var _ kvflowcontrol.HandleFactory = &storeFlowControlHandleFactory{}

// makeStoreFlowControlHandleFactory returns a new storeFlowControlHandleFactory
// instance.
func makeStoreFlowControlHandleFactory(store *Store) *storeFlowControlHandleFactory {
	return (*storeFlowControlHandleFactory)(store)
}

// NewHandle is part of the kvflowcontrol.HandleFactory interface.
func (shf *storeFlowControlHandleFactory) NewHandle(
	rangeID roachpb.RangeID, tenantID roachpb.TenantID,
) kvflowcontrol.Handle {
	s := (*Store)(shf)
	return kvflowhandle.New(
		s.cfg.KVFlowController,
		s.cfg.KVFlowHandleMetrics,
		s.cfg.Clock,
		rangeID,
		tenantID,
	)
}

// NoopStoresFlowControlIntegration is a no-op implementation of the
// StoresFlowControlIntegration interface.
type NoopStoresFlowControlIntegration struct{}

var _ StoresFlowControlIntegration = NoopStoresFlowControlIntegration{}

// Lookup is part of the StoresFlowControlIntegration interface.
func (l NoopStoresFlowControlIntegration) Lookup(roachpb.RangeID) (kvflowcontrol.Handle, bool) {
	return nil, false
}

// ResetStreams is part of the StoresFlowControlIntegration interface.
func (l NoopStoresFlowControlIntegration) ResetStreams(context.Context) {
}

// Inspect is part of the StoresFlowControlIntegration interface.
func (l NoopStoresFlowControlIntegration) Inspect() []roachpb.RangeID {
	return nil
}

// OnRaftTransportDisconnected is part of the RaftTransportDisconnectedListener
// interface.
func (NoopStoresFlowControlIntegration) OnRaftTransportDisconnected(
	context.Context, ...roachpb.StoreID,
) {
}
