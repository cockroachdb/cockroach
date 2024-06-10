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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvadmission"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowconnectedstream"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowhandle"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

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

// Inspect is part of the StoresForFlowControl interface.
func (sh *storesForFlowControl) Inspect() []roachpb.RangeID {
	ls := (*Stores)(sh)
	var rangeIDs []roachpb.RangeID
	if err := ls.VisitStores(func(s *Store) error {
		rangeIDs = append(rangeIDs, makeStoreForFlowControl(s).Inspect()...)
		return nil
	}); err != nil {
		ctx := ls.AnnotateCtx(context.Background())
		log.Errorf(ctx, "unexpected error: %s", err)
		return nil
	}
	return rangeIDs
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

	if knobs := s.TestingKnobs().FlowControlTestingKnobs; knobs != nil &&
		knobs.UseOnlyForScratchRanges &&
		!repl.IsScratchRange() {
		return nil, false
	}

	repl.mu.Lock()
	defer repl.mu.Unlock()
	return repl.mu.replicaFlowControlIntegration.handle()
}

// ResetStreams is part of the StoresForFlowControl interface.
func (sh *storeForFlowControl) ResetStreams(ctx context.Context) {
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

// Inspect is part of the StoresForFlowControl interface.
func (sh *storeForFlowControl) Inspect() []roachpb.RangeID {
	s := (*Store)(sh)
	var rangeIDs []roachpb.RangeID
	s.VisitReplicas(func(replica *Replica) (wantMore bool) {
		rangeIDs = append(rangeIDs, replica.RangeID)
		return true
	})
	return rangeIDs
}

// OnRaftTransportDisconnected is part of the StoresForFlowControl
// interface.
func (sh *storeForFlowControl) OnRaftTransportDisconnected(
	ctx context.Context, storeIDs ...roachpb.StoreID,
) {
	s := (*Store)(sh)
	s.mu.replicasByRangeID.Range(func(_ roachpb.RangeID, replica *Replica) bool {
		replica.mu.Lock()
		defer replica.mu.Unlock()
		replica.mu.replicaFlowControlIntegration.onRaftTransportDisconnected(ctx, storeIDs...)
		return true
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
	var knobs *kvflowcontrol.TestingKnobs
	if s.TestingKnobs() != nil {
		knobs = s.TestingKnobs().FlowControlTestingKnobs
	}
	return kvflowhandle.New(
		s.cfg.KVFlowController,
		s.cfg.KVFlowHandleMetrics,
		s.cfg.Clock,
		rangeID,
		tenantID,
		knobs,
	)
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

// OnRaftTransportDisconnected is part of the StoresForFlowControl
// interface.
func (NoopStoresFlowControlIntegration) OnRaftTransportDisconnected(
	context.Context, ...roachpb.StoreID,
) {
}

// RACv2

type StoresForRACv2 interface {
	kvadmission.StoresForEvalRACv2
	admission.OnLogEntryAdmitted
}

func MakeStoresForRACv2(stores *Stores) StoresForRACv2 {
	return (*storesForRACv2)(stores)
}

type storesForRACv2 Stores

var _ kvadmission.StoresForEvalRACv2 = &storesForRACv2{}

// Lookup implements kvadmission.StoresForEvalRACv2.
func (ss *storesForRACv2) Lookup(rangeID roachpb.RangeID) kvadmission.RangeControllerProvider {
	return ss.lookup(rangeID)
}

func (ss *storesForRACv2) lookup(rangeID roachpb.RangeID) *replicaRACv2Integration {
	ls := (*Stores)(ss)
	var rr *replicaRACv2Integration
	if err := ls.VisitStores(func(s *Store) error {
		if rr != nil {
			return nil
		}
		repl := s.GetReplicaIfExists(rangeID)
		if repl == nil {
			return nil
		}
		rr = &repl.raftMu.racV2Integration
		return nil
	}); err != nil {
		panic("")
	}
	return rr
}

func (ss *storesForRACv2) AdmittedLogEntry(
	ctx context.Context,
	_ roachpb.NodeID,
	pri admissionpb.WorkPriority,
	_ roachpb.StoreID,
	rangeID roachpb.RangeID,
	pos admission.LogPosition,
) {
	rr := ss.lookup(rangeID)
	if rr != nil {
		rr.admittedLogEntry(ctx, kvflowconnectedstream.RaftPriority(pri), pos.Index)
	}
	// Else range does not have a replica on this store, so ignore.
}
