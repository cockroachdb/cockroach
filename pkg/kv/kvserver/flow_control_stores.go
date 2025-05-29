// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/rac2"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/replica_rac2"
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
func MakeStoresForFlowControl(stores *Stores) kvflowcontrol.ReplicationAdmissionHandles {
	return (*storesForFlowControl)(stores)
}

// TODO(rac1): cleanup and stop implementing StoresForFlowControl.

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

// LookupInspect is part of the StoresForFlowControl interface.
func (sh *storesForFlowControl) LookupInspect(
	rangeID roachpb.RangeID,
) (handle kvflowinspectpb.Handle, found bool) {
	if handle, found := sh.Lookup(rangeID); found {
		return handle.Inspect(context.Background()), found
	}
	return kvflowinspectpb.Handle{}, false
}

// LookupReplicationAdmissionHandle is part of the StoresForFlowControl
// interface.
func (sh *storesForFlowControl) LookupReplicationAdmissionHandle(
	rangeID roachpb.RangeID,
) (handle kvflowcontrol.ReplicationAdmissionHandle, found bool) {
	ls := (*Stores)(sh)
	if err := ls.VisitStores(func(s *Store) error {
		if h, ok := makeStoreForFlowControl(s).LookupReplicationAdmissionHandle(rangeID); ok {
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
	repl := sh.lookupReplica(rangeID)
	if repl == nil {
		return nil, false
	}
	repl.mu.Lock()
	defer repl.mu.Unlock()
	return repl.mu.replicaFlowControlIntegration.handle()
}

// LookupInspect is part of the StoresForFlowControl interface.
func (sh *storeForFlowControl) LookupInspect(
	rangeID roachpb.RangeID,
) (handle kvflowinspectpb.Handle, found bool) {
	if handle, found := sh.Lookup(rangeID); found {
		return handle.Inspect(context.Background()), found
	}
	return kvflowinspectpb.Handle{}, false
}

// LookupReplicationAdmissionHandle is part of the StoresForFlowControl
// interface.
func (sh *storeForFlowControl) LookupReplicationAdmissionHandle(
	rangeID roachpb.RangeID,
) (kvflowcontrol.ReplicationAdmissionHandle, bool) {
	repl := sh.lookupReplica(rangeID)
	if repl == nil {
		return nil, false
	}
	// NB: Admit is called soon after this lookup.
	return admissionHandle{
		r: repl,
	}, true
}

func (sh *storeForFlowControl) lookupReplica(rangeID roachpb.RangeID) *Replica {
	s := (*Store)(sh)
	repl := s.GetReplicaIfExists(rangeID)
	if repl == nil {
		return nil
	}
	if knobs := s.TestingKnobs().FlowControlTestingKnobs; knobs != nil &&
		knobs.UseOnlyForScratchRanges &&
		!repl.IsScratchRange() {
		return nil
	}
	return repl
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

// NoopStoresFlowControlIntegration is a no-op implementation of the
// StoresForFlowControl interface.
//
// TODO(rac1): remove.
type NoopStoresFlowControlIntegration struct{}

var _ StoresForFlowControl = NoopStoresFlowControlIntegration{}

// Lookup is part of the StoresForFlowControl interface.
func (l NoopStoresFlowControlIntegration) Lookup(roachpb.RangeID) (kvflowcontrol.Handle, bool) {
	return nil, false
}

// LookupReplicationAdmissionHandle is part of the StoresForFlowControl
// interface.
func (l NoopStoresFlowControlIntegration) LookupReplicationAdmissionHandle(
	rangeID roachpb.RangeID,
) (kvflowcontrol.ReplicationAdmissionHandle, bool) {
	return l.Lookup(rangeID)
}

// ResetStreams is part of the StoresForFlowControl interface.
func (l NoopStoresFlowControlIntegration) ResetStreams(context.Context) {
}

// LookupInspect is part of the StoresForFlowControl interface.
func (l NoopStoresFlowControlIntegration) LookupInspect(
	roachpb.RangeID,
) (kvflowinspectpb.Handle, bool) {
	return kvflowinspectpb.Handle{}, false
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

// StoresForRACv2 implements various interfaces to route to the relevant
// range's Processor.
type StoresForRACv2 interface {
	admission.OnLogEntryAdmitted
	PiggybackedAdmittedResponseScheduler
	kvflowcontrol.InspectHandles
}

// PiggybackedAdmittedResponseScheduler routes followers piggybacked admitted
// response messages to the relevant ranges, and schedules those ranges for
// processing.
type PiggybackedAdmittedResponseScheduler interface {
	ScheduleAdmittedResponseForRangeRACv2(
		ctx context.Context, msgs []kvflowcontrolpb.PiggybackedAdmittedState)
}

func MakeStoresForRACv2(stores *Stores) StoresForRACv2 {
	return (*storesForRACv2)(stores)
}

type storesForRACv2 Stores

// AdmittedLogEntry implements admission.OnLogEntryAdmitted.
func (ss *storesForRACv2) AdmittedLogEntry(
	ctx context.Context, cbState admission.LogEntryAdmittedCallbackState,
) {
	p := ss.lookup(cbState.StoreID, cbState.RangeID, cbState.ReplicaID)
	if p == nil {
		return
	}
	p.AdmittedLogEntry(ctx, replica_rac2.EntryForAdmissionCallbackState{
		Mark:     rac2.LogMark{Term: cbState.LeaderTerm, Index: cbState.Pos.Index},
		Priority: cbState.RaftPri,
	})
}

func (ss *storesForRACv2) lookup(
	storeID roachpb.StoreID, rangeID roachpb.RangeID, replicaID roachpb.ReplicaID,
) replica_rac2.Processor {
	ls := (*Stores)(ss)
	s, err := ls.GetStore(storeID)
	if err != nil {
		// Store has disappeared!
		panic(err)
	}
	r := s.GetReplicaIfExists(rangeID)
	if r == nil || r.replicaID != replicaID {
		return nil
	}
	if flowTestKnobs := r.store.TestingKnobs().FlowControlTestingKnobs; flowTestKnobs != nil &&
		flowTestKnobs.UseOnlyForScratchRanges && !r.IsScratchRange() {
		return nil
	}
	return r.flowControlV2
}

// ScheduleAdmittedResponseForRangeRACv2 implements PiggybackedAdmittedResponseScheduler.
func (ss *storesForRACv2) ScheduleAdmittedResponseForRangeRACv2(
	ctx context.Context, msgs []kvflowcontrolpb.PiggybackedAdmittedState,
) {
	ls := (*Stores)(ss)
	for _, m := range msgs {
		s, err := ls.GetStore(m.ToStoreID)
		if err != nil {
			log.Errorf(ctx, "store %s not found", m.ToStoreID)
			continue
		}
		repl := s.GetReplicaIfExists(m.RangeID)
		if repl == nil || repl.replicaID != m.ToReplicaID {
			continue
		}
		repl.flowControlV2.EnqueuePiggybackedAdmittedAtLeader(m.FromReplicaID, m.Admitted)
		s.scheduler.EnqueueRACv2PiggybackAdmitted(m.RangeID)
	}
}

// LookupInspect implements kvflowcontrol.InspectHandles.
func (ss *storesForRACv2) LookupInspect(
	rangeID roachpb.RangeID,
) (handle kvflowinspectpb.Handle, found bool) {
	ls := (*Stores)(ss)
	if err := ls.VisitStores(func(s *Store) error {
		if found {
			return nil
		}
		if r := s.GetReplicaIfExists(rangeID); r != nil {
			r.raftMu.Lock()
			defer r.raftMu.Unlock()
			handle, found = r.flowControlV2.InspectRaftMuLocked(context.Background())
		}
		return nil
	}); err != nil {
		log.Errorf(ls.AnnotateCtx(context.Background()),
			"unexpected error iterating stores: %s", err)
	}
	return handle, found
}

// Inspect implements kvflowcontrol.InspectHandles.
func (ss *storesForRACv2) Inspect() []roachpb.RangeID {
	ls := (*Stores)(ss)
	var rangeIDs []roachpb.RangeID
	if err := ls.VisitStores(func(s *Store) error {
		s.VisitReplicas(func(r *Replica) (wantMore bool) {
			rangeIDs = append(rangeIDs, r.RangeID)
			return true
		})
		return nil
	}); err != nil {
		log.Errorf(ls.AnnotateCtx(context.Background()),
			"unexpected error iterating stores: %s", err)
	}
	return rangeIDs
}

type admissionHandle struct {
	r *Replica
}

// Admit implements kvflowcontrol.ReplicationAdmissionHandle.
func (h admissionHandle) Admit(
	ctx context.Context, pri admissionpb.WorkPriority, ct time.Time,
) (admitted bool, err error) {
	return h.r.flowControlV2.AdmitForEval(ctx, pri, ct)
}
