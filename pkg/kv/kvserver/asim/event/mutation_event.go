// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package event

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// SetSpanConfigEvent represents a mutation event responsible for updating the
// span config for ranges represented by the span.
type SetSpanConfigEvent struct {
	Span   roachpb.Span
	Config roachpb.SpanConfig
}

// AddNodeEvent represents a mutation event responsible for adding a node with
// its store count and locality specified by NumStores and LocalityString.
type AddNodeEvent struct {
	NumStores      int
	LocalityString string
}

// SetNodeLivenessEvent represents a mutation event responsible for setting
// liveness status of a node identified by the NodeID to the specified
// LivenessStatus.
type SetNodeLivenessEvent struct {
	NodeId         state.NodeID
	LivenessStatus livenesspb.NodeLivenessStatus
}

// SetCapacityOverrideEvent represents a mutation event responsible for updating
// the capacity for a store identified by StoreID.
type SetCapacityOverrideEvent struct {
	StoreID          state.StoreID
	CapacityOverride state.CapacityOverride
}

// SetNodeLocalityEvent represents a mutation event responsible for updating
// the locality of a node identified by NodeID.
type SetNodeLocalityEvent struct {
	NodeID         state.NodeID
	LocalityString string
}

var _ Event = &SetSpanConfigEvent{}
var _ Event = &AddNodeEvent{}
var _ Event = &SetNodeLivenessEvent{}
var _ Event = &SetCapacityOverrideEvent{}
var _ Event = &SetNodeLocalityEvent{}

func (se SetSpanConfigEvent) Func() EventFunc {
	return MutationFunc(func(ctx context.Context, s state.State) {
		s.SetSpanConfig(se.Span, &se.Config)
	})
}

func (se SetSpanConfigEvent) String() string {
	return fmt.Sprintf("set span config event with span=%v, config=%v", se.Span, &se.Config)
}

func (ae AddNodeEvent) Func() EventFunc {
	return MutationFunc(func(ctx context.Context, s state.State) {
		node := s.AddNode()
		if ae.LocalityString != "" {
			var locality roachpb.Locality
			if err := locality.Set(ae.LocalityString); err != nil {
				panic(fmt.Sprintf("unable to set node locality %s", err.Error()))
			}
			s.SetNodeLocality(node.NodeID(), locality)
		}
		for i := 0; i < ae.NumStores; i++ {
			if _, ok := s.AddStore(node.NodeID()); !ok {
				panic(fmt.Sprintf("adding store to node=%d failed", node))
			}
		}
	})
}

func (ae AddNodeEvent) String() string {
	return fmt.Sprintf("add node event with num_of_stores=%d, locality_string=%s", ae.NumStores, ae.LocalityString)
}

func (sne SetNodeLivenessEvent) Func() EventFunc {
	return MutationFunc(func(ctx context.Context, s state.State) {
		s.SetNodeLiveness(
			sne.NodeId,
			sne.LivenessStatus,
		)
	})
}

func (sne SetNodeLivenessEvent) String() string {
	return fmt.Sprintf("set node liveness event with nodeID=%d, liveness_status=%v", sne.NodeId, sne.LivenessStatus)
}

func (sce SetCapacityOverrideEvent) Func() EventFunc {
	return MutationFunc(func(ctx context.Context, s state.State) {
		log.Infof(ctx, "setting capacity override %v", sce.CapacityOverride)
		s.SetCapacityOverride(sce.StoreID, sce.CapacityOverride)
	})
}

func (sce SetCapacityOverrideEvent) String() string {
	return fmt.Sprintf("set capacity override event with storeID=%d, capacity_override=%v", sce.StoreID, sce.CapacityOverride)
}

func (sne SetNodeLocalityEvent) Func() EventFunc {
	return MutationFunc(func(ctx context.Context, s state.State) {
		log.Infof(ctx, "setting node locality %v", sne.LocalityString)
		if sne.LocalityString != "" {
			var locality roachpb.Locality
			if err := locality.Set(sne.LocalityString); err != nil {
				panic(fmt.Sprintf("unable to set node locality %s", err.Error()))
			}
			s.SetNodeLocality(sne.NodeID, locality)
		}
	})
}

func (sne SetNodeLocalityEvent) String() string {
	return fmt.Sprintf("set node locality event with nodeID=%d, locality=%v", sne.NodeID, sne.LocalityString)
}
