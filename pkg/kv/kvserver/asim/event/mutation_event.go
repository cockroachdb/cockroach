// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package event

import (
	"context"
	"fmt"
	"strings"

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

// SetSimulationSettingsEvent represents a mutation event responsible for
// changing a simulation setting during the simulation.
type SetSimulationSettingsEvent struct {
	IsClusterSetting bool
	Key              string
	Value            interface{}
}

var _ Event = &SetSpanConfigEvent{}
var _ Event = &AddNodeEvent{}
var _ Event = &SetNodeLivenessEvent{}
var _ Event = &SetCapacityOverrideEvent{}
var _ Event = &SetNodeLocalityEvent{}
var _ Event = &SetSimulationSettingsEvent{}

func (se SetSpanConfigEvent) Func() EventFunc {
	return MutationFunc(func(ctx context.Context, s state.State) {
		s.SetSpanConfig(se.Span, &se.Config)
	})
}

func (se SetSpanConfigEvent) String() string {
	var buf strings.Builder
	voter, nonvoter := se.Config.GetNumVoters(), se.Config.GetNumNonVoters()
	var nonvoterStr string
	if nonvoter != 0 {
		nonvoterStr = fmt.Sprintf(",%dnonvoters", nonvoter)
	}
	_, _ = fmt.Fprintf(&buf, "[%s,%s): %dvoters%s", se.Span.Key, se.Span.EndKey, voter, nonvoterStr)

	printConstraint := func(tag string, constraints []roachpb.ConstraintsConjunction) {
		if len(constraints) != 0 {
			_, _ = fmt.Fprintf(&buf, "%s", tag)
			for _, c := range constraints {
				_, _ = fmt.Fprintf(&buf, "{%d:", c.NumReplicas)
				for j, constraint := range c.Constraints {
					_, _ = fmt.Fprintf(&buf, "%s=%s", constraint.Key, constraint.Value)
					if j != len(c.Constraints)-1 {
						_, _ = fmt.Fprintf(&buf, ",")
					}
				}
				_, _ = fmt.Fprintf(&buf, "}")
			}
			_, _ = fmt.Fprintf(&buf, "]")
		}
	}
	printConstraint(" [replicas:", se.Config.Constraints)
	printConstraint(" [voters:", se.Config.VoterConstraints)
	if len(se.Config.LeasePreferences) != 0 {
		_, _ = fmt.Fprint(&buf, " [lease:")
		for i, lp := range se.Config.LeasePreferences {
			_, _ = fmt.Fprint(&buf, "{")
			for j, c := range lp.Constraints {
				_, _ = fmt.Fprintf(&buf, "%s=%s", c.Key, c.Value)
				if j != len(lp.Constraints)-1 {
					_, _ = fmt.Fprintf(&buf, ",")
				}
			}
			_, _ = fmt.Fprintf(&buf, "}")
			if i != len(se.Config.LeasePreferences)-1 {
				_, _ = fmt.Fprintf(&buf, ">")
			}
		}
		_, _ = fmt.Fprint(&buf, "]")
	}
	noQuotesBuf := strings.ReplaceAll(buf.String(), "\"", "")
	return noQuotesBuf
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
	var buf strings.Builder
	_, _ = fmt.Fprintf(&buf, "add node with %d stores", ae.NumStores)
	if ae.LocalityString != "" {
		_, _ = fmt.Fprintf(&buf, ", locality_string=%s", ae.LocalityString)
	}
	return buf.String()
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
	return fmt.Sprintf("set n%d to %v", sne.NodeId, sne.LivenessStatus)
}

func (sce SetCapacityOverrideEvent) Func() EventFunc {
	return MutationFunc(func(ctx context.Context, s state.State) {
		log.Dev.Infof(ctx, "setting capacity override %v", sce.CapacityOverride)
		s.SetCapacityOverride(sce.StoreID, sce.CapacityOverride)
	})
}

func (sce SetCapacityOverrideEvent) String() string {
	return fmt.Sprintf("set capacity override event with storeID=%d, capacity_override=%v", sce.StoreID, sce.CapacityOverride)
}

func (sne SetNodeLocalityEvent) Func() EventFunc {
	return MutationFunc(func(ctx context.Context, s state.State) {
		log.Dev.Infof(ctx, "setting node locality %v", sne.LocalityString)
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

func (se SetSimulationSettingsEvent) Func() EventFunc {
	return MutationFunc(func(ctx context.Context, s state.State) {
		if se.IsClusterSetting {
			s.SetClusterSetting(se.Key, se.Value)
		} else {
			s.SetSimulationSettings(se.Key, se.Value)
		}
	})
}

func (se SetSimulationSettingsEvent) String() string {
	return fmt.Sprintf("set %s to %v", se.Key, se.Value)
}
