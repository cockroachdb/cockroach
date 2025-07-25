// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package faulttolerance provides visibility into the cluster's ability
// to tolerate failures across different failure domains.
package faulttolerance

import (
	"context"
	"iter"
	"maps"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// LocalityMapsFromGossip constructs a map from each NodeID to its
// Locality, by deserializing the node descriptors from Gossip.
func LocalityMapsFromGossip(g *gossip.Gossip) (map[roachpb.NodeID]roachpb.Locality, error) {
	nodeLocality := make(map[roachpb.NodeID]roachpb.Locality)
	if err := g.IterateInfos(gossip.KeyNodeDescPrefix, func(key string, info gossip.Info) error {
		bs, err := info.Value.GetBytes()
		if err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err,
				"failed to extract bytes for key %q", key)
		}

		var d roachpb.NodeDescriptor
		if err := protoutil.Unmarshal(bs, &d); err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err,
				"failed to parse value for key %q", key)
		}

		// Don't use node descriptors with NodeID 0, because that's meant to
		// indicate that the node has been removed from the cluster.
		if d.NodeID != 0 {
			nodeLocality[d.NodeID] = d.Locality
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return nodeLocality, nil
}

// ComputeFaultTolerance computes the fault tolerance margin for each
// failure domain. A failure domain is a set of nodes that map to a
// single locality in the nodeLocality map, or a merged set of nodes
// that share a locality prefix. The fault tolerance margin is the
// number of additional nodes (beyond the nodes in the failure domain)
// that can fail before any range experiences unavailability.
//
// Margins can be negative (indicating that the failure domain is
// critical; its failure will cause unavailability), 0 (the failure
// domain is not critical, but no additional node failures can be
// tolerated), or positive.
//
// The keys in the returned map are locality strings, as returned by
// `roachpb.Locality.String()`. They include "parent localities" as
// well as the actual leaf localities to which nodes are assigned.
//
// The fault tolerance computation occurs in the context of the
// livenessMap (to determine any existing node failures), and is
// evaluated for only the replicas that appear in the replicas Seq.
//
// The evaluation for each replica is independent: it is valid to merge
// the results of this computation for different sets of replicas by
// taking the `min` of the margin values.
func ComputeFaultTolerance(
	ctx context.Context,
	livenessMap livenesspb.NodeVitalityMap,
	nodeLocality map[roachpb.NodeID]roachpb.Locality,
	replicas iter.Seq[*kvserver.Replica],
) (map[string]int, error) {
	return computeFaultToleranceImpl(ctx, livenessMap, nodeLocality, replicas)
}

// replicaDescriber is an interface that makes tests easier to write.
type replicaDescriber interface {
	Desc() *roachpb.RangeDescriptor
}

func computeFaultToleranceImpl[RD replicaDescriber](
	ctx context.Context,
	livenessMap livenesspb.NodeVitalityMap,
	nodeLocality map[roachpb.NodeID]roachpb.Locality,
	replicas iter.Seq[RD],
) (map[string]int, error) {
	for n, l := range nodeLocality {
		if len(l.Tiers) == 0 {
			return nil, errors.AssertionFailedf("node %d missing locality", n)
		}
	}
	failureDomainMap := makeFailureDomains(nodeLocality)

	domainMargins := make(map[string]int)
	for replica := range replicas {
		for domainKey, fd := range failureDomainMap {
			if err := ctx.Err(); err != nil {
				return nil, err
			}

			margin := replica.Desc().Replicas().ReplicationMargin(func(rd roachpb.ReplicaDescriptor) bool {
				if _, ok := fd.nodes[rd.NodeID]; ok {
					return false
				}
				return livenessMap[rd.NodeID].IsLive(livenesspb.SpanConfigConformance)
			})

			if oldMargin, ok := domainMargins[domainKey]; ok {
				domainMargins[domainKey] = min(oldMargin, margin)
			} else {
				domainMargins[domainKey] = margin
			}
		}
	}
	return domainMargins, nil
}

// makeFailureDomains takes a map from NodeID to Locality, and produces
// a map from locality to the set of nodes that belong to that locality.
// It then extends that concept by merging localities with a common
// prefix, producing a map from "locality string prefix" to failureDomain.
func makeFailureDomains(
	nodeLocality map[roachpb.NodeID]roachpb.Locality,
) map[string]*failureDomain {
	domainMap := make(map[string]*failureDomain)
	var unresolved []string
	// First, construct the leaf domains.
	for _, l := range nodeLocality {
		k := l.String()
		if _, ok := domainMap[k]; !ok {
			domainMap[k] = newFailureDomain(l, nodeLocality)
		}
		unresolved = append(unresolved, k)
	}

	// Sort the domains by descending length. In case the depth of the
	// locality tree varies, we want to handle the taller trees first.
	slices.SortStableFunc(unresolved, func(a, b string) int {
		aComma := strings.Count(a, ",")
		bComma := strings.Count(b, ",")
		return bComma - aComma
	})

	// Merge existing domains into parent domains.
	for len(unresolved) > 0 {
		fd := domainMap[unresolved[0]]
		unresolved = unresolved[1:]

		pdKey := fd.parentKey()
		if pdKey == "" {
			continue
		}
		if parentFailureDomain, ok := domainMap[pdKey]; !ok {
			// new unresolved parent domain
			pd := fd.parent()
			domainMap[pdKey] = pd
			unresolved = append(unresolved, pdKey)
		} else {
			// merge child into parent domain
			parentFailureDomain.merge(fd)
		}
	}
	return domainMap
}

// A failureDomain is a set of NodeIDs, labeled by some locality prefix.
// Each actual locality assigned to a node is a failure domain
// containing all the nodes assigned that locality. We also define a
// failure domain for each locality prefix, dropping the last locality
// Tier and including all nodes that have that locality prefix.
type failureDomain struct {
	domain roachpb.Locality
	nodes  map[roachpb.NodeID]struct{}
}

func (fd *failureDomain) merge(rhs *failureDomain) {
	if match, _ := rhs.domain.Matches(fd.domain); !match {
		panic("cannot merge failure domain")
	}

	for n := range rhs.nodes {
		fd.nodes[n] = struct{}{}
	}
}

func (fd *failureDomain) parentKey() string {
	return roachpb.Locality{Tiers: fd.domain.Tiers[0 : len(fd.domain.Tiers)-1]}.String()
}

func newFailureDomain(
	domain roachpb.Locality, nodeLocality map[roachpb.NodeID]roachpb.Locality,
) *failureDomain {
	faultScenario := make(map[roachpb.NodeID]struct{})
	for node := range nodeLocality {
		if match, _ := nodeLocality[node].Matches(domain); match {
			faultScenario[node] = struct{}{}
		}
	}
	return &failureDomain{
		domain: domain,
		nodes:  faultScenario,
	}
}

func (fd *failureDomain) parent() *failureDomain {
	pd := roachpb.Locality{Tiers: fd.domain.Tiers[0 : len(fd.domain.Tiers)-1]}
	nodes := make(map[roachpb.NodeID]struct{}, len(fd.nodes))
	maps.Copy(nodes, fd.nodes)
	return &failureDomain{
		domain: pd,
		nodes:  nodes,
	}
}
