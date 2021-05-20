// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracingservice

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// node captures the relevant bits of each node as it pertains to the tracing
// service infrastructure.
type node struct {
	id    roachpb.NodeID
	epoch int64
}

// nodes is a collection of node objects.
type nodes []node

// nodesFromNodeLiveness returns the IDs and epochs for all nodes that are
// currently part of the cluster (i.e. they haven't been decommissioned away).
// If any of the nodes are found to be unavailable, an error is returned.
//
// It's important to note that this makes no guarantees about new nodes being
// added to the cluster. It's entirely possible for that to happen concurrently
// with the retrieval of the current set of nodes.
func nodesFromNodeLiveness(ctx context.Context, nl NodeLiveness) (nodes, error) {
	var ns []node
	ls, err := nl.GetLivenessesFromKV(ctx)
	if err != nil {
		return nil, err
	}
	for _, l := range ls {
		if l.Membership.Decommissioned() {
			continue
		}
		live, err := nl.IsLive(l.NodeID)
		if err != nil {
			return nil, err
		}
		if !live {
			return nil, errors.Newf("n%d required, but unavailable", l.NodeID)
		}
		ns = append(ns, node{id: l.NodeID, epoch: l.Epoch})
	}
	return ns, nil
}

func (ns nodes) String() string {
	return redact.StringWithoutMarkers(ns)
}

// SafeFormat implements redact.SafeFormatter.
func (ns nodes) SafeFormat(s redact.SafePrinter, _ rune) {
	s.SafeString("n{")
	if len(ns) > 0 {
		s.Printf("%d", ns[0].id)
		for _, node := range ns[1:] {
			s.Printf(",%d", node.id)
		}
	}
	s.SafeString("}")
}
