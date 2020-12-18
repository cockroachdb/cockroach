// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migration

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
)

// node captures the relevant bits of each node as it pertains to the migration
// infrastructure.
type node struct {
	id    roachpb.NodeID
	epoch int64
}

// nodes is a collection of node objects.
type nodes []node

// identical returns whether or not two lists of nodes are identical as sets,
// and if not, what changed (in terms of cluster membership operations and epoch
// changes). The textual diffs are only to be used for logging purposes.
func (ns nodes) identical(other nodes) (ok bool, _ []redact.RedactableString) {
	a, b := ns, other

	type ent struct {
		node         node
		count        int
		epochChanged bool
	}
	m := map[roachpb.NodeID]ent{}
	for _, node := range a {
		m[node.id] = ent{count: 1, node: node, epochChanged: false}
	}
	for _, node := range b {
		e, ok := m[node.id]
		e.count--
		if ok && e.node.epoch != node.epoch {
			e.epochChanged = true
		}
		m[node.id] = e
	}

	var diffs []redact.RedactableString
	for id, e := range m {
		if e.epochChanged {
			diffs = append(diffs, redact.Sprintf("n%d's epoch changed", id))
		}
		if e.count > 0 {
			diffs = append(diffs, redact.Sprintf("n%d was decommissioned", id))
		}
		if e.count < 0 {
			diffs = append(diffs, redact.Sprintf("n%d joined the cluster", id))
		}
	}

	return len(diffs) == 0, diffs
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

// fenceVersionFor constructs the appropriate "fence version" for the given
// cluster version. Fence versions allow the migrations infrastructure to safely
// step through consecutive cluster versions in the presence of nodes (running
// any binary version) being added to the cluster. See the migration manager
// above for intended usage.
//
// Fence versions (and the migrations infrastructure entirely) were introduced
// in the 21.1 release cycle. In the same release cycle, we introduced the
// invariant that new user-defined versions (users being crdb engineers) must
// always have even-numbered Internal versions, thus reserving the odd numbers
// to slot in fence versions for each cluster version. See top-level
// documentation in pkg/clusterversion for more details.
func fenceVersionFor(
	ctx context.Context, cv clusterversion.ClusterVersion,
) clusterversion.ClusterVersion {
	if (cv.Internal % 2) != 0 {
		log.Fatalf(ctx, "only even numbered internal versions allowed, found %s", cv.Version)
	}

	// We'll pick the odd internal version preceding the cluster version,
	// slotting ourselves right before it.
	fenceCV := cv
	fenceCV.Internal--
	return fenceCV
}

// register is a short hand to register a given migration within the global
// registry.
func register(key clusterversion.Key, fn migrationFn, desc string) {
	cv := clusterversion.ClusterVersion{Version: clusterversion.ByKey(key)}
	if _, ok := registry[cv]; ok {
		log.Fatalf(context.Background(), "doubly registering migration for %s", cv)
	}
	registry[cv] = Migration{cv: cv, fn: fn, desc: desc}
}
