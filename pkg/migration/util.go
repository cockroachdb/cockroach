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
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
)

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

type nodes []node

// node captures the relevant bits of each node as it pertains to the migration
// infrastructure.
type node struct {
	id    roachpb.NodeID
	epoch int64
}

// identical returns whether or not two lists of nodes are identical as sets.
func (ns nodes) identical(other nodes) bool {
	a, b := ns, other
	if len(a) != len(b) {
		return false
	}

	// Sort by node IDs.
	sort.Slice(a, func(i, j int) bool {
		return a[i].id < a[j].id
	})
	sort.Slice(b, func(i, j int) bool {
		return b[i].id < b[j].id
	})

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (ns nodes) String() string {
	var b strings.Builder
	b.WriteString("n{")
	if len(ns) > 0 {
		b.WriteString(fmt.Sprintf("%d", ns[0].id))
		for _, node := range ns[1:] {
			b.WriteString(fmt.Sprintf(",%d", node.id))
		}
	}
	b.WriteString("}")

	return b.String()
}

// SafeFormat implements redact.SafeFormatter.
func (ns nodes) SafeFormat(s redact.SafePrinter, _ rune) {
	s.SafeString(redact.SafeString(ns.String()))
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
