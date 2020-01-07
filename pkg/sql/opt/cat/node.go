// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cat

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// Node is an interface to a node in a CockroachDB cluster, exposing only the
// information needed by the query optimizer.
type Node interface {
	// ID returns the stable identifier for this node that corresponds to the
	// Cockroach Node ID.
	ID() roachpb.NodeID

	// Locality is an ordered set of key value Tiers that describe a node's
	// location. The tier keys should be the same across all nodes.
	Locality() roachpb.Locality

	// Attributes specifies a list of arbitrary strings describing
	// node topology, store type, and machine capabilities.
	Attrs() roachpb.Attributes
}
