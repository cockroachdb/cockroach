// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opt

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
)

// NodeID uniquely identifies the usage of a Cockroach node within the scope of
// a query. NodeID 0 is reserved to mean "unknown node".
type NodeID int32

// index returns the index of the node in Metadata.nodes. It's biased by 1, so
// that NodeID 0 can be be reserved to mean "unknown node".
func (n NodeID) index() int {
	return int(n - 1)
}

// NodeMeta stores information about one of the nodes stored in the metadata.
type NodeMeta struct {
	// MetaID is the identifier for this node that is unique within the query
	// metadata.
	MetaID NodeID

	// Node is a reference to the node in the catalog.
	Node cat.Node

	// Locality is an ordered set of key value Tiers that describe a node's
	// location. The tier keys should be the same across all nodes.
	Locality roachpb.Locality

	// Attributes specifies a list of arbitrary strings describing
	// node topology, store type, and machine capabilities.
	Attrs roachpb.Attributes
}

// makeNodeID constructs a new NodeID.
func makeNodeID(index int) NodeID {
	// Bias the node index by 1.
	return NodeID(index + 1)
}
